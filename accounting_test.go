package e2escope

import (
	"context"
	"database/sql"
	"encoding/json"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

type clearinghouseFixture struct {
	dir, db, management, webhook, broker, topic, operator, signerSecret, producerPassword, readonly string
	readDB                                                                                          *sql.DB
	config                                                                                          map[string]any
	process                                                                                         *childProcess
	chain                                                                                           *chainFixture
	owner                                                                                           *testing.T
}

func startClearinghouse(t *testing.T, c *chainFixture, accounting bool) *clearinghouseFixture {
	t.Helper()
	fixturePoolFor(t)
	dir := t.TempDir()
	ch := &clearinghouseFixture{dir: dir, db: filepath.Join(dir, "clearinghouse.sqlite"), management: "http://" + freeAddr(t), webhook: "http://" + freeAddr(t), broker: freeAddr(t), topic: envDefault("E2E_KAFKA_TOPIC", "livepeer-signing"), operator: uuid.NewString(), signerSecret: uuid.NewString(), producerPassword: uuid.NewString(), readonly: uuid.NewString(), chain: c, owner: t}
	credentials := map[string]any{"credentials": []any{
		map[string]any{"id": "operator", "secret": ch.operator, "management": map[string]any{"allow": []string{"grants.*", "allocations.*", "api_keys.*", "sessions.*", "usage.read", "ledger.read", "escrow.read", "settlements.read"}}},
		map[string]any{"id": "reader", "secret": ch.readonly, "management": map[string]any{"allow": []string{"ledger.read"}}},
		map[string]any{"id": "signer", "secret": ch.signerSecret, "webhook": map[string]any{"authorize": true}},
		map[string]any{"id": "accounting", "secret": uuid.NewString(), "kafka": map[string]any{"username": "accounting", "allow": []string{"read"}, "accounting": true}},
		map[string]any{"id": "producer", "secret": ch.producerPassword, "kafka": map[string]any{"username": "producer", "allow": []string{"write"}}},
	}}
	data, err := json.Marshal(credentials)
	require.NoError(t, err)
	creds := filepath.Join(dir, "credentials.json")
	require.NoError(t, os.WriteFile(creds, data, 0600))
	ch.config = map[string]any{"DBPath": ch.db, "CredsFile": creds, "EnableAuthWebhook": strings.TrimPrefix(ch.webhook, "http://"), "EnableManagementAPI": strings.TrimPrefix(ch.management, "http://"), "EnableKafka": accounting, "EnableAccounting": accounting, "KafkaBind": ch.broker, "KafkaTopic": ch.topic}
	ch.management = publicURL(t, ch.management)
	ch.webhook = publicURL(t, ch.webhook)
	if composeMode() && accounting {
		ch.broker = tunneledKafka(t, ch.broker)
	}
	ch.restart(t)
	ch.readDB = openReadDB(t, ch.db)
	t.Cleanup(func() { require.NoError(t, ch.readDB.Close()) })
	return ch
}
func (ch *clearinghouseFixture) restart(t *testing.T) {
	t.Helper()
	if ch.process != nil {
		ch.process.stop()
	}
	path := writeTOML(t, filepath.Join(ch.dir, "clearinghouse.toml"), ch.config)
	ch.process = startChild(ch.owner, "clearinghouse", binary(t, "clearinghouse"), ch.dir, []string{"serve", "--config-file", path})
	waitForHTTP(t, "GET", ch.management+"/readyz", 200, ch.process.logPath)
	waitForHTTP(t, "GET", ch.webhook+"/readyz", 200, ch.process.logPath)
}
func (ch *clearinghouseFixture) enableListener(t *testing.T, payer string) {
	t.Helper()
	ch.config["EnableOnchainListener"] = true
	ch.config["RPCURLFile"] = secretFile(t, ch.dir, "rpc", ch.chain.url)
	ch.config["ChainID"] = ch.chain.chainID.String()
	ch.config["TicketBroker"] = ch.chain.broker.Hex()
	ch.config["SignerAddresses"] = []string{payer}
	confirmations, err := strconv.ParseInt(envDefault("E2E_CHAIN_CONFIRMATIONS", "2"), 10, 64)
	require.NoError(t, err)
	ch.config["Confirmations"] = confirmations
	ch.config["PollInterval"] = envDefault("E2E_CHAIN_POLL_INTERVAL", "200ms")
	ch.restart(t)
}
func (ch *clearinghouseFixture) kafkaConfig(t *testing.T) map[string]any {
	t.Helper()
	maxBytes, err := strconv.ParseInt(envDefault("E2E_OUTBOX_MAX_BYTES", "268435456"), 10, 64)
	require.NoError(t, err)
	require.Positive(t, maxBytes, "E2E_OUTBOX_MAX_BYTES")
	return map[string]any{"Broker": "kafka://" + ch.broker, "Topic": ch.topic, "AuthMethod": "scram-sha-512", "UsernameFile": secretFile(t, ch.dir, "producer-user", "producer"), "PasswordFile": secretFile(t, ch.dir, "producer-password", ch.producerPassword), "OutboxDB": filepath.Join(ch.dir, "signer-outbox.sqlite"), "OutboxMaxBytes": maxBytes}
}
func (ch *clearinghouseFixture) request(t *testing.T, method, path string, payload any, status int, out any) []byte {
	t.Helper()
	got, body := httpJSON(t, method, ch.management+path, payload, map[string]string{"Livepeer-Clearinghouse-Token": ch.operator})
	require.Equal(t, status, got, string(body))
	if out != nil {
		require.NoError(t, json.Unmarshal(body, out))
	}
	return body
}

type allocationKey struct{ Grant, Allocation, ID, Key, Currency string }

func (ch *clearinghouseFixture) key(t *testing.T, currency, amount string) allocationKey {
	t.Helper()
	var grant, key map[string]string
	ch.request(t, "POST", "/v1/grants", map[string]string{"name": "e2e-" + uuid.NewString(), "amount_" + currency: amount, "status": "active"}, 201, &grant)
	ch.request(t, "POST", "/v1/api-keys", map[string]string{"grant_id": grant["id"], "name": "gateway", "amount_" + currency: amount}, 201, &key)
	require.NotEmpty(t, key["api_key"])
	return allocationKey{Grant: grant["id"], Allocation: key["allocation_id"], ID: key["id"], Key: key["api_key"], Currency: currency}
}
func (k allocationKey) headers() map[string]string {
	return map[string]string{"Authorization": "Bearer " + k.Key}
}
func openReadDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	require.NoError(t, err)
	return db
}
func (ch *clearinghouseFixture) scalar(t *testing.T, query string, args ...any) string {
	t.Helper()
	db := ch.readDB
	var result string
	require.NoError(t, db.QueryRow(query, args...).Scan(&result))
	return result
}
func (ch *clearinghouseFixture) usageCount(t *testing.T, status string) int {
	t.Helper()
	db := ch.readDB
	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM usage_events WHERE status=?", status).Scan(&n))
	return n
}
func (ch *clearinghouseFixture) waitUsage(t *testing.T, status string, n int) {
	t.Helper()
	eventually(t, func() bool { return ch.usageCount(t, status) >= n }, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond, "accounting %s >= %d\n%s", status, n, readLog(t, ch.process.logPath))
}
func (ch *clearinghouseFixture) balance(t *testing.T, k allocationKey, kind string) *big.Int {
	s := ch.scalar(t, "SELECT coalesce((SELECT balance_units FROM account_balances WHERE account_type=? AND account_id=? AND currency=?),'0')", kind, k.Allocation, k.Currency)
	v, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok)
	return v
}
func (ch *clearinghouseFixture) publish(t *testing.T, raw []byte) {
	t.Helper()
	mechanism, err := scram.Mechanism(scram.SHA512, "producer", ch.producerPassword)
	require.NoError(t, err)
	transport := &kafka.Transport{SASL: mechanism}
	writer := &kafka.Writer{Addr: kafka.TCP(ch.broker), Topic: ch.topic, Transport: transport, Balancer: &kafka.LeastBytes{}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, writer.WriteMessages(ctx, kafka.Message{Value: raw}))
	require.NoError(t, writer.Close())
	transport.CloseIdleConnections()
}
func (ch *clearinghouseFixture) latestEvent(t *testing.T) []byte {
	t.Helper()
	db := ch.readDB
	var raw []byte
	require.NoError(t, db.QueryRow("SELECT raw_payload FROM usage_events WHERE status='applied' ORDER BY created_at_ms DESC,offset DESC LIMIT 1").Scan(&raw))
	return raw
}

func (ch *clearinghouseFixture) paymentEvent(t *testing.T, payment signedPayment) []byte {
	t.Helper()
	authID := stateObject(t, payment)["AuthID"]
	db := ch.readDB
	var raw []byte
	eventually(t, func() bool {
		err := db.QueryRow("SELECT raw_payload FROM usage_events WHERE status='applied' AND payment_session_id=? ORDER BY created_at_ms DESC,offset DESC LIMIT 1", authID).Scan(&raw)
		if err == sql.ErrNoRows {
			return false
		}
		require.NoError(t, err)
		return true
	}, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond, "accounting event for authorized job %v", authID)
	return raw
}

func TestClearinghouseManagement(t *testing.T) {
	ch := startClearinghouse(t, nil, true)
	t.Run("service permissions", func(t *testing.T) {
		for _, token := range []string{"", "wrong", ch.signerSecret} {
			status, _ := httpJSON(t, "GET", ch.management+"/v1/grants", nil, map[string]string{"Livepeer-Clearinghouse-Token": token})
			require.Equal(t, 401, status)
		}
		status, _ := httpJSON(t, "POST", ch.management+"/v1/grants", map[string]string{"name": "denied"}, map[string]string{"Livepeer-Clearinghouse-Token": ch.readonly})
		require.Equal(t, 403, status)
		for _, token := range []string{"", "wrong", ch.operator, ch.readonly} {
			status, _ := httpJSON(t, "POST", ch.webhook+"/v1/signer/authorize", map[string]any{}, map[string]string{"Livepeer-Clearinghouse-Token": token})
			require.Equal(t, 401, status, "webhook requires a webhook service role")
		}
	})
	t.Run("Kafka service permissions", func(t *testing.T) {
		for _, credentials := range []struct{ name, user, secret string }{{"missing", "", ""}, {"wrong secret", "producer", "wrong"}, {"wrong role", "signer", ch.signerSecret}, {"producer", "producer", ch.producerPassword}} {
			t.Run(credentials.name, func(t *testing.T) {
				dialer := &kafka.Dialer{Timeout: 3 * time.Second}
				if credentials.user != "" {
					mechanism, err := scram.Mechanism(scram.SHA512, credentials.user, credentials.secret)
					require.NoError(t, err)
					dialer.SASLMechanism = mechanism
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				conn, err := dialer.DialContext(ctx, "tcp", ch.broker)
				if conn != nil {
					defer conn.Close()
					require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
					if err == nil {
						_, err = conn.ReadPartitions(ch.topic)
					}
				}
				if credentials.name == "producer" {
					require.NoError(t, err, "authorized producer can access its configured topic")
				} else {
					require.Error(t, err, "Kafka requires authenticated credentials with a Kafka service role")
				}
			})
		}
	})
	for _, currency := range []string{"eth", "usd"} {
		t.Run(currency, func(t *testing.T) {
			k := ch.key(t, currency, "1.123456789012345678")
			want := new(big.Int)
			want.SetString("1123456789012345678", 10)
			require.Equal(t, 0, want.Cmp(ch.balance(t, k, "allocation_available")))
			ch.request(t, "POST", "/v1/grants/"+k.Grant+"/fund", map[string]string{"amount_" + currency: "0.1"}, 200, nil)
			ch.request(t, "POST", "/v1/allocations/"+k.Allocation+"/fund", map[string]string{"amount_" + currency: "0.1"}, 200, nil)
			want.Add(want, big.NewInt(100000000000000000))
			require.Equal(t, 0, want.Cmp(ch.balance(t, k, "allocation_available")))
			other := ch.key(t, currency, "1")
			ch.request(t, "POST", "/v1/allocations/"+k.Allocation+"/revoke", nil, 200, nil)
			require.Zero(t, ch.balance(t, k, "allocation_available").Sign())
			require.Equal(t, "1000000000000000000", ch.balance(t, other, "allocation_available").String())
			ch.request(t, "GET", "/v1/ledger/report", nil, 200, nil)
		})
	}
	t.Run("webhook key and lifecycle", func(t *testing.T) {
		k := ch.key(t, "eth", "1")
		state := map[string]any{"StateID": uuid.NewString(), "OrchestratorAddress": "0x0000000000000000000000000000000000001234", "App": "e2e", "Type": "fixed", "FeeWei": "1"}
		authorize := func(headers any) map[string]any {
			status, body := httpJSON(t, "POST", ch.webhook+"/v1/signer/authorize", map[string]any{"headers": headers, "state": state}, map[string]string{"Livepeer-Clearinghouse-Token": ch.signerSecret})
			require.Equal(t, 200, status, string(body))
			var decision map[string]any
			require.NoError(t, json.Unmarshal(body, &decision))
			return decision
		}
		for _, header := range []any{map[string][]string{}, map[string][]string{"Authorization": {"Bearer invalid"}}, map[string][]string{"Authorization": {"Bearer " + k.Key, "Bearer " + k.Key}}} {
			require.Equal(t, float64(401), authorize(header)["status"])
		}
		headers := map[string][]string{"Authorization": {"Bearer " + k.Key}}
		decision := authorize(headers)
		require.Equal(t, float64(200), decision["status"])
		authID := decision["auth_id"].(string)
		state["AuthID"] = authID
		require.Equal(t, authID, authorize(headers)["auth_id"])
		for _, binding := range []string{"App", "Type", "OrchestratorAddress", "AuthID"} {
			old := state[binding]
			state[binding] = "changed"
			if binding == "OrchestratorAddress" {
				state[binding] = "0x0000000000000000000000000000000000005678"
			}
			require.Equal(t, float64(403), authorize(headers)["status"])
			state[binding] = old
		}
		for _, resource := range []string{"grants/" + k.Grant, "allocations/" + k.Allocation} {
			ch.request(t, "PATCH", "/v1/"+resource+"/status", map[string]string{"status": "paused"}, 200, nil)
			require.Equal(t, float64(403), authorize(headers)["status"])
			ch.request(t, "PATCH", "/v1/"+resource+"/status", map[string]string{"status": "active"}, 200, nil)
		}
		ch.request(t, "POST", "/v1/sessions/"+authID+"/revoke", nil, 200, nil)
		require.Equal(t, float64(403), authorize(headers)["status"])
		ch.request(t, "POST", "/v1/api-keys/"+k.ID+"/revoke", nil, 200, nil)
		require.Equal(t, float64(401), authorize(headers)["status"])
	})
}

func TestClearinghouseSigning(t *testing.T) {
	forEachPair(t, func(t *testing.T, orchBackend, signerBackend string, wallets walletMode) {
		fixturePoolFor(t)
		c := preflightChain(t)
		ch := startClearinghouse(t, c, true)
		kafkaConfig := ch.signerKafkaConfig(t, signerBackend)
		f := setupPaid(t, c, orchBackend, signerBackend, wallets, ch.webhook+"/v1/signer/authorize", "Livepeer-Clearinghouse-Token: "+ch.signerSecret, kafkaConfig)
		ch.enableListener(t, f.payer.address.Hex())
		runClearinghouseAuthorization(t, ch, f)
		t.Run("signing authorization", func(t *testing.T) {
			k := ch.key(t, "eth", "1")
			j := f.job(t, "fixed", "persistent")
			j.obtainChallenge(t)
			for _, headers := range []map[string]string{nil, {"Authorization": "Bearer invalid"}} {
				status, body := j.generate(t, headers)
				require.Equal(t, 401, status, string(body))
				var p signedPayment
				_ = json.Unmarshal(body, &p)
				require.Empty(t, p.Payment)
			}
			j.sign(t, k.headers())
			s := j.submit(t)
			stopPaidSession(t, f.orch, j.route, s.SessionID)
			ch.request(t, "POST", "/v1/api-keys/"+k.ID+"/revoke", nil, 200, nil)
			status, body := j.generate(t, k.headers())
			require.Equal(t, 401, status, string(body))
		})
		t.Run("escrow reporting", func(t *testing.T) {
			c.requireCanonicalHeaders(t)
			ch.request(t, "GET", "/v1/escrow/report", nil, 200, nil)
			ch.request(t, "GET", "/v1/escrow/activity", nil, 200, nil)
			c.fundPayer(t, f.payer, big.NewInt(1000000000), big.NewInt(2000000000))
			eventually(t, func() bool {
				db := ch.readDB
				var n int
				err := db.QueryRow("SELECT count(*) FROM ticket_broker_events").Scan(&n)
				return err == nil && n >= 2
			}, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 200*time.Millisecond)
			ch.request(t, "GET", "/v1/escrow/report", nil, 200, nil)
		})
		runSignerAccounting(t, ch, f)
		runLateAccounting(t, ch, f)
	})
}

func runSignerAccounting(t *testing.T, ch *clearinghouseFixture, f *paidFixture) {
	for _, currency := range []string{"eth", "usd"} {
		for _, unit := range []string{"fixed", "hour"} {
			t.Run("usage/"+currency+"/"+unit, func(t *testing.T) {
				k := ch.key(t, currency, "1")
				before := ch.balance(t, k, "allocation_available")
				j := f.job(t, unit, "persistent")
				j.obtainChallenge(t)
				j.sign(t, k.headers())
				raw := ch.paymentEvent(t, j.payment)
				var event struct {
					ID, Type string
					Data     map[string]any
				}
				require.NoError(t, json.Unmarshal(raw, &event))
				require.Equal(t, "create_signed_ticket", event.Type)
				require.Equal(t, f.recipient.address.Hex(), event.Data["orch_address"])
				require.Equal(t, stateObject(t, j.payment)["AuthID"], event.Data["auth_id"])
				require.Equal(t, float64(0), event.Data["pixels"])
				fee := new(big.Int)
				fee.SetString(event.Data["computed_fee"].(string), 10)
				if currency == "usd" {
					r, ok := new(big.Rat).SetString(event.Data["computed_fee_usd"].(string))
					require.True(t, ok)
					r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
					require.True(t, r.IsInt())
					fee.Set(r.Num())
				}
				require.Equal(t, 0, new(big.Int).Sub(before, fee).Cmp(ch.balance(t, k, "allocation_available")))
				require.Equal(t, 0, fee.Cmp(ch.balance(t, k, "allocation_spent")))
				session := j.submit(t)
				stopPaidSession(t, f.orch, j.route, session.SessionID)
				ch.request(t, "GET", "/v1/usage", nil, 200, nil)
				ch.request(t, "GET", "/v1/ledger/report", nil, 200, nil)
			})
		}
	}
	t.Run("replay and quarantine", func(t *testing.T) {
		k := ch.key(t, "usd", "1")
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		j.sign(t, k.headers())
		raw := ch.paymentEvent(t, j.payment)
		balance := ch.balance(t, k, "allocation_spent")
		for _, change := range []string{"replay", "conflicting-id", "missing-auth", "wrong-session", "negative-fee", "missing-usd", "ticket-count", "unknown-type", "malformed", "zero count", "fractional count", "negative count", "invalid time", "negative timestamp", "negative USD", "nondecimal fee", "recipient mismatch", "app mismatch", "type mismatch", "missing request", "missing PM session"} {
			t.Run(change, func(t *testing.T) {
				expected := "quarantined"
				switch change {
				case "replay":
					expected = "duplicate"
				case "unknown-type":
					expected = "ignored"
				}
				before := ch.usageCount(t, expected)
				ch.publish(t, malformedEvent(t, raw, change))
				ch.waitUsage(t, expected, before+1)
				require.Equal(t, balance.String(), ch.balance(t, k, "allocation_spent").String(), "invalid usage cannot charge the allocation")
			})
		}
	})

	t.Run("exhaustion funding and overdraw", func(t *testing.T) {
		k := ch.key(t, "eth", "0.000000000000000001")
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		j.sign(t, k.headers())
		ch.paymentEvent(t, j.payment)
		require.Negative(t, ch.balance(t, k, "allocation_available").Sign())
		status, body := j.generate(t, k.headers())
		require.Equal(t, 402, status, string(body))
		ch.request(t, "POST", "/v1/grants/"+k.Grant+"/fund", map[string]string{"amount_eth": "0.01"}, 200, nil)
		ch.request(t, "POST", "/v1/allocations/"+k.Allocation+"/fund", map[string]string{"amount_eth": "0.01"}, 200, nil)
		j.sign(t, k.headers())
	})
	if f.signer.backend != "node" {
		return
	}
	t.Run("broker outage signer restart and replay", func(t *testing.T) {
		k := ch.key(t, "eth", "1")
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		ch.config["EnableKafka"] = false
		ch.config["EnableAccounting"] = false
		ch.restart(t)
		j.sign(t, k.headers())
		f.signer.process.stop()
		require.Positive(t, outboxCount(t, f.signer.opts.kafka["OutboxDB"].(string)))
		ch.config["EnableKafka"] = true
		ch.config["EnableAccounting"] = true
		n := ch.usageCount(t, "applied")
		ch.restart(t)
		f.signer = startSigner(f.owner, f.signer.opts)
		ch.waitUsage(t, "applied", n+1)
		ch.paymentEvent(t, j.payment)
		eventually(t, func() bool { return outboxCount(t, f.signer.opts.kafka["OutboxDB"].(string)) == 0 }, 20*time.Second, 100*time.Millisecond)
		before := ch.balance(t, k, "allocation_spent")
		ch.restart(t)
		require.Equal(t, 0, before.Cmp(ch.balance(t, k, "allocation_spent")))
	})
	t.Run("outbox capacity withholds payment", func(t *testing.T) {
		opts := f.signer.opts
		opts.dir = ""
		opts.kafka = maps.Clone(opts.kafka)
		opts.kafka["OutboxDB"] = filepath.Join(t.TempDir(), "tiny.sqlite")
		opts.kafka["OutboxMaxBytes"] = int64(1)
		s := startSigner(t, opts)
		j := f.job(t, "fixed", "persistent")
		j.signer = s
		j.obtainChallenge(t)
		k := ch.key(t, "eth", "1")
		status, body := j.generate(t, k.headers())
		require.Equal(t, 413, status, string(body))
		var p signedPayment
		_ = json.Unmarshal(body, &p)
		require.Empty(t, p.Payment)
	})
	runFullOutbox(t, ch, f)
}
func outboxCount(t *testing.T, path string) int {
	t.Helper()
	db := openReadDB(t, path)
	defer db.Close()
	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM signer_kafka_events").Scan(&n))
	return n
}

func malformedEvent(t *testing.T, raw []byte, change string) []byte {
	t.Helper()
	if change == "replay" {
		return raw
	}
	if change == "malformed" {
		return []byte("{")
	}
	var event map[string]any
	require.NoError(t, json.Unmarshal(raw, &event))
	if change != "conflicting-id" {
		event["id"] = uuid.NewString()
	}
	data := event["data"].(map[string]any)
	switch change {
	case "conflicting-id":
		data["request_id"] = "conflict"
	case "missing-auth":
		data["auth_id"] = "unknown"
	case "wrong-session":
		data["session_id"] = "wrong"
	case "negative-fee":
		data["computed_fee"] = "-1"
	case "missing-usd":
		delete(data, "computed_fee_usd")
	case "ticket-count":
		data["num_tickets"] = 101
	case "unknown-type":
		event["type"] = "unrelated"
	case "zero count":
		data["num_tickets"] = 0
	case "fractional count":
		data["num_tickets"] = 1.5
	case "negative count":
		data["num_tickets"] = -1
	case "invalid time":
		data["current_time_unix"] = "invalid"
	case "negative timestamp":
		data["current_time_unix"] = -1
	case "negative USD":
		data["computed_fee_usd"] = "-1"
	case "nondecimal fee":
		data["computed_fee"] = "NaN"
	case "recipient mismatch":
		data["orch_address"] = "0x0000000000000000000000000000000000001234"
	case "app mismatch":
		data["app"] = "wrong"
	case "type mismatch":
		data["pipeline"] = "wrong"
	case "missing request":
		delete(data, "request_id")
	case "missing PM session":
		delete(data, "pm_session_id")
	}
	result, err := json.Marshal(event)
	require.NoError(t, err)
	return result
}

func scalarInt(t *testing.T, ch *clearinghouseFixture, query string, args ...any) int64 {
	t.Helper()
	db := ch.readDB
	var n int64
	require.NoError(t, db.QueryRow(query, args...).Scan(&n))
	return n
}
