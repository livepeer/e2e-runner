package e2escope

import (
	"encoding/json"
	"math/big"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func runClearinghouseAuthorization(t *testing.T, ch *clearinghouseFixture, f *paidFixture) {
	for _, scenario := range []string{"zero funding", "grant paused", "allocation paused", "allocation revoked", "session revoked", "grant not started", "grant ended", "allocation not started", "allocation ended"} {
		t.Run("authorization/"+scenario, func(t *testing.T) {
			amount := "1"
			if scenario == "zero funding" {
				amount = "0"
			}
			k := ch.key(t, "eth", amount)
			if scenario == "grant not started" || scenario == "grant ended" || scenario == "allocation not started" || scenario == "allocation ended" {
				create := map[string]string{"name": "window", "amount_eth": "1", "status": "active"}
				field, when := "starts_at", time.Now().Add(time.Hour)
				if scenario == "grant ended" || scenario == "allocation ended" {
					field = "ends_at"
					when = time.Now().Add(-time.Hour)
				}
				create[field] = when.UTC().Format(time.RFC3339)
				var value map[string]string
				if scenario == "grant not started" || scenario == "grant ended" {
					ch.request(t, "POST", "/v1/grants", create, 201, &value)
					k.Grant = value["id"]
					ch.request(t, "POST", "/v1/api-keys", map[string]string{"grant_id": k.Grant, "amount_eth": "1", "name": "window"}, 201, &value)
				} else {
					create["grant_id"] = k.Grant
					ch.request(t, "POST", "/v1/grants/"+k.Grant+"/fund", map[string]string{"amount_eth": "1"}, 200, nil)
					ch.request(t, "POST", "/v1/allocations", create, 201, &value)
					ch.request(t, "POST", "/v1/api-keys", map[string]string{"allocation_id": value["id"], "name": "window"}, 201, &value)
				}
				k.Key = value["api_key"]
			}
			j := f.job(t, "fixed", "persistent")
			j.obtainChallenge(t)
			if scenario == "session revoked" {
				j.sign(t, k.headers())
				state := stateObject(t, j.payment)
				ch.request(t, "POST", "/v1/sessions/"+state["AuthID"].(string)+"/revoke", nil, 200, nil)
			}
			switch scenario {
			case "grant paused":
				ch.request(t, "PATCH", "/v1/grants/"+k.Grant+"/status", map[string]string{"status": "paused"}, 200, nil)
			case "allocation paused":
				ch.request(t, "PATCH", "/v1/allocations/"+k.Allocation+"/status", map[string]string{"status": "paused"}, 200, nil)
			case "allocation revoked":
				ch.request(t, "POST", "/v1/allocations/"+k.Allocation+"/revoke", nil, 200, nil)
			}
			status, body := j.generate(t, k.headers())
			expected := 403
			if scenario == "zero funding" {
				expected = 402
			}
			require.Equal(t, expected, status, string(body))
			assertNoCredentials(t, body)
		})
	}
}

func runLateAccounting(t *testing.T, ch *clearinghouseFixture, f *paidFixture) {
	t.Run("revocation with late actual usage", func(t *testing.T) {
		k := ch.key(t, "eth", "1")
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		restore := pauseAccounting(t, ch)
		defer restore()
		j.sign(t, k.headers())
		session := j.submit(t)
		stopPaidSession(t, f.orch, j.route, session.SessionID)
		n := ch.usageCount(t, "applied")
		ch.request(t, "POST", "/v1/allocations/"+k.Allocation+"/revoke", nil, 200, nil)
		require.Zero(t, ch.balance(t, k, "allocation_available").Sign())
		restore()
		ch.waitUsage(t, "applied", n+1)
		ch.paymentEvent(t, j.payment)
		require.Positive(t, ch.balance(t, k, "allocation_spent").Sign())
		require.Negative(t, ch.balance(t, k, "allocation_available").Sign(), "late issued usage survives revocation")
	})
	t.Run("concurrent liabilities and exact aggregate", func(t *testing.T) {
		// Disable ingestion so independent authorized jobs can issue before either
		// charge is observed. This deliberately tests permitted accounting overdraw.
		k := ch.key(t, "eth", "0.000000000000000001")
		restore := pauseAccounting(t, ch)
		defer restore()
		n := ch.usageCount(t, "applied")
		total := new(big.Int)
		jobs := make([]*paymentJob, 2)
		for i := range jobs {
			j := f.job(t, "fixed", "persistent")
			j.obtainChallenge(t)
			jobs[i] = j
		}
		type issued struct {
			index, status int
			body          []byte
			err           error
		}
		results := make(chan issued, len(jobs))
		for i, j := range jobs {
			go func(i int, j *paymentJob) {
				status, body, err := requestJSON("POST", j.signer.base+"/generate-live-payment", j.request(), k.headers())
				results <- issued{i, status, body, err}
			}(i, j)
		}
		for range jobs {
			result := <-results
			require.NoError(t, result.err)
			require.Equal(t, 200, result.status, string(result.body))
			j := jobs[result.index]
			require.NoError(t, json.Unmarshal(result.body, &j.payment))
			state := stateObject(t, j.payment)
			require.NotEmpty(t, state["AuthID"])
			s := j.submit(t)
			stopPaidSession(t, f.orch, j.route, s.SessionID)
		}
		restore()
		ch.waitUsage(t, "applied", n+2)
		for _, j := range jobs {
			ch.paymentEvent(t, j.payment)
		}
		db := openReadDB(t, ch.db)
		rows, err := db.Query("SELECT u.computed_fee_wei FROM usage_events u JOIN payment_sessions s ON s.id=u.payment_session_id WHERE s.allocation_id=? AND u.status='applied'", k.Allocation)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var amount string
			require.NoError(t, rows.Scan(&amount))
			v, ok := new(big.Int).SetString(amount, 10)
			require.True(t, ok)
			total.Add(total, v)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, total.String(), ch.balance(t, k, "allocation_spent").String())
		require.Equal(t, new(big.Int).Sub(big.NewInt(1), total).String(), ch.balance(t, k, "allocation_available").String())
	})
	if f.signer.backend != "node" {
		return
	}
	t.Run("outbox storage failure", func(t *testing.T) {
		opts := f.signer.opts
		opts.kafka = cloneMap(opts.kafka)
		opts.kafka["OutboxDB"] = "/dev/null/outbox.sqlite"
		// Startup must reject an unusable durable sink before issuing any tickets.
		dir := t.TempDir()
		addr := freeAddr(t)
		config := map[string]any{"Listen": addr, "MetricsListen": freeAddr(t), "KeystoreFile": f.payer.keystore, "KeystorePasswordFile": f.payer.passwordFile, "RPCURLFile": secretFile(t, dir, "rpc", f.chain.url), "ChainID": f.chain.chainID.Uint64(), "Controller": f.chain.controller.Hex(), "MaxHourlyPrice": "100", "MaxFixedPrice": "1", "WeiPerUSD": envDefault("E2E_WEI_PER_USD", "1000000000000000"), "Kafka": opts.kafka}
		child := startChild(t, "invalid-outbox", binary(t, "node"), dir, []string{"signer", "--config", writeTOML(t, dir+"/signer.toml", config)})
		select {
		case err := <-child.done:
			require.Error(t, err)
		case <-time.After(envDuration(t, "E2E_READY_TIMEOUT", "30s")):
			t.Fatalf("unusable outbox did not fail startup: %s", readLog(t, child.logPath))
		}
	})
}

func pauseAccounting(t *testing.T, ch *clearinghouseFixture) func() {
	t.Helper()
	ch.config["EnableAccounting"] = false
	ch.restart(t)
	var once sync.Once
	return func() {
		once.Do(func() {
			ch.config["EnableAccounting"] = true
			ch.restart(t)
		})
	}
}

func runFullOutbox(t *testing.T, ch *clearinghouseFixture, f *paidFixture) {
	t.Run("full outbox and recovery", func(t *testing.T) {
		ch.config["EnableKafka"] = false
		restoreAccounting := pauseAccounting(t, ch)
		restore := func() { ch.config["EnableKafka"] = true; restoreAccounting() }
		defer restore()
		opts := f.signer.opts
		opts.kafka = cloneMap(opts.kafka)
		opts.kafka["OutboxDB"] = filepath.Join(t.TempDir(), "full-outbox.sqlite")
		opts.kafka["OutboxMaxBytes"] = int64(10000)
		s := startSigner(t, opts)
		k := ch.key(t, "eth", "1")
		var denied *paymentJob
		for range 16 {
			j := f.job(t, "fixed", "persistent")
			j.signer = s
			j.obtainChallenge(t)
			status, body := j.generate(t, k.headers())
			if status == 503 {
				assertNoCredentials(t, body)
				denied = j
				break
			}
			require.Equal(t, 200, status, string(body))
			require.NoError(t, json.Unmarshal(body, &j.payment))
		}
		require.NotNil(t, denied, "bounded issuance must fill the durable outbox")
		require.Positive(t, outboxCount(t, opts.kafka["OutboxDB"].(string)))
		restore()
		eventually(t, func() bool { return outboxCount(t, opts.kafka["OutboxDB"].(string)) == 0 }, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond)
		denied.sign(t, k.headers())
		ch.paymentEvent(t, denied.payment)
		session := denied.submit(t)
		stopPaidSession(t, f.orch, denied.route, session.SessionID)
	})
}

func malformedEvent(raw []byte, change string) []byte {
	var event map[string]any
	_ = json.Unmarshal(raw, &event)
	event["id"] = uuid.NewString()
	data := event["data"].(map[string]any)
	switch change {
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
	result, _ := json.Marshal(event)
	return result
}
