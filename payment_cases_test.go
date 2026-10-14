package e2escope

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func runExtendedPayments(t *testing.T, f *paidFixture) {
	t.Run("discovery filtering", func(t *testing.T) {
		reg := registerRunnerHTTP(t, f.orch, heartbeatPayload{Label: "gpu-discovery", RunnerURL: f.upstream.url, Mode: "persistent", App: liveRunnerApp, Metadata: "gpu-discovery", Capacity: 2, Status: "ready", GPU: map[string]any{"name": "E2E GPU", "vram_mb": 24576}, PriceInfo: priceInfo{Price: "0.001", Currency: "usd", Unit: "fixed"}}, liveRunnerSecret)
		t.Cleanup(func() { unregisterPaidRunner(t, f.orch, reg.RunnerID, reg.HeartbeatSecret) })
		stop := maintainRunner(t, f.orch, reg, heartbeatPayload{RunnerID: reg.RunnerID, RunnerURL: f.upstream.url, Mode: "persistent", App: liveRunnerApp, Metadata: "gpu-discovery", Capacity: 2, Status: "ready", GPU: map[string]any{"name": "E2E GPU", "vram_mb": 24576}, PriceInfo: priceInfo{Price: "0.001", Currency: "usd", Unit: "fixed"}})
		t.Cleanup(stop)
		for _, query := range []struct {
			query   string
			present bool
		}{{"app=" + url.QueryEscape(liveRunnerApp), true}, {"app=absent", false}, {"gpu=E2E+GPU", true}, {"gpu=absent", false}} {
			t.Run(query.query, func(t *testing.T) {
				var entries []discoveryEntry
				eventually(t, func() bool {
					status, body := httpJSON(t, "GET", f.signer.base+"/discover-orchestrators?"+query.query, nil, nil)
					if status != 200 {
						return false
					}
					require.NoError(t, json.Unmarshal(body, &entries))
					found := false
					for _, e := range entries {
						for _, r := range e.Runners {
							if r.Metadata == "gpu-discovery" {
								found = true
								require.NotNil(t, r.PriceInfo)
								require.Equal(t, "wei", r.PriceInfo.Currency)
							}
						}
					}
					return found == query.present
				}, envDuration(t, "E2E_DISCOVERY_TIMEOUT", "30s"), 200*time.Millisecond, "application/GPU filter")
			})
		}
	})
	t.Run("fixed per request and live replenishment", func(t *testing.T) {
		for _, unit := range []string{"fixed", "hour"} {
			t.Run(unit, func(t *testing.T) {
				j := f.job(t, unit, "persistent")
				j.obtainChallenge(t)
				j.sign(t, nil)
				s := j.submit(t)
				if unit == "fixed" {
					defer stopPaidSession(t, f.orch, j.route, s.SessionID)
				}
				endpoint := f.orch.httpBase + "/apps/" + j.route + "/session/" + s.SessionID + "/payment"
				if unit == "fixed" {
					status, body := httpJSON(t, "POST", endpoint, nil, j.headers())
					require.Equal(t, 409, status, string(body))
					return
				}
				first := stateObject(t, j.payment)
				require.NotEmpty(t, first["Balance"])
				time.Sleep(1100 * time.Millisecond)
				j.sign(t, nil)
				status, body := httpJSON(t, "POST", endpoint, nil, j.headers())
				require.Equal(t, 200, status, string(body))
				requireStatus(t, "POST", s.AppURL, nil, 200)
				// The same recipient submission must not buy another ten seconds.
				status, body = httpJSON(t, "POST", endpoint, nil, j.headers())
				require.GreaterOrEqual(t, status, 400, string(body))
				eventually(t, func() bool {
					resp, err := testHTTPClient.Post(s.AppURL, "application/json", nil)
					if err != nil {
						return false
					}
					resp.Body.Close()
					return resp.StatusCode >= 400
				}, envDuration(t, "E2E_LIVE_EXHAUSTION_TIMEOUT", "20s"), 200*time.Millisecond, "replayed tickets must not extend live credit")
				requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 0, 2)
			})
		}
	})
	t.Run("no tickets needed", func(t *testing.T) {
		reg := registerRunnerHTTP(t, f.orch, heartbeatPayload{Label: "rounding-credit", RunnerURL: f.upstream.url, Mode: "persistent", App: liveRunnerApp, Capacity: 2, PriceInfo: priceInfo{Price: "0.000000000001", Currency: "usd", Unit: "fixed"}}, liveRunnerSecret)
		t.Cleanup(func() { unregisterPaidRunner(t, f.orch, reg.RunnerID, reg.HeartbeatSecret) })
		j := &paymentJob{signer: f.signer, orch: f.orch, route: reg.RunnerID, mode: "persistent", unit: "fixed", app: liveRunnerApp}
		j.obtainChallenge(t)
		j.sign(t, nil)
		require.Equal(t, 1, ticketCount(t, j.payment))
		// A normal continuation maintains at least one ticket EV of credit. Use
		// an explicit lower-EV parameter fixture to exercise the no-ticket branch
		// with authentic signed state, without inventing or resigning its balance.
		info := decode64(t, j.challenge.PaymentParams)
		params := wireBytes(t, info, 2)
		face := new(big.Int).SetBytes(wireBytes(t, params, 2))
		face.Quo(face, big.NewInt(2))
		require.Positive(t, face.Sign())
		params = replaceWire(t, params, 2, protowire.BytesType, face.Bytes())
		req := j.request()
		req["orchestrator"] = base64.StdEncoding.EncodeToString(replaceWire(t, info, 2, protowire.BytesType, params))
		status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
		require.Equal(t, 482, status, string(body))
		assertNoCredentials(t, body)
	})
	t.Run("recipient ticket rejection", func(t *testing.T) {
		for _, change := range []string{"signature", "payer", "recipient", "face-value", "probability", "nonce", "random-hash", "expiration", "expected-price", "zero-count", "duplicate-ticket"} {
			t.Run(change, func(t *testing.T) {
				j := f.job(t, "fixed", "persistent")
				j.obtainChallenge(t)
				j.sign(t, nil)
				payment := decode64(t, j.payment.Payment)
				params := wireBytes(t, payment, 1)
				ticket := wireBytes(t, payment, 4)
				switch change {
				case "signature":
					sig := wireBytes(t, ticket, 2)
					sig[0] ^= 1
					ticket = replaceWire(t, ticket, 2, protowire.BytesType, sig)
					payment = replaceWire(t, payment, 4, protowire.BytesType, ticket)
				case "payer":
					payment = replaceWire(t, payment, 2, protowire.BytesType, common.HexToAddress("0x1234").Bytes())
				case "recipient":
					params = replaceWire(t, params, 1, protowire.BytesType, common.HexToAddress("0x1234").Bytes())
				case "face-value":
					params = replaceWire(t, params, 2, protowire.BytesType, []byte{1})
				case "probability":
					params = replaceWire(t, params, 3, protowire.BytesType, []byte{0})
				case "nonce":
					ticket = replaceWire(t, ticket, 1, protowire.VarintType, protowire.AppendVarint(nil, 0))
					payment = replaceWire(t, payment, 4, protowire.BytesType, ticket)
				case "random-hash":
					params = replaceWire(t, params, 4, protowire.BytesType, make([]byte, 32))
				case "expiration":
					params = replaceWire(t, params, 6, protowire.BytesType, []byte{1})
				case "expected-price":
					payment = replaceWire(t, payment, 5, protowire.BytesType, []byte{8, 1, 16, 1})
				case "zero-count":
					payment = removeWire(t, payment, 4)
				case "duplicate-ticket":
					payment = protowire.AppendTag(payment, 4, protowire.BytesType)
					payment = protowire.AppendBytes(payment, ticket)
				}
				payment = replaceWire(t, payment, 1, protowire.BytesType, params)
				before := f.upstream.calls.Load()
				rejectPayment(t, j, payment)
				require.Equal(t, before, f.upstream.calls.Load())
				requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 0, 2)
			})
		}
	})
	t.Run("concurrent admission and replay", func(t *testing.T) {
		// Obtain and sign sequentially: signer HTTP retries are not promised to be
		// idempotent. Only the recipient receives concurrent identical submissions.
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		j.sign(t, nil)
		payload, _ := json.Marshal(map[string]string{"prompt": "hello"})
		var wg sync.WaitGroup
		results := make(chan int, 8)
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req, _ := http.NewRequest("POST", j.target(), bytesReader(payload))
				for k, v := range j.headers() {
					req.Header.Set(k, v)
				}
				resp, err := testHTTPClient.Do(req)
				if err != nil {
					results <- 0
					return
				}
				resp.Body.Close()
				results <- resp.StatusCode
			}()
		}
		wg.Wait()
		close(results)
		accepted := 0
		for code := range results {
			require.NotZero(t, code)
			if code == 200 {
				accepted++
			} else {
				require.GreaterOrEqual(t, code, 400)
			}
		}
		require.Equal(t, 1, accepted, "one paid admission for identical credentials")
		requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 1, 1)
		stopPaidSession(t, f.orch, j.route, j.challenge.ManifestID)
	})
	t.Run("nonce threshold and refresh continuation", func(t *testing.T) {
		reg := registerRunnerHTTP(t, f.orch, heartbeatPayload{Label: "nonce-threshold", RunnerURL: f.upstream.url, App: liveRunnerApp, Mode: "persistent", Capacity: 2, PriceInfo: priceInfo{Price: "0.0198", Currency: "usd", Unit: "fixed"}}, liveRunnerSecret)
		t.Cleanup(func() { unregisterPaidRunner(t, f.orch, reg.RunnerID, reg.HeartbeatSecret) })
		j := &paymentJob{signer: f.signer, orch: f.orch, route: reg.RunnerID, mode: "persistent", unit: "fixed", app: liveRunnerApp}
		j.obtainChallenge(t)
		var session sessionResponse
		for range 7 {
			status, body := j.generate(t, nil)
			if status == 480 {
				break
			}
			require.Equal(t, 200, status, string(body))
			require.NoError(t, json.Unmarshal(body, &j.payment))
			if session.SessionID == "" {
				session = j.submit(t)
				defer stopPaidSession(t, f.orch, j.route, session.SessionID)
			}
		}
		status, body := j.generate(t, nil)
		require.Equal(t, 480, status, string(body))
		assertNoCredentials(t, body)
		old := stateObject(t, j.payment)
		status, body = httpJSON(t, "POST", f.orch.httpBase+"/refresh-payment", map[string]string{"sender": f.payer.address.Hex(), "manifest_id": j.challenge.ManifestID}, nil)
		require.Equal(t, 200, status, string(body))
		j.challenge = decodeChallenge(t, body)
		j.sign(t, nil)
		next := stateObject(t, j.payment)
		require.Equal(t, old["StateID"], next["StateID"])
		require.Equal(t, old["ManifestID"], next["ManifestID"])
		require.NotEqual(t, old["PMSessionID"], next["PMSessionID"])
	})
	t.Run("one signer identity per job", func(t *testing.T) {
		j := f.job(t, "hour", "persistent")
		j.obtainChallenge(t)
		j.sign(t, nil)
		s := j.submit(t)
		defer stopPaidSession(t, f.orch, j.route, s.SessionID)
		account := f.chain.newAccount(t, true)
		f.chain.fundPayer(t, account, ethAmount(t, "E2E_PAYER_DEPOSIT_ETH", "0.1"), ethAmount(t, "E2E_PAYER_RESERVE_ETH", "0.1"))
		other := startSigner(t, signerOptions{backend: f.signer.backend, chain: f.chain, account: account, orch: f.orch})
		status, body := httpJSON(t, "POST", other.base+"/generate-live-payment", j.request(), nil)
		require.GreaterOrEqual(t, status, 400, string(body))
		assertNoCredentials(t, body)
		// No cross-implementation handoff: a valid job stays with its original key.
		j.sign(t, nil)
		status, body = httpJSON(t, "POST", f.orch.httpBase+"/apps/"+j.route+"/session/"+s.SessionID+"/payment", nil, j.headers())
		require.Equal(t, 200, status, string(body))
		requireStatus(t, "POST", s.AppURL, nil, 200)
	})
	runFundingCases(t, f)
	runWebhookCases(t, f)
	runConfiguredCeilings(t, f)
	runDistinctConcurrentPayments(t, f)
}

func runConfiguredCeilings(t *testing.T, f *paidFixture) {
	for _, unit := range []string{"fixed", "hour"} {
		for _, delta := range []int64{-1, 0, 1} {
			t.Run("configured ceilings/"+unit+"/"+big.NewInt(delta).String(), func(t *testing.T) {
				j := f.job(t, unit, "persistent")
				j.obtainChallenge(t)
				p := wireBytes(t, decode64(t, j.challenge.PaymentParams), 3)
				limit := new(big.Int).SetUint64(wireVarint(t, p, 1) / wireVarint(t, p, 2))
				limit.Add(limit, big.NewInt(delta))
				rate, ok := new(big.Int).SetString(envDefault("E2E_WEI_PER_USD", "1000000000000000"), 10)
				require.True(t, ok)
				usd := new(big.Rat).SetFrac(limit, rate)
				field := "MaxFixedPrice"
				if unit == "hour" {
					usd.Mul(usd, big.NewRat(3600, 1))
					field = "MaxHourlyPrice"
				}
				opts := signerOptions{backend: f.signer.backend, chain: f.chain, account: f.payer, orch: f.orch, maxPriceWei: limit.String(), extra: map[string]any{field: usd.FloatString(18)}}
				j.signer = startSigner(t, opts)
				status, body := j.generate(t, nil)
				expected := 200
				if delta < 0 {
					expected = 481
				}
				require.Equal(t, expected, status, string(body))
				if status != 200 {
					assertNoCredentials(t, body)
				}
			})
		}
	}
}

// Independent jobs race for two slots; every accepted session must be usable and
// release its capacity. The signer is called once for each job before the race.
func runDistinctConcurrentPayments(t *testing.T, f *paidFixture) {
	t.Run("concurrent independent jobs", func(t *testing.T) {
		first := f.job(t, "fixed", "persistent")
		jobs := []*paymentJob{first}
		for range 2 {
			jobs = append(jobs, &paymentJob{signer: f.signer, orch: f.orch, route: first.route, mode: "persistent", unit: "fixed", app: liveRunnerApp})
		}
		for _, j := range jobs {
			j.obtainChallenge(t)
			j.sign(t, nil)
		}
		type result struct {
			status int
			body   []byte
			err    error
		}
		results := make(chan result, len(jobs))
		for _, j := range jobs {
			go func(j *paymentJob) {
				req, err := http.NewRequest("POST", j.target(), nil)
				if err != nil {
					results <- result{err: err}
					return
				}
				for key, value := range j.headers() {
					req.Header.Set(key, value)
				}
				resp, err := testHTTPClient.Do(req)
				if err != nil {
					results <- result{err: err}
					return
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				results <- result{status: resp.StatusCode, body: body, err: err}
			}(j)
		}
		var sessions []string
		for range jobs {
			r := <-results
			require.NoError(t, r.err)
			if r.status == 200 {
				var session sessionResponse
				require.NoError(t, json.Unmarshal(r.body, &session))
				requireStatus(t, "POST", session.AppURL, nil, 200)
				sessions = append(sessions, session.SessionID)
			} else {
				require.Contains(t, []int{409, 503}, r.status, string(r.body))
			}
		}
		require.Len(t, sessions, 2)
		for _, session := range sessions {
			stopPaidSession(t, f.orch, first.route, session)
		}
		requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, first.route), 2, 0, 2)
	})
}

func ticketCount(t *testing.T, p signedPayment) int {
	n := 0
	wireFields(t, decode64(t, p.Payment), func(field protowire.Number, _ protowire.Type, _ []byte) {
		if field == 4 {
			n++
		}
	})
	return n
}
func removeWire(t *testing.T, data []byte, field protowire.Number) []byte {
	var out []byte
	wireFields(t, data, func(n protowire.Number, k protowire.Type, raw []byte) {
		if n != field {
			out = protowire.AppendTag(out, n, k)
			out = append(out, raw...)
		}
	})
	return out
}
func assertNoCredentials(t *testing.T, body []byte) {
	t.Helper()
	var p signedPayment
	_ = json.Unmarshal(body, &p)
	require.Empty(t, p.Payment)
	require.Empty(t, p.Segment)
}

func runFundingCases(t *testing.T, f *paidFixture) {
	for _, tc := range []struct {
		name             string
		deposit, reserve string
	}{{"no deposit", "0", "0.1"}, {"no reserve", "0.1", "0"}, {"insufficient deposit", "0.000000000000000001", "0.1"}, {"withdrawing", "0.1", "0.1"}} {
		t.Run("funding/"+tc.name, func(t *testing.T) {
			if tc.name == "withdrawing" {
				t.Cleanup(func() {
					f.chain.initializeRound(t, f.payer)
					require.True(t, f.chain.recipientActive(t, f.recipient), "recipient remains active after snapshot restore")
				})
				f.chain.requireControl(t)
			}
			a := f.chain.newAccount(t, true)
			deposit := decimalWei(t, tc.deposit)
			reserve := decimalWei(t, tc.reserve)
			f.chain.fundPayer(t, a, deposit, reserve)
			if tc.name == "withdrawing" {
				f.chain.transact(t, a, "ticketBroker", f.chain.broker, "unlock", new(big.Int))
				_, _, withdrawal := senderEscrow(t, f.chain, a.address)
				length := f.chain.call(t, "roundsManager", f.chain.rounds, "roundLength")[0].(*big.Int)
				target := new(big.Int).Mul(new(big.Int).Sub(withdrawal, big.NewInt(1)), length)
				f.chain.mineTo(t, target.Uint64())
				f.chain.initializeRound(t, a)
			}
			s := startSigner(t, signerOptions{backend: f.signer.backend, chain: f.chain, account: a, orch: f.orch})
			j := f.job(t, "fixed", "persistent")
			j.signer = s
			j.obtainChallenge(t, f.payer.address)
			status, body := j.generate(t, nil)
			require.GreaterOrEqual(t, status, 400, string(body))
			assertNoCredentials(t, body)
			if tc.name == "withdrawing" {
				f.chain.transact(t, a, "ticketBroker", f.chain.broker, "cancelUnlock", new(big.Int))
			} else {
				f.chain.fundPayer(t, a, decimalWei(t, "0.1"), decimalWei(t, "0.1"))
			}
			j.obtainChallenge(t)
			eventually(t, func() bool {
				status, body := j.generate(t, nil)
				if status != 200 {
					return false
				}
				require.NoError(t, json.Unmarshal(body, &j.payment))
				return true
			}, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 300*time.Millisecond, "funding recovery")
			session := j.submit(t)
			stopPaidSession(t, f.orch, j.route, session.SessionID)
		})
	}
}
func decimalWei(t *testing.T, s string) *big.Int {
	r, ok := new(big.Rat).SetString(s)
	require.True(t, ok)
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	require.True(t, r.IsInt())
	return new(big.Int).Set(r.Num())
}
func senderEscrow(t *testing.T, c *chainFixture, a common.Address) (*big.Int, *big.Int, *big.Int) {
	v := c.call(t, "ticketBroker", c.broker, "getSenderInfo", a)
	sender := reflect.Indirect(reflect.ValueOf(v[0]))
	reserve := reflect.Indirect(reflect.ValueOf(v[1]))
	return sender.FieldByName("Deposit").Interface().(*big.Int), reserve.FieldByName("FundsRemaining").Interface().(*big.Int), sender.FieldByName("WithdrawRound").Interface().(*big.Int)
}

func runWebhookCases(t *testing.T, f *paidFixture) {
	for _, name := range []string{"approval", "rejection", "unavailable", "malformed", "cached", "per-request", "identity change", "ceiling below", "ceiling equal", "ceiling above"} {
		t.Run("webhook/"+name, func(t *testing.T) {
			var calls atomic.Int64
			server := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				if name == "unavailable" {
					w.WriteHeader(503)
					return
				}
				if name == "malformed" {
					w.Write([]byte("{"))
					return
				}
				decision := map[string]any{"status": 200, "auth_id": "account-A", "expiry": 0}
				if name == "rejection" {
					decision["status"] = 403
				}
				if name == "cached" {
					decision["expiry"] = time.Now().Add(time.Minute).Unix()
				}
				if name == "identity change" && n > 1 {
					decision["auth_id"] = "account-B"
				}
				if name == "ceiling below" || name == "ceiling equal" || name == "ceiling above" {
					price := int64(1_000_000_000_000)
					if name == "ceiling below" {
						price--
					}
					if name == "ceiling above" {
						price++
					}
					decision["maxPrice"] = map[string]any{"price": price, "currency": "wei", "unit": "fixed"}
				}
				respondJSON(t, w, decision)
			}))
			signer := startSigner(t, signerOptions{backend: f.signer.backend, chain: f.chain, account: f.payer, orch: f.orch, webhook: server.URL})
			j := f.job(t, "fixed", "persistent")
			j.signer = signer
			j.obtainChallenge(t)
			status, body := j.generate(t, nil)
			expected := 200
			switch name {
			case "rejection":
				expected = 403
			case "unavailable", "malformed":
				expected = 502
				if signer.backend == "go-livepeer" {
					expected = 500
				}
			case "ceiling below":
				expected = 481
			}
			require.Equal(t, expected, status, string(body))
			if expected != 200 {
				assertNoCredentials(t, body)
				return
			}
			require.NoError(t, json.Unmarshal(body, &j.payment))
			status, body = j.generate(t, nil)
			if name == "identity change" {
				expected := 403
				if signer.backend == "go-livepeer" {
					expected = 500
				}
				require.Equal(t, expected, status, string(body))
				assertNoCredentials(t, body)
				return
			}
			require.Equal(t, 200, status, string(body))
			if name == "cached" {
				require.Equal(t, int64(1), calls.Load())
			} else {
				require.Equal(t, int64(2), calls.Load())
			}
		})
	}
}

// Keep HTTP request goroutines independent of testing.Fatal/require calls.
func bytesReader(data []byte) *bytes.Reader { return bytes.NewReader(data) }
