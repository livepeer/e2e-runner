package e2escope

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runPriceRegistration(t *testing.T, f *paidFixture) {
	cases := []struct {
		name  string
		value any
	}{
		{"missing", nil}, {"empty", map[string]any{}},
		{"zero", map[string]any{"price": 0, "currency": "usd", "unit": "fixed"}},
		{"negative", map[string]any{"price": -1, "currency": "usd", "unit": "fixed"}},
		{"malformed", map[string]any{"price": "not-a-price", "currency": "usd", "unit": "fixed"}},
		{"currency", map[string]any{"price": 1, "currency": "eur", "unit": "fixed"}},
		{"unit", map[string]any{"price": 1, "currency": "usd", "unit": "bananas"}},
	}
	for _, tc := range cases {
		t.Run("registration/"+tc.name, func(t *testing.T) {
			payload := map[string]any{"runner_id": "invalid-" + tc.name, "runner_url": f.upstream.url, "app": liveRunnerApp, "mode": "persistent", "capacity": 1, "status": "ready", "metadata": "invalid-" + tc.name}
			if tc.value != nil {
				payload["price_info"] = tc.value
			}
			status, body := httpJSON(t, "POST", f.orch.httpBase+"/runners/heartbeat", payload, map[string]string{"Authorization": liveRunnerSecret})
			require.Equal(t, 400, status, string(body))
			requireRunnerMetadataAbsent(t, f.orch, "invalid-"+tc.name)
			reg := f.register(t, "update-"+tc.name, "persistent", "fixed", false)
			t.Cleanup(func() { unregisterPaidRunner(t, f.orch, reg.RunnerID, reg.HeartbeatSecret) })
			before := requireDiscoveredRunner(t, f.orch, reg.RunnerID).PriceInfo
			payload["runner_id"] = reg.RunnerID
			status, body = httpJSON(t, "POST", f.orch.httpBase+"/runners/heartbeat", payload, map[string]string{"Authorization": reg.HeartbeatSecret})
			require.Equal(t, 400, status, string(body))
			require.Equal(t, before, requireDiscoveredRunner(t, f.orch, reg.RunnerID).PriceInfo, "rejected heartbeat preserves price")
		})
	}
	for _, tc := range cases {
		t.Run("runtime price/"+tc.name, func(t *testing.T) {
			entry := map[string]any{"label": "invalid-runtime", "routing": "label", "runner_url": f.upstream.url, "health_url": f.upstream.url + "/health", "app": liveRunnerApp, "mode": "persistent", "capacity": 1, "metadata": "invalid-runtime"}
			if tc.value != nil {
				entry["price_info"] = tc.value
			}
			status, body := httpJSON(t, "POST", f.orch.cliBase+"/registerLiveRunners", map[string]any{"runners": []any{entry}}, nil)
			require.Equal(t, 400, status, string(body))
			requireRunnerMetadataAbsent(t, f.orch, "invalid-runtime")
		})
		t.Run("startup price/"+tc.name, func(t *testing.T) {
			entry := map[string]any{"label": "invalid-static", "routing": "label", "runner_url": f.upstream.url, "health_url": f.upstream.url + "/health", "app": liveRunnerApp, "mode": "persistent", "capacity": 1, "metadata": "invalid-static"}
			if tc.value != nil {
				entry["price_info"] = tc.value
			}
			dir := t.TempDir()
			path := filepath.Join(dir, "runners.json")
			if f.orch.backend == "node" {
				path = filepath.Join(dir, "runners.toml")
				value := map[string]any{"ID": "invalid-static", "RunnerURL": f.upstream.url, "HealthURL": f.upstream.url + "/health", "App": liveRunnerApp, "Mode": "persistent", "Capacity": 1, "Metadata": "invalid-static"}
				if p, ok := tc.value.(map[string]any); ok {
					price := map[string]any{}
					for k, v := range p {
						if k == "price" {
							price["Price"] = v
						}
						if k == "currency" {
							price["Currency"] = v
						}
						if k == "unit" {
							price["Unit"] = v
						}
					}
					value["PriceInfo"] = price
				}
				writeTOML(t, path, map[string]any{"Runners": []any{value}})
			} else {
				data, err := json.Marshal(map[string]any{"runners": []any{entry}})
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, data, 0600))
			}
			opts := f.orchestratorOptions()
			opts.staticConfigPath, opts.noReady = path, true
			orch := startOrchestrator(t, opts)
			// Rejection may terminate startup or leave the server running without the
			// invalid runner. Accept neither registration nor a hung startup as rejection.
			deadline := time.Now().Add(envDuration(t, "E2E_READY_TIMEOUT", "30s"))
			for time.Now().Before(deadline) {
				select {
				case err := <-orch.process.done:
					require.Error(t, err, "invalid startup must not exit successfully")
					return
				default:
				}
				resp, err := testHTTPClient.Get(orch.httpBase + "/discovery")
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode == 200 {
						requireRunnerMetadataAbsent(t, orch, "invalid-static")
						return
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Fatalf("invalid static price did not produce a bounded rejection: %s", readLog(t, orch.logPath))
		})
	}
	for _, unit := range []string{"fixed", "hour"} {
		for _, runtime := range []bool{false, true} {
			name := "startup valid price/" + unit
			if runtime {
				name = "runtime valid price/" + unit
			}
			t.Run(name, func(t *testing.T) {
				price := "0.001"
				if unit == "hour" {
					price = "3.6"
				}
				route := "valid-static-" + unit
				cfg := staticRunnerConfig{Runners: []staticRunnerConfigEntry{{Label: route, Routing: "label", RunnerURL: f.upstream.url, HealthURL: f.upstream.url + "/health", App: liveRunnerApp, Mode: "persistent", Capacity: 2, Metadata: route, PriceInfo: priceInfo{Price: json.Number(price), Currency: "usd", Unit: unit}}}}
				orch := f.orch
				if runtime {
					registerStaticRunnerConfig(t, orch, cfg, 200)
				} else {
					opts := f.orchestratorOptions()
					opts.staticConfigPath = writeStaticConfig(t, cfg)
					orch = startOrchestrator(t, opts)
				}
				runner := waitRunnerByMetadata(t, orch, route)
				require.Equal(t, "wei", runner.PriceInfo.Currency)
				j := &paymentJob{signer: f.signer, orch: orch, route: route, app: liveRunnerApp, mode: "persistent", unit: unit}
				j.obtainChallenge(t)
				j.sign(t, nil)
				session := j.submit(t)
				stopPaidSession(t, orch, route, session.SessionID)
			})
		}
	}
	t.Run("session agreed price", func(t *testing.T) {
		reg := f.register(t, "stable-price", "persistent", "hour", false)
		t.Cleanup(func() { unregisterPaidRunner(t, f.orch, reg.RunnerID, reg.HeartbeatSecret) })
		j := &paymentJob{signer: f.signer, orch: f.orch, route: reg.RunnerID, mode: "persistent", unit: "hour", app: liveRunnerApp}
		j.obtainChallenge(t)
		j.sign(t, nil)
		session := j.submit(t)
		defer stopPaidSession(t, f.orch, j.route, session.SessionID)
		oldInfo := decode64(t, j.challenge.PaymentParams)
		oldPrice := wireBytes(t, oldInfo, 3)
		for _, price := range []priceInfo{{}, {Price: "0", Currency: "usd", Unit: "hour"}} {
			requireHeartbeatStatus(t, f.orch, heartbeatPayload{RunnerID: reg.RunnerID, RunnerURL: f.upstream.url, Mode: "persistent", App: liveRunnerApp, Capacity: 2, Status: "ready", PriceInfo: price}, reg.HeartbeatSecret, 400)
			j.sign(t, nil)
			status, body := httpJSON(t, "POST", f.orch.httpBase+"/apps/"+j.route+"/session/"+session.SessionID+"/payment", nil, j.headers())
			require.Equal(t, 200, status, string(body))
			challenge := decodeChallenge(t, body)
			require.Equal(t, oldPrice, wireBytes(t, decode64(t, challenge.PaymentParams), 3))
		}
		registerRunnerHTTP(t, f.orch, heartbeatPayload{RunnerID: reg.RunnerID, RunnerURL: f.upstream.url, Mode: "persistent", App: liveRunnerApp, Capacity: 2, Status: "ready", PriceInfo: priceInfo{Price: "7.2", Currency: "usd", Unit: "hour"}}, reg.HeartbeatSecret)
		j.sign(t, nil)
		status, body := httpJSON(t, "POST", f.orch.httpBase+"/apps/"+j.route+"/session/"+session.SessionID+"/payment", nil, j.headers())
		require.Equal(t, 200, status, string(body))
		old := decodeChallenge(t, body)
		require.Equal(t, oldPrice, wireBytes(t, decode64(t, old.PaymentParams), 3))
		fresh := *j
		fresh.payment = signedPayment{}
		fresh.obtainChallenge(t)
		require.NotEqual(t, oldPrice, wireBytes(t, decode64(t, fresh.challenge.PaymentParams), 3))
	})
	t.Run("exact conversion", func(t *testing.T) {
		for _, unit := range []string{"fixed", "hour"} {
			j := f.job(t, unit, "persistent")
			j.obtainChallenge(t)
			p := wireBytes(t, decode64(t, j.challenge.PaymentParams), 3)
			rate, ok := new(big.Int).SetString(envDefault("E2E_WEI_PER_USD", "1000000000000000"), 10)
			require.True(t, ok)
			if f.orch.backend == "go-livepeer" {
				answer := f.chain.call(t, "aggregator", f.chain.feed, "latestRoundData")[1].(*big.Int)
				rate.Quo(new(big.Int).Exp(big.NewInt(10), big.NewInt(26), nil), answer)
			}
			require.Equal(t, new(big.Int).Quo(rate, big.NewInt(1000)).Uint64(), wireVarint(t, p, 1)/wireVarint(t, p, 2))
		}
	})
	t.Run("SDK missing price", func(t *testing.T) {
		runPythonRunnerMissingPrice(t, f.orch, liveRunnerSecret, f.upstream.url, "persistent", "missing-sdk")
	})
}

func rejectPayment(t *testing.T, j *paymentJob, payment []byte) {
	t.Helper()
	headers := j.headers()
	headers["Livepeer-Payment"] = base64.StdEncoding.EncodeToString(payment)
	status, body := httpJSON(t, http.MethodPost, j.target(), nil, headers)
	require.GreaterOrEqual(t, status, 400, fmt.Sprintf("invalid tickets admitted: %s", body))
}
