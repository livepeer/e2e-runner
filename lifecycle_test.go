package e2escope

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runPaidLifecycle(t *testing.T, f *paidFixture) {
	for _, scenario := range []string{"stop", "unregister", "failed health", "cancellation", "shutdown"} {
		t.Run("paid lifecycle/"+scenario, func(t *testing.T) {
			var healthy atomic.Bool
			healthy.Store(true)
			headers := make(chan http.Header, 8)
			started, canceled := make(chan struct{}), make(chan struct{})
			upstream := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					if !healthy.Load() {
						w.WriteHeader(503)
					}
					return
				}
				if r.URL.Path == "/wait" {
					close(started)
					<-r.Context().Done()
					close(canceled)
					return
				}
				select {
				case headers <- r.Header.Clone():
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"paid":true,"prompt":"hello"}`))
			}))
			cfg := staticRunnerConfig{Runners: []staticRunnerConfigEntry{{Label: "paid-lifecycle", Routing: "label", RunnerURL: upstream.URL, HealthURL: upstream.URL + "/health", App: liveRunnerApp, Metadata: "paid-lifecycle", Mode: "persistent", Capacity: 2, Proxy: true, PriceInfo: priceInfo{Price: "3.6", Currency: "usd", Unit: "hour"}}}}
			opts := orchestratorOptions{backend: f.orch.backend, chain: f.chain, account: f.recipient}
			if scenario != "unregister" && scenario != "cancellation" {
				opts.staticConfigPath = writeStaticConfig(t, cfg)
			}
			orch := startOrchestrator(t, opts)
			route := "paid-lifecycle"
			var reg heartbeatResponse
			var stopHeartbeat func()
			defer func() {
				if stopHeartbeat != nil {
					stopHeartbeat()
				}
			}()
			if opts.staticConfigPath == "" {
				payload := heartbeatPayload{Label: route, RunnerURL: upstream.URL, App: liveRunnerApp, Mode: "persistent", Capacity: 2, Proxy: true, Status: "ready", Metadata: route, PriceInfo: priceInfo{Price: "3.6", Currency: "usd", Unit: "hour"}}
				reg = registerRunnerHTTP(t, orch, payload, liveRunnerSecret)
				route = reg.RunnerID
				payload.RunnerID = route
				stopHeartbeat = maintainRunner(t, orch, reg, payload)
			}
			waitRunnerByMetadata(t, orch, "paid-lifecycle")
			signer := startSigner(t, signerOptions{backend: f.signer.backend, chain: f.chain, account: f.payer, orch: orch})
			j := &paymentJob{signer: signer, orch: orch, route: route, unit: "hour", mode: "persistent", app: liveRunnerApp}
			j.obtainChallenge(t)
			j.sign(t, nil)
			session := j.submit(t)
			h := requireReceiveHeader(t, headers)
			control := h.Get("Livepeer-Session-Control")
			token := map[string]string{"Livepeer-Session-Token": h.Get("Livepeer-Session-Token")}
			require.NotEmpty(t, control)
			status, body := httpJSON(t, "POST", control+"/channels", map[string]any{"channels": []any{map[string]string{"name": "events", "mime_type": "application/json"}}}, token)
			require.Equal(t, 200, status, string(body))
			var channels channelResponse
			require.NoError(t, json.Unmarshal(body, &channels))
			require.Len(t, channels.Channels, 1)
			channel := channels.Channels[0].URL
			requireStatus(t, "POST", channel+"/0", strings.NewReader(`{"event":true}`), 200)
			status, body = httpJSON(t, "POST", control+"/proxy", map[string]string{"target_url": upstream.URL}, token)
			require.Equal(t, 200, status, string(body))
			var proxy struct {
				ProxyID  string `json:"proxy_id"`
				ProxyURL string `json:"url"`
			}
			require.NoError(t, json.Unmarshal(body, &proxy))
			requireStatus(t, "POST", proxy.ProxyURL, nil, 200)

			switch scenario {
			case "stop":
				stopPaidSession(t, orch, route, session.SessionID)
			case "unregister":
				stopHeartbeat()
				stopHeartbeat = nil
				unregisterPaidRunner(t, orch, reg.RunnerID, reg.HeartbeatSecret)
			case "failed health":
				healthy.Store(false)
				requireRunnerAbsent(t, orch, route)
			case "cancellation":
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					req, err := http.NewRequestWithContext(ctx, "POST", session.AppURL+"/wait", nil)
					if err == nil {
						var resp *http.Response
						resp, err = testHTTPClient.Do(req)
						if resp != nil {
							resp.Body.Close()
						}
					}
					done <- err
				}()
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					cancel()
					t.Fatal("paid upstream request never started")
				}
				cancel()
				require.True(t, errors.Is(<-done, context.Canceled))
				select {
				case <-canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("paid upstream did not observe cancellation")
				}
				requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, route), 2, 1, 1)
				stopPaidSession(t, orch, route, session.SessionID)
			case "shutdown":
				orch.process.stop()
				orch = startOrchestrator(t, orch.opts)
				waitRunnerByMetadata(t, orch, "paid-lifecycle")
				for _, target := range []*string{&session.AppURL, &proxy.ProxyURL, &channel, &control} {
					u, err := url.Parse(*target)
					require.NoError(t, err)
					*target = orch.httpBase + u.RequestURI()
				}
			}
			if stopHeartbeat != nil {
				stopHeartbeat()
				stopHeartbeat = nil
			}
			requireStatus(t, "POST", session.AppURL, nil, 404)
			requireStatus(t, "POST", proxy.ProxyURL, nil, 404)
			requireStatus(t, "POST", channel+"/1", strings.NewReader(`{}`), 404)
			status, _ = httpJSON(t, "POST", control+"/channels", map[string]any{"channels": []any{map[string]string{"name": "late", "mime_type": "application/json"}}}, token)
			require.Contains(t, []int{403, 404}, status, "closed session credentials cannot create a channel")
			if scenario != "failed health" && scenario != "unregister" {
				requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, route), 2, 0, 2)
			}
		})
	}
}
