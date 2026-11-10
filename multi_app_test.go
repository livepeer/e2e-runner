package e2escope

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	liverunner "github.com/livepeer/golang-runner"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/python_capacity_failover.py
var pythonCapacityFailover string

func requireRunnerHTTPError(t *testing.T, err error, status int) {
	t.Helper()
	var httpErr *liverunner.HTTPError
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, status, httpErr.StatusCode)
}

func runPaidMultiAppIsolation(t *testing.T, f *paidFixture) {
	for _, mode := range []string{liverunner.ModePersistent, liverunner.ModeSingleShot} {
		t.Run("sibling-"+mode, func(t *testing.T) {
			hb := newHeartbeatFixture(t, f.orch)
			gate, release := heartbeatGate(t)
			entered := make(chan struct{}, 1)
			var calls atomic.Int64
			backend := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				calls.Add(1)
				if req.URL.Path == "/hold" {
					entered <- struct{}{}
					waitHeartbeatGate(req.Context(), gate)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"paid":true,"prompt":"hello"}`))
			}))
			t.Cleanup(release)
			a, b := hb.appOptions(liverunner.ModePersistent), hb.appOptions(mode)
			a.App, b.App = "e2e/multi/live", "e2e/multi/fixed"
			a.RunnerURL, b.RunnerURL, a.Capacity, b.Capacity = backend.URL, backend.URL, 1, 1
			a.PriceInfo, b.PriceInfo = liverunner.PriceInfo{Price: "3.6", Currency: "usd", Unit: "hour"}, liverunner.PriceInfo{Price: "0.002", Currency: "usd", Unit: "fixed"}
			r := startHeartbeatRunner(t, hb.options([]liverunner.AppOptions{a, b}))
			confirmedHeartbeat(t, hb)
			ids := r.AppIDs()
			require.Len(t, ids, 2)
			require.NotEqual(t, ids[0], ids[1])
			require.NotContains(t, ids, r.RunnerID(), "owner identity must be separate from app routes")
			a.ID, b.ID = ids[0], ids[1]
			ja := &paymentJob{signer: f.signer, orch: f.orch, route: a.ID, mode: a.Mode, unit: "hour", app: a.App}
			jb := &paymentJob{signer: f.signer, orch: f.orch, route: b.ID, mode: b.Mode, unit: "fixed", app: b.App}
			ja.obtainChallenge(t)
			jb.obtainChallenge(t)
			priceA := wireBytes(t, decode64(t, ja.challenge.PaymentParams), 3)
			priceB := wireBytes(t, decode64(t, jb.challenge.PaymentParams), 3)
			require.NotEqual(t, priceA, priceB)
			ja.signWhenReady(t, nil)
			jb.signWhenReady(t, nil)
			for _, swap := range []struct{ target, source *paymentJob }{{ja, jb}, {jb, ja}} {
				status, body := httpJSON(t, "POST", swap.target.target(), map[string]string{"prompt": "hello"}, swap.source.headers())
				require.Equal(t, 403, status, string(body))
			}
			require.Zero(t, calls.Load(), "cross-app credentials must never reach the backend")
			sa := ja.submit(t)
			ea := awaitSessionEvent(t, hb.reserve, sa.SessionID)
			var sb sessionResponse
			finished := make(chan error, 1)
			if mode == liverunner.ModePersistent {
				sb = jb.submit(t)
			} else {
				sb.SessionID = jb.challenge.ManifestID
				go func() {
					status, body, err := requestJSON("POST", jb.target()+"/hold", map[string]string{"prompt": "hello"}, jb.headers())
					if err == nil && status != 200 {
						err = fmt.Errorf("held paid request: HTTP %d: %s", status, body)
					}
					finished <- err
				}()
				requireReceiveN(t, entered, 1)
			}
			eb := awaitSessionEvent(t, hb.reserve, sb.SessionID)
			require.Equal(t, a.ID, ea.AppID)
			require.Equal(t, b.ID, eb.AppID)
			require.True(t, ea.SessionToken != eb.SessionToken, "sibling sessions require different credentials")
			requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, a.ID), 1, 1, 0)
			requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, b.ID), 1, 1, 0)
			before := calls.Load()
			for _, job := range []*paymentJob{ja, jb} {
				status, body := httpJSON(t, "POST", job.target(), nil, map[string]string{"Livepeer-Payer-Address": f.payer.address.Hex()})
				require.Equal(t, 503, status, string(body))
				require.NotContains(t, string(body), "payment_params")
				require.NotContains(t, string(body), "manifest_id")
			}
			require.Equal(t, before, calls.Load())
			controlA, controlB := sessionControlRequest(t, f.orch, ea), sessionControlRequest(t, f.orch, eb)
			channelsA, err := r.CreateTrickleChannels(t.Context(), controlA, []liverunner.ChannelRequest{{Name: "events", MimeType: "text/plain"}})
			require.NoError(t, err)
			channelsB, err := r.CreateTrickleChannels(t.Context(), controlB, []liverunner.ChannelRequest{{Name: "events", MimeType: "text/plain"}})
			require.NoError(t, err)
			require.Len(t, channelsA, 1)
			require.Len(t, channelsB, 1)
			require.NotEqual(t, channelsA[0].ChannelName, channelsB[0].ChannelName)
			wrong := controlB.Clone(t.Context())
			wrong.Header.Set(liverunner.HeaderSessionToken, ea.SessionToken)
			_, err = r.DeleteTrickleChannels(t.Context(), wrong, []string{"events"})
			requireRunnerHTTPError(t, err, 401)
			_, err = r.CreateProxy(t.Context(), wrong, backend.URL)
			requireRunnerHTTPError(t, err, 401)
			requireRunnerHTTPError(t, r.StopSession(t.Context(), b.ID, ea.SessionID, ea.SessionToken), 404)
			requireRunnerHTTPError(t, r.StopSession(t.Context(), a.ID, ea.SessionID, eb.SessionToken), 401)
			proxy, err := r.CreateProxy(t.Context(), controlA, backend.URL)
			require.NoError(t, err)
			// Heartbeat pricing changes apply to future work, while live payment
			// continuation remains bound to the price agreed by A's session.
			a.PriceInfo.Price, b.PriceInfo.Price = "7.2", "0.004"
			require.NoError(t, r.SetApps([]liverunner.AppOptions{a, b}))
			awaitHeartbeat(t, hb, func(e liverunner.HeartbeatEvent) bool {
				return e.Err == nil && len(e.Request.Apps) == 2 && e.Request.Apps[0].PriceInfo.Price == "7.2"
			})
			require.Equal(t, ids, r.AppIDs())
			for _, app := range []liverunner.AppOptions{a, b} {
				d := requireDiscoveredRunner(t, f.orch, app.ID)
				require.Equal(t, app.App, d.App)
				require.NotNil(t, d.PriceInfo)
				require.Equal(t, "wei", d.PriceInfo.Currency)
			}
			renew := func() {
				ja.sign(t, nil)
				status, body := httpJSON(t, "POST", f.orch.httpBase+"/apps/"+a.ID+"/session/"+sa.SessionID+"/payment", nil, ja.headers())
				require.Equal(t, 200, status, string(body))
				ack := decodeChallenge(t, body)
				require.Equal(t, priceA, wireBytes(t, decode64(t, ack.PaymentParams), 3), "A's agreed price survives heartbeat updates and withdrawal")
			}
			renew()
			require.NoError(t, r.SetApps([]liverunner.AppOptions{b}))
			withdrawn := awaitHeartbeat(t, hb, func(e liverunner.HeartbeatEvent) bool {
				return e.Err == nil && len(e.Request.Apps) == 1 && len(e.Response.Apps) == 2
			})
			require.Equal(t, b.ID, withdrawn.Response.Apps[0].ID)
			require.Equal(t, a.ID, withdrawn.Response.Apps[1].ID)
			require.Equal(t, "draining", withdrawn.Response.Apps[1].Status)
			requireRunnerAbsent(t, f.orch, a.ID)
			requireStatus(t, "POST", ja.target(), nil, 404)
			renew()
			requireStatus(t, "POST", sa.AppURL+"/draining", nil, 200)
			requireStatus(t, "POST", proxy.URL+"/draining", nil, 200)
			for _, ch := range []liverunner.Channel{channelsA[0], channelsB[0]} {
				requireStatus(t, "POST", ch.URL+"/0", strings.NewReader(ch.ChannelName), 200)
				body, err := checkStatus("GET", ch.URL+"/0", nil, 200)
				require.NoError(t, err)
				require.Equal(t, ch.ChannelName, body)
			}
			if mode == liverunner.ModePersistent {
				requireStatus(t, "POST", sb.AppURL+"/sibling-survives", nil, 200)
				require.NoError(t, r.StopSession(t.Context(), b.ID, sb.SessionID, eb.SessionToken))
			} else {
				release()
				require.NoError(t, <-finished)
			}
			freshB := &paymentJob{signer: f.signer, orch: f.orch, route: b.ID, mode: b.Mode, unit: "fixed", app: b.App}
			freshB.obtainChallenge(t)
			newPrice := wireBytes(t, decode64(t, freshB.challenge.PaymentParams), 3)
			require.Equal(t, 2*wireVarint(t, priceB, 1)/wireVarint(t, priceB, 2), wireVarint(t, newPrice, 1)/wireVarint(t, newPrice, 2))
			freshB.sign(t, nil)
			freshSession := freshB.submit(t)
			require.NoError(t, r.StopSession(t.Context(), a.ID, sa.SessionID, ea.SessionToken))
			awaitHeartbeat(t, hb, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Response.Apps) == 1 })
			requireStatus(t, "POST", proxy.URL+"/after-drain", nil, 404)
			requireStatus(t, "GET", channelsA[0].URL+"/0", nil, 404)
			if mode == liverunner.ModePersistent {
				requireStatus(t, "POST", freshSession.AppURL+"/after-a-drained", nil, 200)
			}
		})
	}
}

func runPythonCapacityFailover(t *testing.T, f *paidFixture) {
	other := startOrchestrator(t, f.orchestratorOptions())
	for _, mode := range []string{liverunner.ModePersistent, liverunner.ModeSingleShot} {
		t.Run(mode, func(t *testing.T) {
			gate, release := heartbeatGate(t)
			entered := make(chan struct{}, 1)
			var busyCalls, backupCalls, signatures atomic.Int64
			backend := func(counter *atomic.Int64) http.HandlerFunc {
				return func(w http.ResponseWriter, req *http.Request) {
					counter.Add(1)
					if req.URL.Path == "/hold" {
						entered <- struct{}{}
						waitHeartbeatGate(req.Context(), gate)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"paid":true,"prompt":"hello"}`))
				}
			}
			busyApp, backupApp := newFixtureServer(t, backend(&busyCalls)), newFixtureServer(t, backend(&backupCalls))
			t.Cleanup(release)
			first, second := newHeartbeatFixture(t, f.orch), newHeartbeatFixture(t, other)
			app := first.appOptions(mode)
			app.App, app.RunnerURL, app.Capacity = "e2e/capacity/"+mode, busyApp.URL, 1
			app.PriceInfo = liverunner.PriceInfo{Price: "0.001", Currency: "usd", Unit: "fixed"}
			r1 := startHeartbeatRunner(t, first.options([]liverunner.AppOptions{app}))
			app.RunnerURL = backupApp.URL
			r2 := startHeartbeatRunner(t, second.options([]liverunner.AppOptions{app}))
			confirmedHeartbeat(t, first)
			confirmedHeartbeat(t, second)
			signerURL, err := url.Parse(f.signer.base)
			require.NoError(t, err)
			signerProxy := httputil.NewSingleHostReverseProxy(signerURL)
			signerProxy.ModifyResponse = func(resp *http.Response) error {
				if resp.Request.URL.Path == "/generate-live-payment" {
					signatures.Add(1)
				}
				return nil
			}
			pythonSigner := newFixtureServer(t, signerProxy)
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			cmd := fixtureCommand(t, ctx, "python-capacity-failover", "uv", repoDir(t, envPythonRunnerDir), "run", "--frozen", "python", "-u", "-c", pythonCapacityFailover)
			input, err := cmd.StdinPipe()
			require.NoError(t, err)
			output, err := cmd.StdoutPipe()
			require.NoError(t, err)
			var stderr lockedBuffer
			cmd.Stderr = &stderr
			require.NoError(t, cmd.Start())
			t.Cleanup(func() {
				_ = input.Close()
				if cmd.ProcessState == nil {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			})
			spec := map[string]any{"orchestrators": []string{f.orch.httpBase, other.httpBase}, "signer": pythonSigner.URL, "payer": f.payer.address.Hex(), "app": app.App, "mode": mode}
			require.NoError(t, json.NewEncoder(input).Encode(spec))
			scanner := bufio.NewScanner(output)
			require.True(t, scanner.Scan(), "Python selector did not finish discovery: %s", stderr.String())
			require.Equal(t, "discovered", scanner.Text())
			// Fill the first candidate only after Python has discovered both.
			job := &paymentJob{signer: f.signer, orch: f.orch, route: r1.AppIDs()[0], app: app.App, mode: mode, unit: "fixed"}
			job.obtainChallenge(t)
			job.signWhenReady(t, nil)
			finished := make(chan error, 1)
			if mode == liverunner.ModePersistent {
				job.submit(t)
			} else {
				go func() {
					status, body, err := requestJSON("POST", job.target()+"/hold", nil, job.headers())
					if err == nil && status != 200 {
						err = fmt.Errorf("busy fixture: HTTP %d: %s", status, body)
					}
					finished <- err
				}()
				requireReceiveN(t, entered, 1)
			}
			before := busyCalls.Load()
			_, err = fmt.Fprintln(input, "continue")
			require.NoError(t, err)
			_ = input.Close()
			var lines []string
			for scanner.Scan() {
				lines = append(lines, scanner.Text())
			}
			require.NoError(t, scanner.Err())
			err = cmd.Wait()
			t.Logf("Python capacity failover: %s", strings.Join(lines, "\n"))
			require.NoError(t, err, "Python failover (https://github.com/livepeer/livepeer-python-gateway/issues/93): %s", stderr.String())
			require.Equal(t, before, busyCalls.Load(), "rejected candidate must not reach its backend")
			require.EqualValues(t, 1, backupCalls.Load(), "the healthy orchestrator serves the job")
			require.EqualValues(t, 1, signatures.Load(), "only the healthy candidate obtains a signed payment")
			requireDiscoveredRunner(t, other, r2.AppIDs()[0])
			if mode == liverunner.ModeSingleShot {
				release()
				require.NoError(t, <-finished)
			}
		})
	}
}
