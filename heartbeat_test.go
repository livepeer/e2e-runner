package e2escope

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	liverunner "github.com/livepeer/golang-runner"
	"github.com/stretchr/testify/require"
)

// Faults surround a genuine HTTP exchange with a child Node process. A dropped
// response is lost after Node has committed it, rather than simulating rejection.
type heartbeatExchange struct {
	Path       string
	Request    liverunner.HeartbeatRequest
	Raw        map[string]json.RawMessage
	Credential string
	Response   liverunner.HeartbeatResponse
	Status     int
	Body       []byte
	Drop       bool
}

type heartbeatProxy struct {
	server *httptest.Server
	target string
	mu     sync.Mutex
	before func(context.Context, *heartbeatExchange) bool
	after  func(context.Context, *heartbeatExchange)
	o2r    func(context.Context, []byte)
}

func newHeartbeatProxy(t *testing.T, orch *testOrchestrator) *heartbeatProxy {
	t.Helper()
	p := &heartbeatProxy{target: orch.httpBase}
	target, err := url.Parse(p.target)
	require.NoError(t, err)
	reverse := httputil.NewSingleHostReverseProxy(target)
	reverse.ModifyResponse = func(resp *http.Response) error {
		p.mu.Lock()
		hook := p.o2r
		p.mu.Unlock()
		if hook != nil && resp.StatusCode == 200 && strings.Contains(resp.Request.URL.Path, "-o2r/") {
			data, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				return err
			}
			hook(resp.Request.Context(), data)
			resp.Body = io.NopCloser(bytes.NewReader(data))
			resp.ContentLength = int64(len(data))
		}
		return nil
	}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/runners/heartbeat" && !strings.HasSuffix(req.URL.Path, "/unregister") {
			reverse.ServeHTTP(w, req)
			return
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		x := &heartbeatExchange{Path: req.URL.Path, Credential: req.Header.Get("Authorization"), Status: 200}
		_ = json.Unmarshal(data, &x.Request)
		_ = json.Unmarshal(data, &x.Raw)
		p.mu.Lock()
		before, after := p.before, p.after
		p.mu.Unlock()
		intercepted := before != nil && before(req.Context(), x)
		if !intercepted {
			forward := req.Clone(req.Context())
			forward.URL.Scheme, forward.URL.Host = target.Scheme, target.Host
			forward.Host, forward.RequestURI = target.Host, ""
			forward.Body = io.NopCloser(bytes.NewReader(data))
			resp, err := http.DefaultTransport.RoundTrip(forward)
			if err != nil {
				http.Error(w, err.Error(), 502)
				return
			}
			x.Status = resp.StatusCode
			x.Body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				http.Error(w, err.Error(), 502)
				return
			}
			if x.Path == "/runners/heartbeat" && x.Status == 200 {
				_ = json.Unmarshal(x.Body, &x.Response)
				x.Response.Orchestrator = p.server.URL
				if x.Response.O2R != nil {
					x.Response.O2R.URL = strings.Replace(x.Response.O2R.URL, p.target, p.server.URL, 1)
				}
			}
			if after != nil {
				after(req.Context(), x)
			}
		}
		if x.Drop {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		if x.Path == "/runners/heartbeat" && x.Status == 200 {
			x.Body, _ = json.Marshal(x.Response)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(x.Status)
		_, _ = w.Write(x.Body)
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *heartbeatProxy) hooks(before func(context.Context, *heartbeatExchange) bool, after func(context.Context, *heartbeatExchange)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.before, p.after = before, after
}

type heartbeatFixture struct {
	proxy   *heartbeatProxy
	app     *httptest.Server
	events  chan liverunner.HeartbeatEvent
	reserve chan liverunner.SessionEvent
	release chan liverunner.SessionEvent
}

func newHeartbeatFixture(t *testing.T, orch *testOrchestrator) *heartbeatFixture {
	t.Helper()
	f := &heartbeatFixture{
		proxy: newHeartbeatProxy(t, orch), events: make(chan liverunner.HeartbeatEvent, 512),
		reserve: make(chan liverunner.SessionEvent, 64), release: make(chan liverunner.SessionEvent, 64),
	}
	f.app = newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, r.Body)
	}))
	return f
}

func (f *heartbeatFixture) appOptions(mode string) liverunner.AppOptions {
	return liverunner.AppOptions{App: "e2e/heartbeat", RunnerURL: f.app.URL, Mode: mode, Capacity: 4}
}

func (f *heartbeatFixture) options(apps []liverunner.AppOptions) liverunner.Options {
	return liverunner.Options{
		OrchestratorURL: f.proxy.server.URL, BootstrapSecret: liveRunnerSecret, Apps: apps,
		HeartbeatInterval: 40 * time.Millisecond, Timeout: 2 * time.Second,
		OnHeartbeat: func(e liverunner.HeartbeatEvent) { f.events <- e },
		OnReserve:   func(e liverunner.SessionEvent) { f.reserve <- e },
		OnRelease:   func(e liverunner.SessionEvent) { f.release <- e },
	}
}

func startHeartbeatRunner(t *testing.T, opts liverunner.Options) *liverunner.Runner {
	t.Helper()
	r, err := liverunner.Register(t.Context(), opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, r.Close(ctx))
	})
	return r
}

func nextHeartbeat(t *testing.T, events <-chan liverunner.HeartbeatEvent) liverunner.HeartbeatEvent {
	t.Helper()
	select {
	case e := <-events:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for heartbeat")
		return liverunner.HeartbeatEvent{}
	}
}

func awaitHeartbeat(t *testing.T, f *heartbeatFixture, match func(liverunner.HeartbeatEvent) bool) liverunner.HeartbeatEvent {
	t.Helper()
	deadline := time.After(4 * time.Second)
	for {
		select {
		case e := <-f.events:
			if match(e) {
				return e
			}
		case <-deadline:
			t.Fatal("expected heartbeat was not observed")
			return liverunner.HeartbeatEvent{}
		}
	}
}

func confirmedHeartbeat(t *testing.T, f *heartbeatFixture) liverunner.HeartbeatEvent {
	t.Helper()
	return awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
		return e.Err == nil && e.Response != nil && e.Response.Seq != nil
	})
}

func awaitSessionEvent(t *testing.T, events <-chan liverunner.SessionEvent, session string) liverunner.SessionEvent {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case e := <-events:
			if e.SessionID == session {
				return e
			}
		case <-deadline:
			t.Fatal("expected O2R session event was not observed")
			return liverunner.SessionEvent{}
		}
	}
}

func heartbeatGate(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	return ch, release
}

func waitHeartbeatGate(ctx context.Context, ch <-chan struct{}) {
	select {
	case <-ch:
	case <-ctx.Done():
	}
}

func sessionControlRequest(t *testing.T, orch *testOrchestrator, e liverunner.SessionEvent) *http.Request {
	t.Helper()
	req, err := http.NewRequest("POST", orch.httpBase, nil)
	require.NoError(t, err)
	req.Header.Set(liverunner.HeaderRunnerRoute, e.AppID)
	req.Header.Set(liverunner.HeaderSessionID, e.SessionID)
	req.Header.Set(liverunner.HeaderSessionToken, e.SessionToken)
	return req
}

func runHeartbeatRegistration(t *testing.T, orch *testOrchestrator) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, *testOrchestrator)
	}{
		{"empty_and_callback_updates", testHeartbeatEmpty},
		{"registration_confirmation", testHeartbeatConfirmation},
		{"restore_owner", testHeartbeatRestore},
		{"authentication", testHeartbeatAuthentication},
		{"concurrent_updates", testHeartbeatConcurrentUpdates},
		{"sequences", testHeartbeatSequences},
		{"events_and_failures", testHeartbeatEvents},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, orch) })
	}
}

func runHeartbeatSessionLifecycle(t *testing.T, orch *testOrchestrator) {
	t.Run("withdrawal", func(t *testing.T) { testHeartbeatWithdrawal(t, orch) })
	t.Run("o2r_ordering", func(t *testing.T) { testHeartbeatO2ROrdering(t, orch) })
	t.Run("retirement", func(t *testing.T) { testHeartbeatRetirement(t, orch) })
	t.Run("drain_deadlines", testHeartbeatDrainDeadlines)
}

func testHeartbeatEmpty(t *testing.T, orch *testOrchestrator) {
	for _, apps := range [][]liverunner.AppOptions{nil, {}} {
		t.Run(fmt.Sprintf("nil=%t", apps == nil), func(t *testing.T) {
			f := newHeartbeatFixture(t, orch)
			var rawApps atomic.Bool
			f.proxy.hooks(func(_ context.Context, x *heartbeatExchange) bool {
				if x.Path == "/runners/heartbeat" && x.Request.RunnerID == "" {
					rawApps.Store(string(x.Raw["apps"]) == "[]" && string(x.Raw["seq"]) == "0")
				}
				return false
			}, nil)
			opts := f.options(apps)
			var runner *liverunner.Runner
			var callbackErr atomic.Pointer[error]
			opts.OnHeartbeat = func(e liverunner.HeartbeatEvent) {
				if e.Err == nil && len(e.Request.Apps) == 0 && e.Response.Seq != nil {
					if err := runner.SetApps([]liverunner.AppOptions{f.appOptions(liverunner.ModeSingleShot)}); err != nil {
						callbackErr.Store(&err)
					}
				}
				f.events <- e
			}
			var err error
			runner, err = liverunner.New(opts)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runner.Close(context.Background())) })
			require.NoError(t, runner.Start(t.Context()))
			initial := nextHeartbeat(t, f.events)
			require.NoError(t, initial.Err)
			require.Empty(t, initial.Request.RunnerID)
			require.NotEmpty(t, runner.RunnerID())
			require.True(t, rawApps.Load(), "empty registration must send apps:[] and seq:0")
			e := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Request.Apps) == 1 })
			require.Nil(t, callbackErr.Load(), "SetApps from OnHeartbeat must not deadlock or fail")
			require.Len(t, e.Response.Apps, 1)
			id := runner.AppIDs()[0]
			require.NotEmpty(t, id)
			requireStatus(t, "POST", orch.httpBase+"/apps/"+id+"/app/probe", strings.NewReader("first app"), 200)
		})
	}
}

func testHeartbeatConfirmation(t *testing.T, orch *testOrchestrator) {
	for _, mode := range []string{liverunner.ModePersistent, liverunner.ModeSingleShot} {
		t.Run(mode, func(t *testing.T) {
			f := newHeartbeatFixture(t, orch)
			lost := make(chan liverunner.HeartbeatResponse, 1)
			var drop atomic.Bool
			f.proxy.hooks(nil, func(_ context.Context, x *heartbeatExchange) {
				if x.Path == "/runners/heartbeat" && drop.CompareAndSwap(false, true) {
					lost <- x.Response
					x.Drop = true
				}
			})
			opts := f.options([]liverunner.AppOptions{f.appOptions(mode)})
			abandoned, err := liverunner.New(opts)
			require.NoError(t, err)
			require.Error(t, abandoned.Start(t.Context()))
			require.NoError(t, abandoned.Close(t.Context()))
			old := <-lost
			require.NotEmpty(t, old.RunnerID)
			requireRunnerAbsent(t, orch, old.Apps[0].ID)
			failed := nextHeartbeat(t, f.events)
			require.Error(t, failed.Err)
			require.Nil(t, failed.Response)
			r := startHeartbeatRunner(t, opts)
			confirmedHeartbeat(t, f)
			require.NotEqual(t, old.RunnerID, r.RunnerID())
			id := r.AppIDs()[0]
			require.NotEqual(t, old.Apps[0].ID, id)
			requireDiscoveredRunner(t, orch, id)
			requireRunnerAbsent(t, orch, old.Apps[0].ID)
			if mode == liverunner.ModePersistent {
				requireStatus(t, "POST", orch.httpBase+"/apps/"+old.Apps[0].ID+"/session", nil, 404)
				s := reserveSession(t, orch, id)
				requireStatus(t, "POST", s.AppURL+"/probe", strings.NewReader("confirmed"), 200)
			} else {
				requireStatus(t, "POST", orch.httpBase+"/apps/"+old.Apps[0].ID+"/app/probe", nil, 404)
				requireStatus(t, "POST", orch.httpBase+"/apps/"+id+"/app/probe", strings.NewReader("confirmed"), 200)
			}
		})
	}
}

func testHeartbeatRestore(t *testing.T, orch *testOrchestrator) {
	f := newHeartbeatFixture(t, orch)
	apps := []liverunner.AppOptions{f.appOptions(liverunner.ModePersistent), f.appOptions(liverunner.ModePersistent)}
	opts := f.options(apps)
	opts.DisableUnregisterOnClose = true
	r := startHeartbeatRunner(t, opts)
	ready := confirmedHeartbeat(t, f)
	ids := r.AppIDs()
	a, b := reserveSession(t, orch, ids[0]), reserveSession(t, orch, ids[1])
	awaitSessionEvent(t, f.reserve, b.SessionID)
	opts.RunnerID, opts.HeartbeatSecret = r.RunnerID(), r.HeartbeatSecret()
	apps[0].ID, apps[1].ID = ids[0], ids[1]
	require.NoError(t, r.Close(t.Context()))
	for len(f.events) > 0 {
		<-f.events
	}
	opts.DisableUnregisterOnClose = false
	r = startHeartbeatRunner(t, opts)
	conflict := nextHeartbeat(t, f.events)
	require.Error(t, conflict.Err)
	require.Nil(t, conflict.Response)
	require.NotNil(t, conflict.AcceptedSeq)
	resumed := nextHeartbeat(t, f.events)
	require.NoError(t, resumed.Err)
	require.Equal(t, opts.RunnerID, r.RunnerID())
	require.True(t, opts.HeartbeatSecret == r.HeartbeatSecret(), "restored credential must survive omitted response credential")
	require.Empty(t, resumed.Response.HeartbeatSecret)
	require.Equal(t, ready.Response.O2R, resumed.Response.O2R)
	require.Equal(t, ids, r.AppIDs())
	require.Equal(t, []string{a.SessionID}, resumed.Response.Apps[0].SessionIDs)
	require.Equal(t, []string{b.SessionID}, resumed.Response.Apps[1].SessionIDs)
	for _, s := range []sessionResponse{a, b} {
		requireStatus(t, "POST", s.AppURL+"/restored", strings.NewReader("still alive"), 200)
	}
}

func testHeartbeatAuthentication(t *testing.T, _ *testOrchestrator) {
	orch := startOffchainOrchestrator(t, orchestratorOptions{extra: map[string]any{"HeartbeatInterval": "40ms", "HeartbeatTTL": "200ms"}})
	f := newHeartbeatFixture(t, orch)
	opts := f.options([]liverunner.AppOptions{f.appOptions(liverunner.ModeSingleShot)})
	opts.DisableUnregisterOnClose = true
	r := startHeartbeatRunner(t, opts)
	confirmedHeartbeat(t, f)
	opts.RunnerID, opts.HeartbeatSecret = r.RunnerID(), r.HeartbeatSecret()
	opts.Apps[0].ID = r.AppIDs()[0]
	o2r := r.O2RChannel().URL
	require.NoError(t, r.Close(t.Context()))
	// Let the real Node expire the stopped owner and invalidate its credential.
	eventually(t, func() bool {
		status, _, err := requestHTTP("GET", o2r+"/0", nil, nil)
		return err == nil && status == 404
	}, time.Second, 20*time.Millisecond)
	for len(f.events) > 0 {
		<-f.events
	}
	bad := opts
	bad.BootstrapSecret = bad.HeartbeatSecret
	var attempts atomic.Int32
	f.proxy.hooks(func(_ context.Context, x *heartbeatExchange) bool {
		if x.Path == "/runners/heartbeat" {
			attempts.Add(1)
		}
		return false
	}, nil)
	rejected, err := liverunner.New(bad)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rejected.Close(context.Background())) })
	require.Error(t, rejected.Start(t.Context()))
	require.EqualValues(t, 1, attempts.Load(), "issued credential cannot be retried as its own bootstrap")
	failure := nextHeartbeat(t, f.events)
	require.Nil(t, failure.Response)
	require.Error(t, failure.Err)
	attempts.Store(0)
	opts.DisableUnregisterOnClose = false
	r = startHeartbeatRunner(t, opts)
	unauthorized := nextHeartbeat(t, f.events)
	require.Error(t, unauthorized.Err)
	require.Nil(t, unauthorized.Response)
	fallback := nextHeartbeat(t, f.events)
	require.NoError(t, fallback.Err)
	require.True(t, fallback.Credential == liveRunnerSecret)
	require.True(t, opts.HeartbeatSecret != r.HeartbeatSecret(), "fallback must obtain a new credential")
	confirmedHeartbeat(t, f)
	requireStatus(t, "POST", orch.httpBase+"/apps/"+r.AppIDs()[0]+"/app/recovered", nil, 200)
}

func testHeartbeatConcurrentUpdates(t *testing.T, orch *testOrchestrator) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit_id=%t", explicit), func(t *testing.T) { testHeartbeatConcurrentUpdate(t, orch, explicit) })
	}
}

func testHeartbeatConcurrentUpdate(t *testing.T, orch *testOrchestrator, explicit bool) {
	f := newHeartbeatFixture(t, orch)
	a, b := f.appOptions(liverunner.ModeSingleShot), f.appOptions(liverunner.ModeSingleShot)
	a.Metadata, b.Metadata, b.Capacity = "A", "B", 3
	if explicit {
		b.ID = uuid.NewString()
	}
	release, unblock := heartbeatGate(t)
	entered := make(chan struct{}, 1)
	var held atomic.Bool
	f.proxy.hooks(nil, func(ctx context.Context, x *heartbeatExchange) {
		if x.Path == "/runners/heartbeat" && held.CompareAndSwap(false, true) {
			entered <- struct{}{}
			waitHeartbeatGate(ctx, release)
		}
	})
	r, err := liverunner.New(f.options([]liverunner.AppOptions{a}))
	require.NoError(t, err)
	t.Cleanup(func() { unblock(); require.NoError(t, r.Close(context.Background())) })
	started := make(chan error, 1)
	go func() { started <- r.Start(t.Context()) }()
	requireReceiveN(t, entered, 1)
	require.NoError(t, r.SetApps([]liverunner.AppOptions{b}))
	unblock()
	require.NoError(t, <-started)
	require.Equal(t, []string{b.ID}, r.AppIDs(), "A's generated ID must not be assigned to B")
	e := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
		return e.Err == nil && len(e.Request.Apps) == 1 && e.Request.Apps[0].Metadata == "B"
	})
	require.Equal(t, 3, e.Request.Apps[0].Capacity)
	require.Equal(t, "B", requireDiscoveredRunner(t, orch, e.Response.Apps[0].ID).Metadata)
	// Now delay a snapshot while an actual reservation arrives over O2R.
	c := f.appOptions(liverunner.ModePersistent)
	c.ID = uuid.NewString()
	require.NoError(t, r.SetApps([]liverunner.AppOptions{c}))
	awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
		return e.Err == nil && len(e.Response.Apps) == 1 && e.Response.Apps[0].ID == c.ID
	})
	release2, unblock2 := heartbeatGate(t)
	var held2 atomic.Bool
	f.proxy.hooks(nil, func(ctx context.Context, x *heartbeatExchange) {
		if x.Path == "/runners/heartbeat" && held2.CompareAndSwap(false, true) {
			entered <- struct{}{}
			waitHeartbeatGate(ctx, release2)
		}
	})
	requireReceiveN(t, entered, 1)
	s := reserveSession(t, orch, c.ID)
	awaitSessionEvent(t, f.reserve, s.SessionID)
	require.Contains(t, r.ActiveSessionIDs(c.ID), s.SessionID)
	unblock2()
	confirmedHeartbeat(t, f)
	require.Contains(t, r.ActiveSessionIDs(c.ID), s.SessionID, "heartbeat snapshot must not erase concurrent O2R reservation")
}

func testHeartbeatWithdrawal(t *testing.T, orch *testOrchestrator) {
	t.Run("persistent_sessions_proxies_channels_and_empty_restore", func(t *testing.T) {
		f := newHeartbeatFixture(t, orch)
		apps := []liverunner.AppOptions{f.appOptions(liverunner.ModePersistent), f.appOptions(liverunner.ModePersistent)}
		opts := f.options(apps)
		opts.DisableUnregisterOnClose = true
		r := startHeartbeatRunner(t, opts)
		stale := confirmedHeartbeat(t, f)
		ids := r.AppIDs()
		apps[0].ID, apps[1].ID = ids[0], ids[1]
		a, b := reserveSession(t, orch, ids[0]), reserveSession(t, orch, ids[1])
		evA := awaitSessionEvent(t, f.reserve, a.SessionID)
		awaitSessionEvent(t, f.reserve, b.SessionID)
		control := sessionControlRequest(t, orch, evA)
		channels, err := r.CreateTrickleChannels(t.Context(), control, []liverunner.ChannelRequest{{Name: "drain", MimeType: "text/plain"}})
		require.NoError(t, err)
		require.Len(t, channels, 1)
		proxy, err := r.CreateProxy(t.Context(), control, f.app.URL)
		require.NoError(t, err)
		require.NoError(t, r.SetApps(apps[1:]))
		withdrawn := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
			return e.Err == nil && len(e.Request.Apps) == 1 && len(e.Response.Apps) == 2 && e.Response.Apps[1].Status == "draining"
		})
		require.Equal(t, ids[0], withdrawn.Response.Apps[1].ID)
		requireRunnerAbsent(t, orch, ids[0])
		requireStatus(t, "POST", orch.httpBase+"/apps/"+ids[0]+"/session", nil, 404)
		for _, endpoint := range []string{a.AppURL, b.AppURL, proxy.URL} {
			requireStatus(t, "POST", endpoint+"/still-draining", strings.NewReader("alive"), 200)
		}
		requireStatus(t, "POST", channels[0].URL+"/0", strings.NewReader("draining channel"), 200)
		body, err := checkStatus("GET", channels[0].URL+"/0", nil, 200)
		require.NoError(t, err)
		require.Equal(t, "draining channel", body)
		// Replay an actually accepted pre-withdrawal snapshot, including its seq.
		status, _, err := requestJSON("POST", orch.httpBase+"/runners/heartbeat", stale.Request, map[string]string{"Authorization": r.HeartbeatSecret()})
		require.NoError(t, err)
		require.Equal(t, 409, status)
		requireRunnerAbsent(t, orch, ids[0])
		stopSession(t, orch, ids[0], a.SessionID)
		awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Response.Apps) == 1 })
		requireStatus(t, "POST", proxy.URL+"/after-drain", nil, 404)
		requireStatus(t, "GET", channels[0].URL+"/0", nil, 404)
		require.NoError(t, r.SetApps(nil))
		awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
			return e.Err == nil && len(e.Request.Apps) == 0 && len(e.Response.Apps) == 1 && e.Response.Apps[0].Status == "draining"
		})
		opts.RunnerID, opts.HeartbeatSecret, opts.Apps = r.RunnerID(), r.HeartbeatSecret(), []liverunner.AppOptions{}
		require.NoError(t, r.Close(t.Context()))
		for len(f.events) > 0 {
			<-f.events
		}
		opts.DisableUnregisterOnClose = false
		r = startHeartbeatRunner(t, opts)
		resumed := confirmedHeartbeat(t, f)
		require.Empty(t, resumed.Request.Apps)
		require.Len(t, resumed.Response.Apps, 1, "empty restart must retain draining sibling")
		requireStatus(t, "POST", b.AppURL+"/after-empty-restart", nil, 200)
		stopSession(t, orch, ids[1], b.SessionID)
		empty := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
			return e.Err == nil && len(e.Request.Apps) == 0 && e.Response.Apps != nil && len(e.Response.Apps) == 0
		})
		require.NotNil(t, empty.Response.Apps, "observe a complete empty membership before Close")
		require.NoError(t, r.Close(t.Context()))
	})
	t.Run("single_shot_request_drains", func(t *testing.T) {
		f := newHeartbeatFixture(t, orch)
		upstream := newRunnerUpstream(t, 1)
		a, b := f.appOptions(liverunner.ModeSingleShot), f.appOptions(liverunner.ModeSingleShot)
		a.RunnerURL = upstream.server.URL
		r := startHeartbeatRunner(t, f.options([]liverunner.AppOptions{a, b}))
		confirmedHeartbeat(t, f)
		ids := r.AppIDs()
		b.ID = ids[1]
		done := make(chan error, 1)
		go func() {
			_, err := checkStatus("POST", orch.httpBase+"/apps/"+ids[0]+"/app/hold", strings.NewReader("held"), 200)
			done <- err
		}()
		requireReceiveN(t, upstream.entered, 1)
		require.NoError(t, r.SetApps([]liverunner.AppOptions{b}))
		awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Request.Apps) == 1 })
		requireStatus(t, "POST", orch.httpBase+"/apps/"+ids[0]+"/app/new-work", nil, 404)
		requireStatus(t, "POST", orch.httpBase+"/apps/"+ids[1]+"/app/sibling", nil, 200)
		select {
		case err := <-done:
			t.Fatalf("in-flight request ended before natural completion: %v", err)
		default:
		}
		require.NoError(t, r.SetApps(nil))
		for range 2 {
			awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
				return e.Err == nil && len(e.Request.Apps) == 0 && len(e.Response.Apps) == 1 && e.Response.Apps[0].ID == ids[0]
			})
		}
		upstream.releaseRequests()
		require.NoError(t, <-done)
		awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
			return e.Err == nil && e.Response.Apps != nil && len(e.Response.Apps) == 0
		})
	})
}

func testHeartbeatSequences(t *testing.T, orch *testOrchestrator) {
	t.Run("successive_conflicts_and_retry_limit", func(t *testing.T) {
		f := newHeartbeatFixture(t, orch)
		opts := f.options([]liverunner.AppOptions{f.appOptions(liverunner.ModeSingleShot)})
		opts.HeartbeatInterval = 200 * time.Millisecond
		r := startHeartbeatRunner(t, opts)
		confirmedHeartbeat(t, f)
		var attempts atomic.Int32
		var missingSeq atomic.Bool
		f.proxy.hooks(func(_ context.Context, x *heartbeatExchange) bool {
			if x.Path != "/runners/heartbeat" {
				return false
			}
			if _, ok := x.Raw["seq"]; !ok {
				missingSeq.Store(true)
			}
			n := attempts.Add(1)
			if n <= 2 {
				x.Status, x.Body = 409, []byte(fmt.Sprintf(`{"error":"forced conflict","seq":%d}`, 100*n))
				return true
			}
			return false
		}, nil)
		first := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err != nil })
		second := nextHeartbeat(t, f.events)
		third := nextHeartbeat(t, f.events)
		require.Nil(t, first.Response)
		require.EqualValues(t, 100, *first.AcceptedSeq)
		require.Error(t, second.Err)
		require.Nil(t, second.Response)
		require.EqualValues(t, 101, second.Request.Seq)
		require.Less(t, second.At.Sub(first.At), 150*time.Millisecond, "first conflict permits an immediate retry")
		require.EqualValues(t, 200, *second.AcceptedSeq)
		require.NoError(t, third.Err)
		require.EqualValues(t, 201, third.Request.Seq)
		require.GreaterOrEqual(t, third.At.Sub(second.At), 150*time.Millisecond, "second conflict must wait for next background cycle")
		require.False(t, missingSeq.Load())
		require.NotEmpty(t, r.AppIDs()[0])
	})
	t.Run("conflict_without_seq", func(t *testing.T) {
		f := newHeartbeatFixture(t, orch)
		startHeartbeatRunner(t, f.options(nil))
		confirmedHeartbeat(t, f)
		var once atomic.Bool
		f.proxy.hooks(func(_ context.Context, x *heartbeatExchange) bool {
			if x.Path == "/runners/heartbeat" && once.CompareAndSwap(false, true) {
				x.Status, x.Body = 409, []byte(`{"error":"non-sequence conflict"}`)
				return true
			}
			return false
		}, nil)
		failed := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err != nil })
		require.Nil(t, failed.Response)
		require.Nil(t, failed.AcceptedSeq)
		recovered := nextHeartbeat(t, f.events)
		require.NoError(t, recovered.Err)
		require.Equal(t, failed.Request.Seq+1, recovered.Request.Seq)
		require.GreaterOrEqual(t, recovered.At.Sub(failed.At), 30*time.Millisecond)
	})
}

func copyHeartbeatEvent(e liverunner.HeartbeatEvent) liverunner.HeartbeatEvent {
	copy := e
	data, _ := json.Marshal(e.Request)
	copy.Request = liverunner.HeartbeatRequest{}
	_ = json.Unmarshal(data, &copy.Request)
	if e.Response != nil {
		data, _ = json.Marshal(e.Response)
		copy.Response = new(liverunner.HeartbeatResponse)
		_ = json.Unmarshal(data, copy.Response)
	}
	if e.AcceptedSeq != nil {
		seq := *e.AcceptedSeq
		copy.AcceptedSeq = &seq
	}
	return copy
}

func testHeartbeatEvents(t *testing.T, orch *testOrchestrator) {
	f := newHeartbeatFixture(t, orch)
	app := f.appOptions(liverunner.ModeSingleShot)
	app.Metadata, app.GPU = "original", &liverunner.GPU{Name: "original GPU"}
	opts := f.options([]liverunner.AppOptions{app})
	opts.OnHeartbeat = func(e liverunner.HeartbeatEvent) {
		saved := copyHeartbeatEvent(e)
		if len(e.Request.Apps) > 0 {
			e.Request.Apps[0].ID, e.Request.Apps[0].Metadata = "corrupt", "corrupt"
			e.Request.Apps[0].GPU.Name = "corrupt"
		}
		if e.Response != nil {
			e.Response.RunnerID, e.Response.HeartbeatSecret = "corrupt", "corrupt"
			e.Response.Apps[0].ID = "corrupt"
			if e.Response.O2R != nil {
				e.Response.O2R.URL = "http://invalid.invalid"
			}
			if e.Response.Seq != nil {
				*e.Response.Seq = 999999
			}
		}
		if e.AcceptedSeq != nil {
			*e.AcceptedSeq = 888888
		}
		f.events <- saved
	}
	r := startHeartbeatRunner(t, opts)
	initial := nextHeartbeat(t, f.events)
	require.NoError(t, initial.Err)
	ready := confirmedHeartbeat(t, f)
	require.Equal(t, 30*time.Second, ready.TTL)
	require.True(t, initial.Credential == liveRunnerSecret)
	require.True(t, ready.Credential == r.HeartbeatSecret())
	require.NotEqual(t, "corrupt", r.RunnerID())
	require.Equal(t, "original", ready.Request.Apps[0].Metadata)
	require.Equal(t, "original GPU", ready.Request.Apps[0].GPU.Name)
	require.Equal(t, ready.Response.Apps[0].ID, r.AppIDs()[0])
	require.Equal(t, ready.Request.Seq, *ready.AcceptedSeq)
	require.Contains(t, r.O2RChannel().URL, f.proxy.server.URL)
	var phase atomic.Int32
	f.proxy.hooks(func(_ context.Context, x *heartbeatExchange) bool {
		if x.Path == "/runners/heartbeat" && phase.CompareAndSwap(0, 1) {
			x.Status, x.Body = 503, []byte(`{"error":"temporary heartbeat outage"}`)
			return true
		}
		return false
	}, func(_ context.Context, x *heartbeatExchange) {
		if x.Path != "/runners/heartbeat" {
			return
		}
		if phase.CompareAndSwap(1, 2) {
			x.Response.Seq = nil // Older Nodes may omit the echo.
		} else if phase.CompareAndSwap(2, 3) {
			wrong := x.Request.Seq + 1
			x.Response.Seq = &wrong
		}
	})
	failed := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err != nil })
	require.Nil(t, failed.Response)
	require.Zero(t, failed.TTL)
	compatible := nextHeartbeat(t, f.events)
	require.NoError(t, compatible.Err)
	require.Nil(t, compatible.Response.Seq)
	require.Nil(t, compatible.AcceptedSeq)
	badEcho := nextHeartbeat(t, f.events)
	require.ErrorContains(t, badEcho.Err, "echo")
	require.Nil(t, badEcho.Response)
	recovered := nextHeartbeat(t, f.events)
	require.NoError(t, recovered.Err)
	events := []liverunner.HeartbeatEvent{failed, compatible, badEcho, recovered}
	for i := 1; i < len(events); i++ {
		require.Greater(t, events[i].Request.Seq, events[i-1].Request.Seq)
		require.True(t, events[i].At.After(events[i-1].At))
		require.True(t, events[i].Credential == r.HeartbeatSecret())
	}
	require.Equal(t, "original", recovered.Request.Apps[0].Metadata)
	require.Equal(t, "original GPU", recovered.Request.Apps[0].GPU.Name)
	requireStatus(t, "POST", orch.httpBase+"/apps/"+r.AppIDs()[0]+"/app/recovered", nil, 200)
}

func testHeartbeatO2ROrdering(t *testing.T, orch *testOrchestrator) {
	f := newHeartbeatFixture(t, orch)
	gate, unblock := heartbeatGate(t)
	entered := make(chan struct{}, 1)
	var delayed atomic.Bool
	f.proxy.mu.Lock()
	f.proxy.o2r = func(ctx context.Context, body []byte) {
		if bytes.Contains(body, []byte(`"released"`)) && delayed.CompareAndSwap(false, true) {
			entered <- struct{}{}
			waitHeartbeatGate(ctx, gate)
		}
	}
	f.proxy.mu.Unlock()
	r := startHeartbeatRunner(t, f.options([]liverunner.AppOptions{f.appOptions(liverunner.ModePersistent)}))
	confirmedHeartbeat(t, f)
	id := r.AppIDs()[0]
	s := reserveSession(t, orch, id)
	awaitSessionEvent(t, f.reserve, s.SessionID)
	require.NoError(t, r.SetApps(nil))
	awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Request.Apps) == 0 })
	stopSession(t, orch, id, s.SessionID)
	requireReceiveN(t, entered, 1)
	awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Response.Apps) == 0 })
	require.Contains(t, r.ActiveSessionIDs(id), s.SessionID, "release is still held by the HTTP proxy")
	unblock()
	released := awaitSessionEvent(t, f.release, s.SessionID)
	require.Equal(t, id, released.AppID)
	require.Empty(t, r.ActiveSessionIDs(id))
}

type heartbeatDoer func(*http.Request) (*http.Response, error)

func (d heartbeatDoer) Do(req *http.Request) (*http.Response, error) { return d(req) }

func testHeartbeatRetirement(t *testing.T, orch *testOrchestrator) {
	for _, kind := range []string{"failed", "lost", "concurrent"} {
		t.Run(kind, func(t *testing.T) {
			f := newHeartbeatFixture(t, orch)
			opts := f.options([]liverunner.AppOptions{f.appOptions(liverunner.ModePersistent)})
			var ptr atomic.Pointer[liverunner.Runner]
			opts.HTTPClient = heartbeatDoer(func(req *http.Request) (*http.Response, error) {
				if r := ptr.Load(); r != nil {
					_ = r.RunnerID()
					_ = r.HeartbeatSecret()
					_ = r.AppIDs()
					_ = r.OrchestratorURL()
				}
				return http.DefaultClient.Do(req)
			})
			r, err := liverunner.New(opts)
			require.NoError(t, err)
			ptr.Store(r)
			require.NoError(t, r.Start(t.Context()))
			t.Cleanup(func() { require.NoError(t, r.Close(context.Background())) })
			confirmedHeartbeat(t, f)
			id := r.AppIDs()[0]
			var attempts atomic.Int32
			f.proxy.hooks(func(_ context.Context, x *heartbeatExchange) bool {
				if !strings.HasSuffix(x.Path, "/unregister") {
					return false
				}
				n := attempts.Add(1)
				if kind == "failed" && n == 1 {
					x.Status, x.Body = 503, []byte(`{"error":"retirement unavailable"}`)
					return true
				}
				return false
			}, func(_ context.Context, x *heartbeatExchange) {
				if strings.HasSuffix(x.Path, "/unregister") && kind == "lost" && attempts.Load() == 1 {
					x.Drop = true
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if kind == "concurrent" {
				errs := make(chan error, 4)
				for range 4 {
					go func() { errs <- r.Close(ctx) }()
				}
				for range 4 {
					require.NoError(t, <-errs)
				}
			} else {
				require.Error(t, r.Close(ctx))
				require.NoError(t, r.Close(ctx), "Close must retry, accepting Node's 404 after a lost successful reply")
				require.EqualValues(t, 2, attempts.Load())
			}
			requireRunnerAbsent(t, orch, id)
			count := attempts.Load()
			require.NoError(t, r.Close(ctx))
			require.Equal(t, count, attempts.Load(), "successful retirement is idempotent")
		})
	}
}

func testHeartbeatDrainDeadlines(t *testing.T) {
	for _, timeout := range []time.Duration{0, 800 * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			orch := startOffchainOrchestrator(t, orchestratorOptions{extra: map[string]any{"DrainTimeout": timeout.String(), "HeartbeatInterval": "40ms"}})
			f := newHeartbeatFixture(t, orch)
			apps := []liverunner.AppOptions{f.appOptions(liverunner.ModePersistent), f.appOptions(liverunner.ModePersistent)}
			opts := f.options(apps)
			opts.DisableUnregisterOnClose = true
			r := startHeartbeatRunner(t, opts)
			confirmedHeartbeat(t, f)
			ids := r.AppIDs()
			apps[0].ID, apps[1].ID = ids[0], ids[1]
			a, b := reserveSession(t, orch, ids[0]), reserveSession(t, orch, ids[1])
			awaitSessionEvent(t, f.reserve, b.SessionID)
			opts.RunnerID, opts.HeartbeatSecret = r.RunnerID(), r.HeartbeatSecret()
			opts.Apps = apps[1:]
			started := time.Now()
			require.NoError(t, r.SetApps(apps[1:]))
			withdrawn := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Request.Apps) == 1 })
			if timeout > 0 {
				require.Len(t, withdrawn.Response.Apps, 2)
				// Restart midway through the first deadline while heartbeats continue.
				for time.Since(started) < timeout/2 {
					confirmedHeartbeat(t, f)
				}
				requireStatus(t, "POST", a.AppURL+"/before-deadline", nil, 200)
				require.NoError(t, r.Close(t.Context()))
				for len(f.events) > 0 {
					<-f.events
				}
				opts.DisableUnregisterOnClose = false
				r = startHeartbeatRunner(t, opts)
				confirmedHeartbeat(t, f)
			}
			expired := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
				return e.Err == nil && len(e.Request.Apps) == 1 && len(e.Response.Apps) == 1
			})
			require.Equal(t, ids[1], expired.Response.Apps[0].ID)
			if timeout > 0 {
				require.Less(t, time.Since(started), timeout+timeout/3, "heartbeats and SDK restart must not extend the original drain deadline")
			}
			requireStatus(t, "POST", a.AppURL+"/expired", nil, 404)
			requireStatus(t, "POST", b.AppURL+"/sibling-survives", nil, 200)
			require.NoError(t, r.Close(t.Context()))
			if timeout == 0 {
				unregisterRunner(t, orch, opts.RunnerID, opts.HeartbeatSecret, 200)
			}
			t.Run("single_shot_cancellation", func(t *testing.T) { testHeartbeatSingleShotDeadline(t, orch, timeout) })
		})
	}
}

func testHeartbeatSingleShotDeadline(t *testing.T, orch *testOrchestrator, timeout time.Duration) {
	f := newHeartbeatFixture(t, orch)
	entered, canceled := make(chan struct{}, 1), make(chan struct{}, 1)
	gate, release := heartbeatGate(t)
	backend := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		entered <- struct{}{}
		select {
		case <-req.Context().Done():
			canceled <- struct{}{}
		case <-gate:
		}
	}))
	t.Cleanup(release)
	a, b := f.appOptions(liverunner.ModeSingleShot), f.appOptions(liverunner.ModeSingleShot)
	a.RunnerURL = backend.URL
	opts := f.options([]liverunner.AppOptions{a, b})
	opts.DisableUnregisterOnClose = true
	r := startHeartbeatRunner(t, opts)
	confirmedHeartbeat(t, f)
	ids := r.AppIDs()
	b.ID = ids[1]
	opts.RunnerID, opts.HeartbeatSecret, opts.Apps = r.RunnerID(), r.HeartbeatSecret(), []liverunner.AppOptions{b}
	finished := make(chan int, 1)
	go func() {
		status, _, _ := requestHTTP("POST", orch.httpBase+"/apps/"+ids[0]+"/app/hold", nil, nil)
		finished <- status
	}()
	requireReceiveN(t, entered, 1)
	started := time.Now()
	require.NoError(t, r.SetApps(opts.Apps))
	withdrawn := awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool { return e.Err == nil && len(e.Request.Apps) == 1 })
	if timeout > 0 {
		require.Len(t, withdrawn.Response.Apps, 2)
		for time.Since(started) < timeout/2 {
			confirmedHeartbeat(t, f)
		}
		select {
		case <-canceled:
			t.Fatal("single-shot request canceled before drain deadline")
		default:
		}
		require.NoError(t, r.Close(t.Context()))
		for len(f.events) > 0 {
			<-f.events
		}
		opts.DisableUnregisterOnClose = false
		r = startHeartbeatRunner(t, opts)
	}
	awaitHeartbeat(t, f, func(e liverunner.HeartbeatEvent) bool {
		return e.Err == nil && len(e.Request.Apps) == 1 && len(e.Response.Apps) == 1
	})
	requireReceiveN(t, canceled, 1)
	select {
	case status := <-finished:
		require.NotEqual(t, 200, status, "expired request must not complete successfully")
	case <-time.After(time.Second):
		t.Fatal("expired single-shot request remained open")
	}
	if timeout > 0 {
		require.Less(t, time.Since(started), timeout+timeout/3, "SDK restart cannot extend single-shot drain")
	}
	requireStatus(t, "POST", orch.httpBase+"/apps/"+ids[0]+"/app/new", nil, 404)
	requireStatus(t, "POST", orch.httpBase+"/apps/"+ids[1]+"/app/sibling", nil, 200)
	require.NoError(t, r.Close(t.Context()))
	if timeout == 0 {
		unregisterRunner(t, orch, opts.RunnerID, opts.HeartbeatSecret, 200)
	}
}
