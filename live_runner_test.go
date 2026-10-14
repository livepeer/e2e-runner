package e2escope

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	liverunner "github.com/livepeer/golang-runner"
	"github.com/stretchr/testify/require"
)

const (
	liveRunnerApp = "livepeer-sample/echo"

	domainProxyURLTemplate = "http://{proxy}.localhost:{http_port}"
)

const (
	envGoLivepeerDir   = "GO_LIVEPEER_DIR"
	envLivepeerBin     = "LIVEPEER_BINARY"
	envPythonRunnerDir = "PYTHON_RUNNER_DIR"
)

var liveRunnerSecret string

var testHTTPClient = &http.Client{Timeout: 5 * time.Second}

//go:embed testdata/python_runner_register.py
var pythonRunnerRegisterScript string

//go:embed testdata/python_runner_session_controls.py
var pythonRunnerSessionControlsScript string

//go:embed testdata/python_runner_missing_price.py
var pythonRunnerMissingPriceScript string

//go:embed testdata/python_multi_track_media_round_trip.py
var pythonMultiTrackMediaRoundTripScript string

type testOrchestrator struct {
	backend          string
	process          *childProcess
	httpBase         string
	httpAddr         string
	cliBase          string
	proxyURLTemplate string
	logPath          string
	dir              string
	opts             orchestratorOptions
}

type orchestratorOptions struct {
	backend          string
	account          *chainAccount
	recipient        common.Address
	chain            *chainFixture
	proxyURLTemplate string
	staticConfigPath string
	dir              string
	extra            map[string]any
	noReady          bool
	useOracle        bool
	goExtra          []string
}

type heartbeatPayload struct {
	GPU       map[string]any `json:"gpu,omitempty"`
	RunnerID  string         `json:"runner_id,omitempty"`
	Label     string         `json:"label,omitempty"`
	Proxy     bool           `json:"proxy,omitempty"`
	RunnerURL string         `json:"runner_url"`
	Version   string         `json:"version,omitempty"`
	Metadata  string         `json:"metadata,omitempty"`
	Status    string         `json:"status,omitempty"`
	Mode      string         `json:"mode,omitempty"`
	App       string         `json:"app"`
	Capacity  int            `json:"capacity"`
	PriceInfo priceInfo      `json:"price_info"`
}

type priceInfo struct {
	Price    json.Number `json:"price,omitempty"`
	PriceUSD json.Number `json:"price_usd,omitempty"`
	Currency string      `json:"currency,omitempty"`
	Unit     string      `json:"unit,omitempty"`
}

type heartbeatResponse struct {
	RunnerID          string `json:"runner_id"`
	Orchestrator      string `json:"orchestrator"`
	HeartbeatInterval string `json:"heartbeat_interval"`
	HeartbeatTTL      string `json:"heartbeat_ttl"`
	HeartbeatSecret   string `json:"heartbeat_secret"`
}

type discoveryEntry struct {
	Address string            `json:"address"`
	Runners []discoveryRunner `json:"runners"`
}

type discoveryRunner struct {
	URL               string     `json:"url"`
	App               string     `json:"app"`
	Version           string     `json:"version"`
	Metadata          string     `json:"metadata"`
	Mode              string     `json:"mode"`
	Capacity          int        `json:"capacity"`
	CapacityUsed      int        `json:"capacity_used"`
	CapacityAvailable int        `json:"capacity_available"`
	PriceInfo         *priceInfo `json:"price_info"`
}

type sessionResponse struct {
	SessionID string `json:"session_id"`
	AppURL    string `json:"app_url"`
}

type channelResponse struct {
	Channels []struct {
		Name        string `json:"name"`
		ChannelName string `json:"channel_name"`
		URL         string `json:"url"`
		MimeType    string `json:"mime_type"`
	} `json:"channels"`
}

type runnerProxyResponse struct {
	RunnerRoute string   `json:"runner_route"`
	SessionID   string   `json:"session_id"`
	ControlURL  string   `json:"control_url"`
	ChannelName string   `json:"channel_name"`
	Deleted     []string `json:"deleted"`
}

type proxyCreateResponse struct {
	ProxyID  string `json:"proxy_id"`
	ProxyURL string `json:"proxy_url"`
}

type multiTrackMediaRoundTripResult struct {
	ObservedVideoTracks  int                `json:"observed_video_tracks"`
	ObservedAudioTracks  int                `json:"observed_audio_tracks"`
	VideoFrameCounts     []int              `json:"video_frame_counts"`
	AudioSampleCounts    []int              `json:"audio_sample_counts"`
	VideoSignaturesOK    bool               `json:"video_signatures_ok"`
	AudioFrequenciesOK   bool               `json:"audio_frequencies_ok"`
	AudioFrequencyRatios map[string]float64 `json:"audio_frequency_ratios"`
	SegmentsConsumed     int                `json:"segments_consumed"`
	SegmentsStarted      int                `json:"segments_started"`
	VerificationOK       bool               `json:"verification_ok"`
	Errors               []string           `json:"errors"`
}

type proxiedTargetResponse struct {
	Method      string `json:"method"`
	Path        string `json:"path"`
	Query       string `json:"query"`
	Body        string `json:"body"`
	RunnerRoute string `json:"runner_route"`
	SessionID   string `json:"session_id"`
	ControlURL  string `json:"control_url"`
}

type runnerConfig struct {
	price     priceInfo
	runnerURL string
	mode      string
	label     string
	metadata  string
	capacity  int
	proxy     bool
}

type staticRunnerConfig struct {
	Runners []staticRunnerConfigEntry `json:"runners"`
}

type staticRunnerConfigEntry struct {
	Label     string    `json:"label"`
	Routing   string    `json:"routing"`
	Proxy     bool      `json:"proxy,omitempty"`
	RunnerURL string    `json:"runner_url"`
	App       string    `json:"app"`
	Metadata  string    `json:"metadata,omitempty"`
	Mode      string    `json:"mode"`
	Capacity  int       `json:"capacity"`
	PriceInfo priceInfo `json:"price_info"`
	HealthURL string    `json:"health_url"`
}

type runnerUpstream struct {
	server      *httptest.Server
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

type staticRunnerScenario struct {
	name     string
	route    string
	cfg      runnerConfig
	upstream *runnerUpstream
}

type runnerRegistration interface {
	RunnerID() string
	OrchestratorURL() string
	Close(t *testing.T)
}

type runnerImplementation interface {
	Name() string
	Start(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) runnerRegistration
	StartSessionControlRunner(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) runnerRegistration
}

type pythonRunnerImplementation struct{}

type goRunnerImplementation struct{}

type pythonRunner struct {
	runnerID        string
	orchestratorURL string
	stdin           io.Closer
	cancel          context.CancelFunc
	done            <-chan error
	stderr          *lockedBuffer
}

type goRunnerRegistration struct {
	ctx context.Context
	reg runnerSDK
}

func runnerImplementations() []runnerImplementation {
	return []runnerImplementation{
		pythonRunnerImplementation{},
		goRunnerImplementation{},
	}
}

func (pythonRunnerImplementation) Name() string {
	return "python-runner"
}

func (pythonRunnerImplementation) Start(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) runnerRegistration {
	return startPythonRunner(t, ctx, pythonRunnerRegisterScript, orch, cfg)
}

func (pythonRunnerImplementation) StartSessionControlRunner(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) runnerRegistration {
	return startPythonRunner(t, ctx, pythonRunnerSessionControlsScript, orch, cfg)
}

func (goRunnerImplementation) Name() string {
	return "golang-runner"
}

func (goRunnerImplementation) Start(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) runnerRegistration {
	return startGoRunner(t, ctx, orch, cfg)
}

func (goRunnerImplementation) StartSessionControlRunner(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) runnerRegistration {
	var reg runnerSDK
	var upstream *httptest.Server
	upstream = newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reg == nil {
			http.Error(w, "runner not registered", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/channels":
			channels, err := reg.CreateTrickleChannels(ctx, r, []liverunner.ChannelRequest{{Name: "events", MimeType: "application/json"}})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			deleted, err := reg.DeleteTrickleChannels(ctx, r, []string{"events"})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if len(channels) != 1 {
				http.Error(w, fmt.Sprintf("expected 1 channel, got %d", len(channels)), http.StatusInternalServerError)
				return
			}
			respondJSON(t, w, runnerProxyResponse{
				RunnerRoute: r.Header.Get(liverunner.HeaderRunnerRoute),
				SessionID:   r.Header.Get(liverunner.HeaderSessionID),
				ControlURL:  r.Header.Get(liverunner.HeaderSessionControl),
				ChannelName: channels[0].ChannelName,
				Deleted:     deleted,
			})
		case "/proxy":
			proxy, err := reg.CreateProxy(ctx, r, upstream.URL+"/proxy-target")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			respondJSON(t, w, proxyCreateResponse{
				ProxyID:  proxy.ProxyID,
				ProxyURL: proxy.URL,
			})
		case "/proxy-default":
			proxy, err := reg.CreateProxy(ctx, r, "")
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			respondJSON(t, w, proxyCreateResponse{
				ProxyID:  proxy.ProxyID,
				ProxyURL: proxy.URL,
			})
		case "/stop":
			if err := liverunner.StopSession(ctx, nil, r, time.Second); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			if strings.HasPrefix(r.URL.Path, "/proxy-target") || strings.HasPrefix(r.URL.Path, "/app-proxy") {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				respondJSON(t, w, proxiedTargetResponse{
					Method:      r.Method,
					Path:        r.URL.Path,
					Query:       r.URL.RawQuery,
					Body:        string(body),
					RunnerRoute: r.Header.Get(liverunner.HeaderRunnerRoute),
					SessionID:   r.Header.Get(liverunner.HeaderSessionID),
					ControlURL:  r.Header.Get(liverunner.HeaderSessionControl),
				})
				return
			}
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(upstream.Close)

	cfg.runnerURL = upstream.URL
	registered := startGoRunner(t, ctx, orch, cfg)
	reg = registered.reg
	return registered
}

func newRunnerUpstream(t *testing.T, capacity int) *runnerUpstream {
	t.Helper()
	upstream := &runnerUpstream{
		entered: make(chan struct{}, capacity+1),
		release: make(chan struct{}),
	}
	upstream.server = newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/hold" {
			upstream.entered <- struct{}{}
			<-upstream.release
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(func() {
		upstream.releaseRequests()
		upstream.server.Close()
	})
	return upstream
}

func (u *runnerUpstream) releaseRequests() {
	u.releaseOnce.Do(func() {
		close(u.release)
	})
}

func runDynamicRunnerScenario(t *testing.T, ctx context.Context, orch *testOrchestrator, impl runnerImplementation, cfg runnerConfig) {
	t.Helper()
	upstream := newRunnerUpstream(t, cfg.capacity)
	cfg.runnerURL = upstream.server.URL
	reg := impl.Start(t, ctx, orch, cfg)
	runnerID := reg.RunnerID()
	require.Equal(t, orch.httpBase, reg.OrchestratorURL())

	runRegisteredRunnerScenario(t, orch, runnerID, cfg, upstream)

	reg.Close(t)
	requireRunnerMetadataAbsent(t, orch, cfg.metadata)
	if cfg.mode == liverunner.ModePersistent {
		requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/session", nil, http.StatusNotFound)
		return
	}
	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/app/after-close", strings.NewReader("x"), http.StatusNotFound)
}

func runRegisteredRunnerScenario(t *testing.T, orch *testOrchestrator, route string, cfg runnerConfig, upstream *runnerUpstream) {
	t.Helper()
	runner := requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata)
	require.Equal(t, liveRunnerApp, runner.App)
	require.Equal(t, cfg.metadata, runner.Metadata)
	require.Equal(t, cfg.mode, runner.Mode)
	requireRunnerCapacity(t, runner, cfg.capacity, 0, cfg.capacity)

	if cfg.mode == liverunner.ModePersistent {
		runPersistentRunnerLifecycle(t, orch, route, cfg)
		return
	}
	runSingleShotRunnerLifecycle(t, orch, route, cfg, upstream)
}

func runPersistentRunnerLifecycle(t *testing.T, orch *testOrchestrator, route string, cfg runnerConfig) {
	t.Helper()
	runner := requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata)
	require.Equal(t, orch.httpBase+"/apps/"+url.PathEscape(route)+"/session", runner.URL)

	s1 := reserveSession(t, orch, route)
	requireRunnerCapacity(t, requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata), cfg.capacity, 1, cfg.capacity-1)
	s2 := reserveSession(t, orch, route)
	require.NotEqual(t, s1.SessionID, s2.SessionID)
	requirePersistentAppURL(t, orch, route, s1, cfg.proxy)
	requirePersistentAppURL(t, orch, route, s2, cfg.proxy)
	requireStatus(t, http.MethodPost, s1.AppURL+"/check", strings.NewReader("x"), http.StatusOK)

	t.Run("full capacity discovery", func(t *testing.T) {
		requireRunnerCapacity(t, requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata), cfg.capacity, cfg.capacity, 0)
	})
	t.Run("busy rejection", func(t *testing.T) {
		requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(route)+"/session", nil, http.StatusServiceUnavailable)
	})

	t.Run("stop response", func(t *testing.T) { stopSession(t, orch, route, s1.SessionID) })
	requireRunnerCapacity(t, requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata), cfg.capacity, cfg.capacity-1, 1)
	s3 := reserveSession(t, orch, route)
	require.NotEmpty(t, s3.SessionID)
	requirePersistentAppURL(t, orch, route, s3, cfg.proxy)
}

func requirePersistentAppURL(t *testing.T, orch *testOrchestrator, route string, session sessionResponse, proxy bool) {
	t.Helper()
	if proxy {
		requireDomainProxyURL(t, orch, session.AppURL, "")
		return
	}
	require.Equal(t, orch.httpBase+"/apps/"+url.PathEscape(route)+"/session/"+url.PathEscape(session.SessionID)+"/app", session.AppURL)
}

func runSingleShotRunnerLifecycle(t *testing.T, orch *testOrchestrator, route string, cfg runnerConfig, upstream *runnerUpstream) {
	t.Helper()
	runner := requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata)
	if cfg.proxy {
		requireDomainProxyURL(t, orch, runner.URL, route)
	} else {
		require.Equal(t, orch.httpBase+"/apps/"+url.PathEscape(route)+"/app", runner.URL)
	}

	var wg sync.WaitGroup
	errs := make(chan error, cfg.capacity)
	for i := 0; i < cfg.capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := checkStatus(http.MethodPost, runner.URL+"/hold", strings.NewReader("x"), http.StatusOK)
			errs <- err
		}()
	}
	requireReceiveN(t, upstream.entered, cfg.capacity)
	t.Run("full capacity discovery", func(t *testing.T) {
		requireRunnerCapacity(t, requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata), cfg.capacity, cfg.capacity, 0)
	})
	t.Run("busy rejection", func(t *testing.T) {
		requireStatus(t, http.MethodPost, runner.URL+"/blocked", strings.NewReader("x"), http.StatusServiceUnavailable)
	})
	upstream.releaseRequests()
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	requireRunnerCapacity(t, requireDiscoveredRunnerByMetadata(t, orch, cfg.metadata), cfg.capacity, 0, cfg.capacity)
	requireStatus(t, http.MethodPost, runner.URL+"/after-release", strings.NewReader("x"), http.StatusOK)
}

func requireDomainProxyURL(t *testing.T, orch *testOrchestrator, rawURL, route string) {
	t.Helper()
	require.NotEmpty(t, orch.proxyURLTemplate)
	if route != "" {
		require.Equal(t, strings.Replace(orch.proxyURLTemplate, "{proxy}", route, 1), rawURL)
		return
	}
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	serviceURL, err := url.Parse(orch.httpBase)
	require.NoError(t, err)
	if composeMode() {
		require.Equal(t, "https", u.Scheme)
		require.Equal(t, serviceURL.Host, u.Host)
		require.True(t, strings.HasPrefix(u.Path, "/e2e-proxy/"), rawURL)
		require.NotEmpty(t, strings.TrimPrefix(u.Path, "/e2e-proxy/"))
		return
	}
	require.Equal(t, "http", u.Scheme)
	require.Equal(t, serviceURL.Port(), u.Port())
	require.True(t, strings.HasSuffix(u.Hostname(), ".localhost"), rawURL)
	require.NotEqual(t, ".localhost", u.Hostname())
	require.Empty(t, u.EscapedPath())
}

func TestDynamicRunnerProxyRegistration(t *testing.T) {
	forEachOrchestrator(t, runDynamicRunnerProxyRegistration)
}

func runDynamicRunnerProxyRegistration(t *testing.T) {
	orch := startOffchainOrchestrator(t, orchestratorOptions{proxyURLTemplate: domainProxyURLTemplate})
	ctx := context.Background()

	for _, impl := range runnerImplementations() {
		impl := impl
		t.Run(impl.Name(), func(t *testing.T) {
			for _, mode := range []string{liverunner.ModePersistent, liverunner.ModeSingleShot} {
				for _, proxy := range []bool{false, true} {
					state := "proxy-unset"
					if proxy {
						state = "proxy-set"
					}
					t.Run(mode+"/"+state, func(t *testing.T) {
						cfg := runnerConfig{
							mode:     mode,
							label:    impl.Name() + "-" + mode + "-" + state,
							metadata: fmt.Sprintf(`{"registration":"dynamic","implementation":%q,"mode":%q,"proxy":%t}`, impl.Name(), mode, proxy),
							capacity: 2,
							proxy:    proxy,
						}
						runDynamicRunnerScenario(t, ctx, orch, impl, cfg)
					})
				}
			}
		})
	}
}

func TestStaticRunnerProxyConfiguration(t *testing.T) {
	forEachOrchestrator(t, runStaticRunnerProxyConfiguration)
}

func runStaticRunnerProxyConfiguration(t *testing.T) {
	t.Run("startup config", func(t *testing.T) {
		cfg, scenarios := newStaticRunnerMatrix(t, "startup-static")
		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		configPath := filepath.Join(t.TempDir(), "live-runners.json")
		require.NoError(t, os.WriteFile(configPath, data, 0o600))
		orch := startOffchainOrchestrator(t, orchestratorOptions{
			proxyURLTemplate: domainProxyURLTemplate,
			staticConfigPath: configPath,
		})
		runStaticRunnerMatrix(t, orch, scenarios)
	})

	t.Run("CLI registration", func(t *testing.T) {
		orch := startOffchainOrchestrator(t, orchestratorOptions{proxyURLTemplate: domainProxyURLTemplate})
		cfg, scenarios := newStaticRunnerMatrix(t, "cli-static")
		registerStaticRunnerConfig(t, orch, cfg, http.StatusOK)
		runStaticRunnerMatrix(t, orch, scenarios)
	})
}

func newStaticRunnerMatrix(t *testing.T, prefix string) (staticRunnerConfig, []staticRunnerScenario) {
	t.Helper()
	var cfg staticRunnerConfig
	var scenarios []staticRunnerScenario
	for _, mode := range []string{liverunner.ModePersistent, liverunner.ModeSingleShot} {
		for _, proxy := range []bool{false, true} {
			state := "proxy-unset"
			if proxy {
				state = "proxy-set"
			}
			name := mode + "/" + state
			route := prefix + "-" + mode + "-" + state
			metadata := fmt.Sprintf(`{"registration":%q,"mode":%q,"proxy":%t}`, prefix, mode, proxy)
			upstream := newRunnerUpstream(t, 2)
			runnerCfg := runnerConfig{
				runnerURL: upstream.server.URL,
				mode:      mode,
				label:     route,
				metadata:  metadata,
				capacity:  2,
				proxy:     proxy,
			}
			cfg.Runners = append(cfg.Runners, staticRunnerConfigEntry{
				Label:     route,
				Routing:   "label",
				Proxy:     proxy,
				RunnerURL: upstream.server.URL,
				App:       liveRunnerApp,
				Metadata:  metadata,
				Mode:      mode,
				Capacity:  2,
				PriceInfo: priceInfo{Price: "10", Currency: "usd", Unit: "fixed"},
				HealthURL: upstream.server.URL + "/health",
			})
			scenarios = append(scenarios, staticRunnerScenario{
				name:     name,
				route:    route,
				cfg:      runnerCfg,
				upstream: upstream,
			})
		}
	}
	return cfg, scenarios
}

func runStaticRunnerMatrix(t *testing.T, orch *testOrchestrator, scenarios []staticRunnerScenario) {
	t.Helper()
	for _, scenario := range scenarios {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			runRegisteredRunnerScenario(t, orch, scenario.route, scenario.cfg, scenario.upstream)
		})
	}
}

func TestLiveRunnerServerSurface(t *testing.T) { forEachOrchestrator(t, runLiveRunnerServerSurface) }

func runLiveRunnerServerSurface(t *testing.T) {
	orch := startOffchainOrchestrator(t)

	t.Run("heartbeat auth lifecycle", func(t *testing.T) {
		payload := heartbeatPayload{
			RunnerID:  "auth-runner",
			RunnerURL: "http://runner.invalid",
			Metadata:  "initial metadata",
			Status:    "ready",
			Mode:      "persistent",
			App:       liveRunnerApp,
			Capacity:  1,
			PriceInfo: priceInfo{Price: "10", Currency: "usd", Unit: "fixed"},
		}
		resp := registerRunnerHTTP(t, orch, payload, liveRunnerSecret)
		payload.Capacity = 2
		payload.Metadata = "updated metadata"
		requireHeartbeatStatus(t, orch, payload, "wrong-secret", http.StatusUnauthorized)
		updated := registerRunnerHTTP(t, orch, payload, resp.HeartbeatSecret)
		require.Empty(t, updated.HeartbeatSecret)
		require.Equal(t, "updated metadata", requireDiscoveredRunner(t, orch, resp.RunnerID).Metadata)
		unregisterRunner(t, orch, resp.RunnerID, "wrong-secret", http.StatusUnauthorized)
		unregisterRunner(t, orch, resp.RunnerID, resp.HeartbeatSecret, http.StatusNoContent)
	})

	t.Run("proxy headers and session token isolation", func(t *testing.T) {
		headersCh := make(chan http.Header, 2)
		upstream := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headersCh <- r.Header.Clone()
			_, _ = w.Write([]byte("ok"))
		}))
		defer upstream.Close()

		resp := registerRunnerHTTP(t, orch, heartbeatPayload{
			RunnerID:  "token-runner",
			RunnerURL: upstream.URL,
			Status:    "ready",
			Mode:      "persistent",
			App:       liveRunnerApp,
			Capacity:  2,
			PriceInfo: priceInfo{Price: "10", Currency: "usd", Unit: "fixed"},
		}, liveRunnerSecret)
		s1 := reserveSession(t, orch, resp.RunnerID)
		s2 := reserveSession(t, orch, resp.RunnerID)

		requireStatus(t, http.MethodPost, s1.AppURL+"/echo", strings.NewReader("x"), http.StatusOK)
		h1 := requireReceiveHeader(t, headersCh)
		require.Equal(t, resp.RunnerID, h1.Get("Livepeer-Runner-Route"))
		require.Equal(t, s1.SessionID, h1.Get("Livepeer-Session-Id"))
		require.NotEmpty(t, h1.Get("Livepeer-Session-Token"))
		require.Contains(t, h1.Get("Livepeer-Session-Control"), "/runner/"+resp.RunnerID+"/session/"+s1.SessionID)

		requireStatus(t, http.MethodPost, s2.AppURL+"/echo", strings.NewReader("x"), http.StatusOK)
		h2 := requireReceiveHeader(t, headersCh)
		token1 := h1.Get("Livepeer-Session-Token")
		token2 := h2.Get("Livepeer-Session-Token")
		require.NotEqual(t, token1, token2)

		channelURL := orch.httpBase + "/runner/" + url.PathEscape(resp.RunnerID) + "/session/" + url.PathEscape(s2.SessionID) + "/channels"
		requireChannelStatus(t, channelURL, token1, http.StatusUnauthorized)
		requireChannelStatus(t, channelURL, token2, http.StatusOK)
	})

	t.Run("static health and label routing", func(t *testing.T) {
		var healthy atomic.Bool
		healthy.Store(true)
		healthSrv := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if healthy.Load() {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer healthSrv.Close()

		cfg := staticRunnerConfig{Runners: []staticRunnerConfigEntry{{Label: "static-echo", Routing: "label", RunnerURL: "http://runner.invalid", App: liveRunnerApp, Metadata: "static application metadata", Mode: "persistent", Capacity: 1, PriceInfo: priceInfo{Price: "10", Currency: "usd", Unit: "fixed"}, HealthURL: healthSrv.URL}}}
		path := writeStaticConfig(t, cfg)
		orch := startOffchainOrchestrator(t, orchestratorOptions{staticConfigPath: path})
		waitDiscovered(t, orch, "static application metadata")
		runner := requireDiscoveredRunner(t, orch, "static-echo")
		require.Contains(t, runner.URL, "/apps/static-echo/session")
		require.Equal(t, "static application metadata", runner.Metadata)
		requireRunnerCapacity(t, runner, 1, 0, 1)
		session := reserveSession(t, orch, "static-echo")
		require.NotEmpty(t, session.SessionID)

		healthy.Store(false)

		requireRunnerAbsent(t, orch, "static-echo")
		requireStatus(t, http.MethodPost, orch.httpBase+"/apps/static-echo/session", nil, http.StatusNotFound)
	})
}

func TestRunnerSessionControlAPIs(t *testing.T) { forEachOrchestrator(t, runRunnerSessionControlAPIs) }

func runRunnerSessionControlAPIs(t *testing.T) {
	orch := startOffchainOrchestrator(t)
	ctx := context.Background()

	for _, impl := range runnerImplementations() {
		impl := impl
		t.Run(impl.Name(), func(t *testing.T) {
			cfg := runnerConfig{
				mode:     liverunner.ModePersistent,
				label:    impl.Name() + "-session-controls",
				metadata: "session controls for " + impl.Name(),
				capacity: 1,
			}
			reg := impl.StartSessionControlRunner(t, ctx, orch, cfg)
			runnerID := reg.RunnerID()
			require.Equal(t, orch.httpBase, reg.OrchestratorURL())

			session := reserveSession(t, orch, runnerID)
			t.Run("full capacity discovery", func(t *testing.T) {
				discovered := requireDiscoveredRunner(t, orch, runnerID)
				require.Equal(t, cfg.metadata, discovered.Metadata)
				requireRunnerCapacity(t, discovered, 1, 1, 0)
			})

			var proxyResp runnerProxyResponse
			requireJSONStatus(t, http.MethodPost, session.AppURL+"/channels", nil, nil, http.StatusOK, &proxyResp)
			require.Equal(t, runnerID, proxyResp.RunnerRoute)
			require.Equal(t, session.SessionID, proxyResp.SessionID)
			require.Contains(t, proxyResp.ControlURL, "/runner/"+runnerID+"/session/"+session.SessionID)
			require.NotEmpty(t, proxyResp.ChannelName)
			require.Equal(t, []string{"events"}, proxyResp.Deleted)

			var createdProxy proxyCreateResponse
			requireJSONStatus(t, http.MethodPost, session.AppURL+"/proxy", nil, nil, http.StatusOK, &createdProxy)
			require.NotEmpty(t, createdProxy.ProxyID)
			require.NotEmpty(t, createdProxy.ProxyURL)
			require.Contains(t, createdProxy.ProxyURL, orch.httpBase+"/run/")

			proxiedBody, err := checkStatus(http.MethodPost, createdProxy.ProxyURL+"/foo/bar?x=1", strings.NewReader("proxied-body"), http.StatusOK)
			require.NoError(t, err)
			var proxied proxiedTargetResponse
			require.NoError(t, json.Unmarshal([]byte(proxiedBody), &proxied), proxiedBody)
			require.Equal(t, http.MethodPost, proxied.Method)
			require.Equal(t, "/proxy-target/foo/bar", proxied.Path)
			require.Equal(t, "x=1", proxied.Query)
			require.Equal(t, "proxied-body", proxied.Body)
			require.Equal(t, runnerID, proxied.RunnerRoute)
			require.Equal(t, session.SessionID, proxied.SessionID)
			require.Contains(t, proxied.ControlURL, "/runner/"+runnerID+"/session/"+session.SessionID)

			var createdDefaultProxy proxyCreateResponse
			requireJSONStatus(t, http.MethodPost, session.AppURL+"/proxy-default", nil, nil, http.StatusOK, &createdDefaultProxy)
			require.NotEmpty(t, createdDefaultProxy.ProxyID)
			require.NotEmpty(t, createdDefaultProxy.ProxyURL)
			require.Contains(t, createdDefaultProxy.ProxyURL, orch.httpBase+"/run/")

			defaultProxiedBody, err := checkStatus(http.MethodPost, createdDefaultProxy.ProxyURL+"/app-proxy/foo?y=2", strings.NewReader("default-body"), http.StatusOK)
			require.NoError(t, err)
			var defaultProxied proxiedTargetResponse
			require.NoError(t, json.Unmarshal([]byte(defaultProxiedBody), &defaultProxied), defaultProxiedBody)
			require.Equal(t, http.MethodPost, defaultProxied.Method)
			require.Equal(t, "/app-proxy/foo", defaultProxied.Path)
			require.Equal(t, "y=2", defaultProxied.Query)
			require.Equal(t, "default-body", defaultProxied.Body)
			require.Equal(t, runnerID, defaultProxied.RunnerRoute)
			require.Equal(t, session.SessionID, defaultProxied.SessionID)
			require.Contains(t, defaultProxied.ControlURL, "/runner/"+runnerID+"/session/"+session.SessionID)

			requireStatus(t, http.MethodPost, session.AppURL+"/stop", nil, http.StatusNoContent)
			requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), 1, 0, 1)
			requireStatus(t, http.MethodPost, createdProxy.ProxyURL+"/after-stop", strings.NewReader("x"), http.StatusNotFound)
			requireStatus(t, http.MethodPost, createdDefaultProxy.ProxyURL+"/after-stop", strings.NewReader("x"), http.StatusNotFound)
			_ = reserveSession(t, orch, runnerID)

			reg.Close(t)
			requireRunnerAbsent(t, orch, runnerID)
		})
	}
}

func TestPythonRunnerMultiTrackMediaRoundTrip(t *testing.T) {
	forEachOrchestrator(t, runPythonRunnerMultiTrackMediaRoundTrip)
}

func runPythonRunnerMultiTrackMediaRoundTrip(t *testing.T) {
	runMediaRoundTrip(t, startOffchainOrchestrator(t), nil)
}

func runMediaRoundTrip(t *testing.T, orch *testOrchestrator, signer *testSigner) {
	pythonRunnerDir := repoDir(t, envPythonRunnerDir)
	requestHeaders := make(chan http.Header, 1)
	upstream := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestHeaders <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	runnerID := fmt.Sprintf("multi-track-round-trip-%d", time.Now().UnixNano())
	price := priceInfo{Price: "10", Currency: "usd", Unit: "fixed"}
	if signer != nil {
		price.Price = "0.001"
	}
	registration := registerRunnerHTTP(t, orch, heartbeatPayload{
		RunnerID:  runnerID,
		RunnerURL: upstream.URL,
		Status:    "ready",
		Mode:      "persistent",
		App:       liveRunnerApp,
		Capacity:  1,
		PriceInfo: price,
	}, liveRunnerSecret)
	var session sessionResponse
	if signer == nil {
		session = reserveSession(t, orch, registration.RunnerID)
	} else {
		j := &paymentJob{signer: signer, orch: orch, route: registration.RunnerID, mode: "persistent", unit: "fixed", app: liveRunnerApp}
		j.obtainChallenge(t)
		j.sign(t, nil)
		status, body := httpJSON(t, "POST", j.target(), nil, j.headers())
		require.Equal(t, 200, status, string(body))
		require.NoError(t, json.Unmarshal(body, &session))
		t.Cleanup(func() {
			stopPaidSession(t, orch, registration.RunnerID, session.SessionID)
			unregisterPaidRunner(t, orch, registration.RunnerID, registration.HeartbeatSecret)
		})
	}
	requireStatus(t, http.MethodPost, session.AppURL+"/bootstrap", nil, http.StatusNoContent)
	sessionHeaders := requireReceiveHeader(t, requestHeaders)

	channelName := fmt.Sprintf("multi-track-%d", time.Now().UnixNano())
	channelEndpoint := orch.httpBase +
		"/runner/" + url.PathEscape(registration.RunnerID) +
		"/session/" + url.PathEscape(session.SessionID) +
		"/channels"
	var channels channelResponse
	requireJSONStatus(
		t,
		http.MethodPost,
		channelEndpoint,
		map[string]any{
			"channels": []map[string]string{{
				"name":      channelName,
				"mime_type": "video/MP2T",
			}},
		},
		map[string]string{
			"Livepeer-Session-Token": sessionHeaders.Get("Livepeer-Session-Token"),
		},
		http.StatusOK,
		&channels,
	)
	require.Len(t, channels.Channels, 1)
	publishURL := channels.Channels[0].URL
	require.Contains(t, publishURL, orch.httpBase+"/ai/trickle/")

	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_MEDIA_TIMEOUT", "40s"))
	defer cancel()
	cmd := fixtureCommand(t, ctx, "python-media", "uv", pythonRunnerDir, "run", "--frozen", "--extra", "examples", "python", "-u", "-c",
		pythonMultiTrackMediaRoundTripScript,
		publishURL,
	)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf(
			"python multi-track media round-trip timed out\npython stderr:\n%s\norchestrator logs:\n%s",
			stderr.String(),
			readLog(t, orch.logPath),
		)
	}
	require.NoError(
		t,
		err,
		"python multi-track media round-trip failed\npython stderr:\n%s\norchestrator logs:\n%s",
		stderr.String(),
		readLog(t, orch.logPath),
	)

	var result multiTrackMediaRoundTripResult
	require.NoError(
		t,
		json.Unmarshal([]byte(strings.TrimSpace(stdout.String())), &result),
		"invalid helper output %q\npython stderr:\n%s\norchestrator logs:\n%s",
		stdout.String(),
		stderr.String(),
		readLog(t, orch.logPath),
	)
	failureMessage := strings.Join(result.Errors, "; ")
	require.Equal(t, 2, result.ObservedVideoTracks, failureMessage)
	require.Equal(t, 2, result.ObservedAudioTracks, failureMessage)
	require.Len(t, result.VideoFrameCounts, 2, failureMessage)
	require.Len(t, result.AudioSampleCounts, 2, failureMessage)
	for _, frameCount := range result.VideoFrameCounts {
		require.Positive(t, frameCount, failureMessage)
	}
	for _, sampleCount := range result.AudioSampleCounts {
		require.Positive(t, sampleCount, failureMessage)
	}
	require.True(t, result.VideoSignaturesOK, failureMessage)
	require.True(t, result.AudioFrequenciesOK, failureMessage)
	require.GreaterOrEqual(t, result.AudioFrequencyRatios["440"], 2.5, failureMessage)
	require.GreaterOrEqual(t, result.AudioFrequencyRatios["880"], 2.5, failureMessage)
	require.True(t, result.VerificationOK, failureMessage)
	require.Positive(t, result.SegmentsConsumed)
	require.Positive(t, result.SegmentsStarted)
}

func startOffchainOrchestrator(t *testing.T, optionList ...orchestratorOptions) *testOrchestrator {
	t.Helper()
	require.LessOrEqual(t, len(optionList), 1)
	var options orchestratorOptions
	if len(optionList) == 1 {
		options = optionList[0]
	}
	return startOrchestrator(t, options)
}

func startPythonRunner(t *testing.T, ctx context.Context, script string, orch *testOrchestrator, cfg runnerConfig) *pythonRunner {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	pythonRunnerDir := repoDir(t, envPythonRunnerDir)
	port := fixturePort(t)
	runnerURL := ""
	if composeMode() && script == pythonRunnerSessionControlsScript {
		runnerURL = publicURL(t, "http://127.0.0.1:"+port)
	}
	cmd := fixtureCommand(t, runCtx, "python-runner", "uv", pythonRunnerDir, "run", "--frozen", "python", "-u", "-c", script,
		orch.httpBase,
		liveRunnerSecret,
		cfg.runnerURL,
		liveRunnerApp,
		cfg.mode,
		cfg.label,
		fmt.Sprintf("%d", cfg.capacity),
		cfg.metadata,
		fmt.Sprintf("%t", cfg.proxy),
		string(runnerPrice(cfg).Price), runnerPrice(cfg).Unit, port, runnerURL,
	)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr

	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()

	type lineResult struct {
		line string
		err  error
	}
	lineCh := make(chan lineResult, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		lineCh <- lineResult{line: line, err: err}
	}()

	var line string
	select {
	case result := <-lineCh:
		require.NoError(t, result.err, stderr.String())
		line = result.line
	case <-time.After(envDuration(t, "E2E_READY_TIMEOUT", "30s")):
		cancel()
		<-done
		t.Fatalf("timed out waiting for python-runner registration: %s", stderr.String())
	}

	var meta struct {
		RunnerID        string  `json:"runner_id"`
		OrchestratorURL string  `json:"orchestrator_url"`
		HeartbeatS      float64 `json:"heartbeat_interval_s"`
	}
	require.NoError(t, json.Unmarshal([]byte(line), &meta), line)
	require.NotEmpty(t, meta.RunnerID)
	require.Equal(t, orch.httpBase, meta.OrchestratorURL)
	require.NotZero(t, meta.HeartbeatS)

	runner := &pythonRunner{
		runnerID:        meta.RunnerID,
		orchestratorURL: meta.OrchestratorURL,
		stdin:           stdin,
		cancel:          cancel,
		done:            done,
		stderr:          stderr,
	}
	t.Cleanup(func() {
		runner.Close(t)
	})
	return runner
}

func startGoRunner(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) *goRunnerRegistration {
	t.Helper()
	if composeMode() {
		return startContainerGoRunner(t, ctx, orch, cfg)
	}
	require.NotEmpty(t, cfg.runnerURL)
	price, err := liverunner.NewPriceInfo(string(runnerPrice(cfg).Price), "USD", runnerPrice(cfg).Unit)
	require.NoError(t, err)
	reg, err := liverunner.Register(ctx, liverunner.Options{
		OrchestratorURL: orch.httpBase,
		BootstrapSecret: liveRunnerSecret,
		RunnerURL:       cfg.runnerURL,
		App:             liveRunnerApp,
		Metadata:        cfg.metadata,
		Mode:            cfg.mode,
		Proxy:           cfg.proxy,
		Capacity:        cfg.capacity,
		Label:           cfg.label,
		PriceInfo:       price,
	})
	require.NoError(t, err)
	require.NotEmpty(t, reg.RunnerID())
	require.Equal(t, orch.httpBase, reg.OrchestratorURL())
	require.NotZero(t, reg.HeartbeatInterval())

	runner := &goRunnerRegistration{ctx: ctx, reg: reg}
	t.Cleanup(func() {
		runner.Close(t)
	})
	return runner
}

func runPythonRunnerMissingPrice(t *testing.T, orch *testOrchestrator, secret, runnerURL, mode, label string) {
	t.Helper()
	pythonRunnerDir := repoDir(t, envPythonRunnerDir)
	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_JOB_TIMEOUT", "30s"))
	defer cancel()
	cmd := fixtureCommand(t, ctx, "python-negative", "uv", pythonRunnerDir, "run", "--frozen", "python", "-u", "-c", pythonRunnerMissingPriceScript,
		orch.httpBase,
		secret,
		runnerURL,
		liveRunnerApp,
		mode,
		label,
	)
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("timed out waiting for python-runner missing-price check: %s", string(output))
	}
	require.NoError(t, err, string(output))
}

func (r *pythonRunner) RunnerID() string {
	return r.runnerID
}

func (r *pythonRunner) OrchestratorURL() string {
	return r.orchestratorURL
}

func (r *pythonRunner) Close(t *testing.T) {
	t.Helper()
	if r.stdin == nil {
		return
	}
	_ = r.stdin.Close()
	r.stdin = nil
	select {
	case err := <-r.done:
		require.NoError(t, err, r.stderr.String())
	case <-time.After(5 * time.Second):
		r.cancel()
		err := <-r.done
		require.NoError(t, err, r.stderr.String())
	}
}

func (r *goRunnerRegistration) RunnerID() string {
	return r.reg.RunnerID()
}

func (r *goRunnerRegistration) OrchestratorURL() string {
	return r.reg.OrchestratorURL()
}

func (r *goRunnerRegistration) Close(t *testing.T) {
	t.Helper()
	require.NoError(t, r.reg.Close(r.ctx))
}

func freeAddr(t *testing.T) string { t.Helper(); return fixturePoolFor(t).take(t) }

func waitForHTTP(t *testing.T, method, target string, status int, logPath string) {
	t.Helper()
	deadline := time.Now().Add(envDuration(t, "E2E_READY_TIMEOUT", "30s"))
	for time.Now().Before(deadline) {
		got, _, err := requestHTTP(method, target, nil, nil)
		if err == nil && got == status {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("INFRASTRUCTURE: waiting for %s\n%s", target, readLog(t, logPath))
}

func registerRunnerHTTP(t *testing.T, orch *testOrchestrator, payload heartbeatPayload, auth string) heartbeatResponse {
	t.Helper()
	var resp heartbeatResponse
	requireJSONStatus(t, http.MethodPost, orch.httpBase+"/runners/heartbeat", payload, map[string]string{"Authorization": auth}, http.StatusOK, &resp)
	return resp
}

func requireHeartbeatStatus(t *testing.T, orch *testOrchestrator, payload heartbeatPayload, auth string, status int) {
	t.Helper()
	requireJSONStatus(t, http.MethodPost, orch.httpBase+"/runners/heartbeat", payload, map[string]string{"Authorization": auth}, status, nil)
}

func unregisterRunner(t *testing.T, orch *testOrchestrator, runnerID, auth string, status int) {
	t.Helper()
	requireJSONStatus(t, http.MethodPost, orch.httpBase+"/runners/"+url.PathEscape(runnerID)+"/unregister", nil, map[string]string{"Authorization": auth}, status, nil)
}

func discover(t *testing.T, orch *testOrchestrator) []discoveryEntry {
	t.Helper()
	var entries []discoveryEntry
	requireJSONStatus(t, http.MethodGet, orch.httpBase+"/discovery", nil, nil, http.StatusOK, &entries)
	return entries
}

func findRunner(t *testing.T, orch *testOrchestrator, matches func(discoveryRunner) bool) *discoveryRunner {
	t.Helper()
	for _, entry := range discover(t, orch) {
		for _, runner := range entry.Runners {
			if matches(runner) {
				return &runner
			}
		}
	}
	return nil
}

func requireDiscoveredRunner(t *testing.T, orch *testOrchestrator, runnerID string) discoveryRunner {
	t.Helper()
	runner := findRunner(t, orch, func(r discoveryRunner) bool { return strings.Contains(r.URL, "/apps/"+runnerID+"/") })
	require.NotNil(t, runner, "runner %q not found in discovery", runnerID)
	return *runner
}

func requireDiscoveredRunnerByMetadata(t *testing.T, orch *testOrchestrator, metadata string) discoveryRunner {
	t.Helper()
	runner := findRunner(t, orch, func(r discoveryRunner) bool { return r.Metadata == metadata })
	require.NotNil(t, runner, "runner with metadata %q not found in discovery", metadata)
	return *runner
}

func requireRunnerCapacity(t *testing.T, runner discoveryRunner, capacity, used, available int) {
	t.Helper()
	require.Equal(t, capacity, runner.Capacity)
	require.Equal(t, used, runner.CapacityUsed)
	require.Equal(t, available, runner.CapacityAvailable)
}

func requireRunnerAbsent(t *testing.T, orch *testOrchestrator, runnerID string) {
	t.Helper()
	eventually(t, func() bool {
		return findRunner(t, orch, func(r discoveryRunner) bool { return strings.Contains(r.URL, "/apps/"+runnerID+"/") }) == nil
	}, 5*time.Second, 25*time.Millisecond)
}

func requireRunnerMetadataAbsent(t *testing.T, orch *testOrchestrator, metadata string) {
	t.Helper()
	eventually(t, func() bool {
		return findRunner(t, orch, func(r discoveryRunner) bool { return r.Metadata == metadata }) == nil
	}, 5*time.Second, 25*time.Millisecond)
}

func reserveSession(t *testing.T, orch *testOrchestrator, runnerID string) sessionResponse {
	t.Helper()
	var resp sessionResponse
	requireJSONStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/session", nil, nil, http.StatusOK, &resp)
	require.NotEmpty(t, resp.SessionID)
	require.NotEmpty(t, resp.AppURL)
	return resp
}

func stopSession(t *testing.T, orch *testOrchestrator, runnerID, sessionID string) {
	t.Helper()
	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/session/"+url.PathEscape(sessionID)+"/stop", nil, http.StatusNoContent)
}

func registerStaticRunnerConfig(t *testing.T, orch *testOrchestrator, cfg staticRunnerConfig, status int) {
	t.Helper()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	requireStatus(t, http.MethodPost, orch.cliBase+"/registerLiveRunners", bytes.NewReader(data), status)
}

func requireChannelStatus(t *testing.T, endpoint, token string, status int) {
	t.Helper()
	body := `{"channels":[{"name":"events","mime_type":"application/json"}]}`
	got, payload, err := requestHTTP(http.MethodPost, endpoint, strings.NewReader(body), map[string]string{"Content-Type": "application/json", "Livepeer-Session-Token": token})
	require.NoError(t, err)
	require.Equal(t, status, got, string(payload))
	if status == http.StatusOK {
		var decoded channelResponse
		require.NoError(t, json.Unmarshal(payload, &decoded))
		require.Len(t, decoded.Channels, 1)
	}
}

func requireJSONStatus(t *testing.T, method, target string, payload any, headers map[string]string, status int, out any) {
	t.Helper()
	got, data, err := requestHTTP(method, target, payload, headers)
	require.NoError(t, err)
	require.Equal(t, status, got, string(data))
	if out != nil {
		require.NoError(t, json.Unmarshal(data, out), string(data))
	}
}

func respondJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(data)
	require.NoError(t, err)
}

func requireStatus(t *testing.T, method, target string, body io.Reader, status int) {
	t.Helper()
	_, err := checkStatus(method, target, body, status)
	require.NoError(t, err)
}

func checkStatus(method, target string, body io.Reader, status int) (string, error) {
	got, data, err := requestHTTP(method, target, body, nil)
	if err != nil {
		return "", err
	}
	if got != status {
		return "", fmt.Errorf("expected status %d, got %d: %s", status, got, data)
	}
	return string(data), nil
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	if len(data) > 12000 {
		data = data[len(data)-12000:]
	}
	return string(data)
}

func requireReceiveN(t *testing.T, ch <-chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for receive %d/%d", i+1, n)
		}
	}
}

func requireReceiveHeader(t *testing.T, ch <-chan http.Header) http.Header {
	t.Helper()
	select {
	case h := <-ch:
		return h
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for proxied runner request")
		return nil
	}
}
