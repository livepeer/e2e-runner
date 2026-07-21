package e2escope

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	liverunner "github.com/livepeer/golang-runner"
	"github.com/stretchr/testify/require"
)

const (
	liveRunnerSecret = "live-runner-secret"
	liveRunnerApp    = "livepeer-sample/echo"
)

const (
	envGoLivepeerDir   = "GO_LIVEPEER_DIR"
	envLivepeerBin     = "LIVEPEER_BINARY"
	envPythonRunnerDir = "PYTHON_RUNNER_DIR"

	envOnchainOrchURL = "LIVEPEER_ONCHAIN_ORCH_URL"
	envOnchainCLIURL  = "LIVEPEER_ONCHAIN_CLI_URL"
	envOnchainSecret  = "LIVEPEER_ONCHAIN_ORCH_SECRET"
)

var testHTTPClient = &http.Client{Timeout: 5 * time.Second}

//go:embed testdata/python_runner_register.py
var pythonRunnerRegisterScript string

//go:embed testdata/python_runner_session_controls.py
var pythonRunnerSessionControlsScript string

//go:embed testdata/python_runner_missing_price.py
var pythonRunnerMissingPriceScript string

type testOrchestrator struct {
	httpBase string
	cliBase  string
	cancel   context.CancelFunc
	logPath  string
}

type heartbeatPayload struct {
	RunnerID  string    `json:"runner_id,omitempty"`
	Label     string    `json:"label,omitempty"`
	RunnerURL string    `json:"runner_url"`
	Version   string    `json:"version,omitempty"`
	Metadata  string    `json:"metadata,omitempty"`
	Status    string    `json:"status,omitempty"`
	Mode      string    `json:"mode,omitempty"`
	App       string    `json:"app"`
	Capacity  int       `json:"capacity"`
	PriceInfo priceInfo `json:"price_info"`
}

type priceInfo struct {
	PricePerUnit  int64  `json:"price_per_unit"`
	PixelsPerUnit int64  `json:"pixels_per_unit"`
	Unit          string `json:"unit,omitempty"`
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
	runnerURL string
	mode      string
	label     string
	metadata  string
	capacity  int
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
	stderr          *bytes.Buffer
}

type goRunnerRegistration struct {
	ctx context.Context
	reg *liverunner.Runner
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
	var reg *liverunner.Runner
	var upstream *httptest.Server
	upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		case "/stop":
			if err := liverunner.StopSession(ctx, nil, r, time.Second); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			if strings.HasPrefix(r.URL.Path, "/proxy-target") {
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

func runPersistentRunnerScenario(t *testing.T, ctx context.Context, orch *testOrchestrator, impl runnerImplementation, cfg runnerConfig) {
	t.Helper()
	cfg.runnerURL = "http://runner.invalid"
	reg := impl.Start(t, ctx, orch, cfg)
	runnerID := reg.RunnerID()
	require.Equal(t, orch.httpBase, reg.OrchestratorURL())

	runner := requireDiscoveredRunner(t, orch, runnerID)
	require.Equal(t, liveRunnerApp, runner.App)
	require.Equal(t, cfg.metadata, runner.Metadata)
	require.Equal(t, liverunner.ModePersistent, runner.Mode)
	require.Contains(t, runner.URL, "/apps/"+runnerID+"/session")
	requireRunnerCapacity(t, runner, cfg.capacity, 0, cfg.capacity)

	s1 := reserveSession(t, orch, runnerID)
	requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), cfg.capacity, 1, cfg.capacity-1)
	s2 := reserveSession(t, orch, runnerID)
	require.NotEqual(t, s1.SessionID, s2.SessionID)
	requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), cfg.capacity, cfg.capacity, 0)
	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/session", nil, http.StatusServiceUnavailable)

	stopSession(t, orch, runnerID, s1.SessionID)
	requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), cfg.capacity, cfg.capacity-1, 1)
	s3 := reserveSession(t, orch, runnerID)
	require.NotEmpty(t, s3.SessionID)

	reg.Close(t)
	requireRunnerAbsent(t, orch, runnerID)
	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/session", nil, http.StatusNotFound)
}

func runSingleShotRunnerScenario(t *testing.T, ctx context.Context, orch *testOrchestrator, impl runnerImplementation, cfg runnerConfig) {
	t.Helper()
	release := make(chan struct{})
	entered := make(chan struct{}, cfg.capacity)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			entered <- struct{}{}
			<-release
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	cfg.runnerURL = upstream.URL
	reg := impl.Start(t, ctx, orch, cfg)
	runnerID := reg.RunnerID()
	require.Equal(t, orch.httpBase, reg.OrchestratorURL())

	runner := requireDiscoveredRunner(t, orch, runnerID)
	require.Equal(t, liveRunnerApp, runner.App)
	require.Equal(t, cfg.metadata, runner.Metadata)
	require.Equal(t, liverunner.ModeSingleShot, runner.Mode)
	require.Contains(t, runner.URL, "/apps/"+runnerID+"/app")
	requireRunnerCapacity(t, runner, cfg.capacity, 0, cfg.capacity)

	var wg sync.WaitGroup
	errs := make(chan error, cfg.capacity)
	for i := 0; i < cfg.capacity; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := checkStatus(http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/app/hold", strings.NewReader("x"), http.StatusOK)
			errs <- err
		}()
	}
	requireReceiveN(t, entered, cfg.capacity)
	requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), cfg.capacity, cfg.capacity, 0)
	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/app/blocked", strings.NewReader("x"), http.StatusServiceUnavailable)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), cfg.capacity, 0, cfg.capacity)

	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/app/after-release", strings.NewReader("x"), http.StatusOK)
	reg.Close(t)
	requireRunnerAbsent(t, orch, runnerID)
	requireStatus(t, http.MethodPost, orch.httpBase+"/apps/"+url.PathEscape(runnerID)+"/app/after-close", strings.NewReader("x"), http.StatusNotFound)
}

func TestRunnerImplementations(t *testing.T) {
	orch := startOffchainOrchestrator(t)
	ctx := context.Background()

	for _, impl := range runnerImplementations() {
		impl := impl
		t.Run(impl.Name(), func(t *testing.T) {
			for _, tc := range []struct {
				name string
				mode string
			}{
				{name: "persistent", mode: liverunner.ModePersistent},
				{name: "single-shot", mode: liverunner.ModeSingleShot},
			} {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					cfg := runnerConfig{
						mode:     tc.mode,
						label:    impl.Name() + "-" + tc.name,
						metadata: fmt.Sprintf(`{"implementation":%q,"mode":%q}`, impl.Name(), tc.mode),
						capacity: 2,
					}
					if tc.mode == liverunner.ModePersistent {
						runPersistentRunnerScenario(t, ctx, orch, impl, cfg)
						return
					}
					runSingleShotRunnerScenario(t, ctx, orch, impl, cfg)
				})
			}
		})
	}
}

func TestLiveRunnerServerSurface(t *testing.T) {
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
			PriceInfo: priceInfo{PricePerUnit: 10, PixelsPerUnit: 1, Unit: "WEI"},
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
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			PriceInfo: priceInfo{PricePerUnit: 10, PixelsPerUnit: 1, Unit: "WEI"},
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
		healthy := true
		healthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if healthy {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer healthSrv.Close()

		registerStaticRunner(t, orch, fmt.Sprintf(`{"runners":[{"label":"static-echo","routing":"label","runner_url":"http://runner.invalid","app":%q,"metadata":"static application metadata","mode":"persistent","capacity":1,"price_info":{"price_per_unit":10,"pixels_per_unit":1,"unit":"WEI"},"health_url":%q}]}`, liveRunnerApp, healthSrv.URL), http.StatusOK)
		runner := requireDiscoveredRunner(t, orch, "static-echo")
		require.Contains(t, runner.URL, "/apps/static-echo/session")
		require.Equal(t, "static application metadata", runner.Metadata)
		requireRunnerCapacity(t, runner, 1, 0, 1)
		session := reserveSession(t, orch, "static-echo")
		require.NotEmpty(t, session.SessionID)

		healthy = false
		registerStaticRunner(t, orch, fmt.Sprintf(`{"runners":[{"label":"static-echo","routing":"label","runner_url":"http://runner.invalid","app":%q,"metadata":"static application metadata","mode":"persistent","capacity":1,"price_info":{"price_per_unit":10,"pixels_per_unit":1,"unit":"WEI"},"health_url":%q}]}`, liveRunnerApp, healthSrv.URL), http.StatusOK)
		requireRunnerAbsent(t, orch, "static-echo")
		requireStatus(t, http.MethodPost, orch.httpBase+"/apps/static-echo/session", nil, http.StatusNotFound)
	})
}

func TestRunnerSessionControlAPIs(t *testing.T) {
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
			discovered := requireDiscoveredRunner(t, orch, runnerID)
			require.Equal(t, cfg.metadata, discovered.Metadata)
			requireRunnerCapacity(t, discovered, 1, 1, 0)

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

			requireStatus(t, http.MethodPost, session.AppURL+"/stop", nil, http.StatusNoContent)
			requireRunnerCapacity(t, requireDiscoveredRunner(t, orch, runnerID), 1, 0, 1)
			requireStatus(t, http.MethodPost, createdProxy.ProxyURL+"/after-stop", strings.NewReader("x"), http.StatusNotFound)
			_ = reserveSession(t, orch, runnerID)

			reg.Close(t)
			requireRunnerAbsent(t, orch, runnerID)
		})
	}
}

func TestOnchainPriceInfoRequired(t *testing.T) {
	orchURL := strings.TrimRight(os.Getenv(envOnchainOrchURL), "/")
	secret := os.Getenv(envOnchainSecret)
	if orchURL == "" || secret == "" {
		t.Skipf("set %s and %s to run optional on-chain price_info e2e", envOnchainOrchURL, envOnchainSecret)
	}
	orch := &testOrchestrator{
		httpBase: orchURL,
		cliBase:  strings.TrimRight(os.Getenv(envOnchainCLIURL), "/"),
	}

	t.Run("python-runner runtime runner without price fails", func(t *testing.T) {
		runPythonRunnerMissingPrice(t, orch, secret, "http://runner.invalid", "persistent", "py-onchain-no-price")
	})

	t.Run("golang-runner runtime runner without price fails", func(t *testing.T) {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}))
		defer upstream.Close()

		_, err := liverunner.Register(context.Background(), liverunner.Options{
			OrchestratorURL: orch.httpBase,
			BootstrapSecret: secret,
			RunnerURL:       upstream.URL,
			App:             liveRunnerApp,
			Mode:            liverunner.ModePersistent,
			Capacity:        1,
			Label:           "go-onchain-no-price",
		})
		requirePriceInfoError(t, err)
	})

	t.Run("static runner without price fails", func(t *testing.T) {
		if orch.cliBase == "" {
			t.Skipf("set %s to run optional static runner on-chain price_info e2e", envOnchainCLIURL)
		}
		body := fmt.Sprintf(`{"runners":[{"label":"static-onchain-no-price","routing":"label","runner_url":"http://runner.invalid","app":%q,"mode":"persistent","capacity":1,"health_url":"http://runner.invalid/health"}]}`, liveRunnerApp)
		respBody := requireStatusBody(t, http.MethodPost, orch.cliBase+"/registerLiveRunners", strings.NewReader(body), http.StatusBadRequest)
		require.Contains(t, respBody, "price_info")
	})
}

func startOffchainOrchestrator(t *testing.T) *testOrchestrator {
	t.Helper()
	goLivepeerDir := repoDir(t, envGoLivepeerDir, "go-livepeer")
	livepeerBin := os.Getenv(envLivepeerBin)
	if livepeerBin == "" {
		livepeerBin = filepath.Join(goLivepeerDir, "livepeer")
	}
	httpAddr := freeAddr(t)
	cliAddr := freeAddr(t)
	rtmpAddr := freeAddr(t)
	serviceAddr := "http://" + httpAddr
	ctx, cancel := context.WithCancel(context.Background())

	logFile, err := os.Create(filepath.Join(t.TempDir(), "livepeer.log"))
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, livepeerBin,
		"-network", "offchain",
		"-orchestrator",
		"-orchSecret", liveRunnerSecret,
		"-useLiveRunners",
		"-serviceAddr", serviceAddr,
		"-httpAddr", "http://"+httpAddr,
		"-cliAddr", cliAddr,
		"-rtmpAddr", rtmpAddr,
		"-datadir", t.TempDir(),
		"-startupAvailabilityCheck=false",
	)
	cmd.Dir = goLivepeerDir
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()

	orch := &testOrchestrator{
		httpBase: serviceAddr,
		cliBase:  "http://" + cliAddr,
		cancel:   cancel,
		logPath:  logFile.Name(),
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		_ = logFile.Close()
	})
	waitForHTTP(t, orch.cliBase+"/status", http.StatusOK, orch.logPath)
	waitForHTTP(t, orch.httpBase+"/discovery", http.StatusOK, orch.logPath)
	return orch
}

func startPythonRunner(t *testing.T, ctx context.Context, script string, orch *testOrchestrator, cfg runnerConfig) *pythonRunner {
	t.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	pythonRunnerDir := repoDir(t, envPythonRunnerDir, filepath.Join("livepeer", "python-runner"))
	cmd := exec.CommandContext(runCtx,
		"uv", "run", "python", "-u", "-c", script,
		orch.httpBase,
		liveRunnerSecret,
		cfg.runnerURL,
		liveRunnerApp,
		cfg.mode,
		cfg.label,
		fmt.Sprintf("%d", cfg.capacity),
		cfg.metadata,
	)
	cmd.Dir = pythonRunnerDir
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr := &bytes.Buffer{}
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
	case <-time.After(10 * time.Second):
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
	require.NotEmpty(t, cfg.runnerURL)
	reg, err := liverunner.Register(ctx, liverunner.Options{
		OrchestratorURL: orch.httpBase,
		BootstrapSecret: liveRunnerSecret,
		RunnerURL:       cfg.runnerURL,
		App:             liveRunnerApp,
		Metadata:        cfg.metadata,
		Mode:            cfg.mode,
		Capacity:        cfg.capacity,
		Label:           cfg.label,
		PriceInfo:       liverunner.PriceInfo{PricePerUnit: 10, PixelsPerUnit: 1, Unit: "WEI"},
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
	pythonRunnerDir := repoDir(t, envPythonRunnerDir, filepath.Join("livepeer", "python-runner"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx,
		"uv", "run", "python", "-u", "-c", pythonRunnerMissingPriceScript,
		orch.httpBase,
		secret,
		runnerURL,
		liveRunnerApp,
		mode,
		label,
	)
	cmd.Dir = pythonRunnerDir
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("timed out waiting for python-runner missing-price check: %s", string(output))
	}
	require.NoError(t, err, string(output))
}

func requirePriceInfoError(t *testing.T, err error) {
	t.Helper()
	var httpErr *liverunner.HTTPError
	require.Error(t, err)
	require.True(t, errors.As(err, &httpErr), "expected *liverunner.HTTPError, got %T: %v", err, err)
	require.Equal(t, http.StatusBadRequest, httpErr.StatusCode)
	require.Contains(t, httpErr.Body, "price_info")
}

func repoDir(t *testing.T, envName, defaultRel string, fallbackEnvNames ...string) string {
	t.Helper()
	if dir := os.Getenv(envName); dir != "" {
		return dir
	}
	for _, fallbackEnvName := range fallbackEnvNames {
		if dir := os.Getenv(fallbackEnvName); dir != "" {
			return dir
		}
	}
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	return filepath.Join(home, defaultRel)
}

func (r *pythonRunner) RunnerID() string {
	return r.runnerID
}

func (r *pythonRunner) OrchestratorURL() string {
	return r.orchestratorURL
}

func (r *pythonRunner) Close(t *testing.T) {
	t.Helper()
	r.close(t)
}

func (r *pythonRunner) close(t *testing.T) {
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

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().String()
}

func waitForHTTP(t *testing.T, target string, status int, logPath string) {
	t.Helper()
	require.Eventuallyf(t, func() bool {
		resp, err := testHTTPClient.Get(target)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode == status
	}, 10*time.Second, 50*time.Millisecond, "waiting for %s\n%s", target, readLog(t, logPath))
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
	req, err := http.NewRequest(http.MethodPost, orch.httpBase+"/runners/"+url.PathEscape(runnerID)+"/unregister", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", auth)
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	require.Equal(t, status, resp.StatusCode, string(body))
}

func discover(t *testing.T, orch *testOrchestrator) []discoveryEntry {
	t.Helper()
	var entries []discoveryEntry
	requireJSONStatus(t, http.MethodGet, orch.httpBase+"/discovery", nil, nil, http.StatusOK, &entries)
	return entries
}

func requireDiscoveredRunner(t *testing.T, orch *testOrchestrator, runnerID string) discoveryRunner {
	t.Helper()
	for _, entry := range discover(t, orch) {
		for _, runner := range entry.Runners {
			if strings.Contains(runner.URL, "/apps/"+runnerID+"/") {
				return runner
			}
		}
	}
	t.Fatalf("runner %q not found in discovery", runnerID)
	return discoveryRunner{}
}

func requireRunnerCapacity(t *testing.T, runner discoveryRunner, capacity, used, available int) {
	t.Helper()
	require.Equal(t, capacity, runner.Capacity)
	require.Equal(t, used, runner.CapacityUsed)
	require.Equal(t, available, runner.CapacityAvailable)
}

func requireRunnerAbsent(t *testing.T, orch *testOrchestrator, runnerID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, entry := range discover(t, orch) {
			for _, runner := range entry.Runners {
				if strings.Contains(runner.URL, "/apps/"+runnerID+"/") {
					return false
				}
			}
		}
		return true
	}, 2*time.Second, 25*time.Millisecond)
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

func registerStaticRunner(t *testing.T, orch *testOrchestrator, body string, status int) {
	t.Helper()
	requireStatus(t, http.MethodPost, orch.cliBase+"/registerLiveRunners", strings.NewReader(body), status)
}

func requireChannelStatus(t *testing.T, endpoint, token string, status int) {
	t.Helper()
	body := `{"channels":[{"name":"events","mime_type":"application/json"}]}`
	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Livepeer-Session-Token", token)
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	require.Equal(t, status, resp.StatusCode, string(payload))
	if status == http.StatusOK {
		var decoded channelResponse
		require.NoError(t, json.Unmarshal(payload, &decoded))
		require.Len(t, decoded.Channels, 1)
	}
}

func requireJSONStatus(t *testing.T, method, target string, payload any, headers map[string]string, status int, out any) {
	t.Helper()
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		require.NoError(t, err)
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, target, body)
	require.NoError(t, err)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := testHTTPClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	require.Equal(t, status, resp.StatusCode, string(data))
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

func requireStatusBody(t *testing.T, method, target string, body io.Reader, status int) string {
	t.Helper()
	data, err := checkStatus(method, target, body, status)
	require.NoError(t, err)
	return data
}

func checkStatus(method, target string, body io.Reader, status int) (string, error) {
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return "", err
	}
	resp, err := testHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != status {
		return "", fmt.Errorf("expected status %d, got %d: %s", status, resp.StatusCode, string(data))
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
