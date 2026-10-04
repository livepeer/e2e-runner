package e2escope

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

var buildRoot string
var builds sync.Map

func TestMain(m *testing.M) {
	path := envDefault("E2E_ENV_FILE", "test.env")
	if err := godotenv.Load(path); err != nil {
		fmt.Fprintf(os.Stderr, "load %s: %v\n", path, err)
		os.Exit(1)
	}
	liveRunnerSecret = uuid.NewString()
	if path := os.Getenv("E2E_BOOTSTRAP_SECRET_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		liveRunnerSecret = strings.TrimSpace(string(data))
		if liveRunnerSecret == "" {
			fmt.Fprintln(os.Stderr, "empty E2E_BOOTSTRAP_SECRET_FILE")
			os.Exit(1)
		}
	}
	timeout, err := time.ParseDuration(envDefault("E2E_HTTP_TIMEOUT", "5s"))
	if err != nil || timeout <= 0 {
		fmt.Fprintln(os.Stderr, "invalid E2E_HTTP_TIMEOUT")
		os.Exit(1)
	}
	testHTTPClient.Timeout = timeout
	var errBuild error
	buildRoot, errBuild = os.MkdirTemp("", "e2e-scope-build-")
	if errBuild != nil {
		panic(errBuild)
	}
	code := m.Run()
	_ = os.RemoveAll(buildRoot)
	os.Exit(code)
}

func envDefault(name, fallback string) string {
	if s := os.Getenv(name); s != "" {
		return s
	}
	return fallback
}
func envDuration(t *testing.T, name, fallback string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(envDefault(name, fallback))
	require.NoError(t, err, name)
	require.Positive(t, d, name)
	return d
}
func selectedBackends(t *testing.T, key string) []string {
	t.Helper()
	values := strings.Split(envDefault(key, "go-livepeer,node"), ",")
	for _, v := range values {
		require.Contains(t, []string{"go-livepeer", "node"}, v, key)
	}
	return values
}
func forEachOrchestrator(t *testing.T, run func(*testing.T)) {
	t.Helper()
	for _, backend := range selectedBackends(t, "E2E_ORCHESTRATORS") {
		t.Run(backend, func(t *testing.T) { fixturePoolFor(t); run(t) })
	}
}
func forEachPair(t *testing.T, run func(*testing.T, string, string)) {
	t.Helper()
	for _, orch := range selectedBackends(t, "E2E_ORCHESTRATORS") {
		t.Run(orch, func(t *testing.T) {
			for _, signer := range selectedBackends(t, "E2E_SIGNERS") {
				t.Run(signer, func(t *testing.T) { run(t, orch, signer) })
			}
		})
	}
}
func backendFor(t *testing.T) string {
	for _, part := range strings.Split(t.Name(), "/") {
		if part == "node" || part == "go-livepeer" {
			return part
		}
	}
	t.Fatal("orchestrator backend missing from test name")
	return ""
}
func runnerPrice(cfg runnerConfig) priceInfo {
	if cfg.price.Price != "" {
		return cfg.price
	}
	return priceInfo{Price: "10", Currency: "usd", Unit: "fixed"}
}

type buildResult struct {
	once   sync.Once
	err    error
	output []byte
}

func binary(t *testing.T, backend string) string {
	t.Helper()
	spec, ok := map[string]struct{ dir, override, name string }{
		"go-livepeer":     {envGoLivepeerDir, envLivepeerBin, "livepeer"},
		"go-livepeer-cli": {envGoLivepeerDir, "E2E_GO_LIVEPEER_CLI_BINARY", "livepeer_cli"},
		"node":            {"E2E_NODE_DIR", "E2E_NODE_BINARY", "livepeer"},
		"clearinghouse":   {"E2E_CLEARINGHOUSE_DIR", "E2E_CLEARINGHOUSE_BINARY", "clearinghouse"},
	}[backend]
	require.True(t, ok, "unknown backend %q", backend)
	path := os.Getenv(spec.override)
	if path == "" {
		dir := repoDir(t, spec.dir)
		path = filepath.Join(dir, spec.name)
		if backend == "node" || backend == "clearinghouse" {
			path = filepath.Join(buildRoot, spec.name)
			targets := []string{"./cmd/" + spec.name}
			if backend == "node" {
				targets = append(targets, "./cmd/livepeer-orchestrator", "./cmd/livepeer-signer", "./cmd/livepeer-chain")
			}
			value, _ := builds.LoadOrStore(backend, &buildResult{})
			result := value.(*buildResult)
			result.once.Do(func() {
				ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_BUILD_TIMEOUT", "3m"))
				defer cancel()
				cmd := exec.CommandContext(ctx, envDefault("E2E_GO_BINARY", "go"), append([]string{"build", "-mod=readonly", "-o", buildRoot}, targets...)...)
				cmd.Dir = dir
				result.output, result.err = cmd.CombinedOutput()
			})
			require.NoError(t, result.err, "build %s: %s", backend, result.output)
			for _, target := range targets[1:] {
				name := filepath.Base(target)
				recordBinary(t, "node-"+strings.TrimPrefix(name, "livepeer-"), filepath.Join(buildRoot, name))
			}
		}
	}
	path = absPath(t, path)
	recordBinary(t, backend, path)
	return path
}

func absPath(t *testing.T, path string) string {
	t.Helper()
	absolute, err := filepath.Abs(path)
	require.NoError(t, err)
	return absolute
}

func repoDir(t *testing.T, key string) string {
	t.Helper()
	dir := os.Getenv(key)
	require.NotEmpty(t, dir, "INFRASTRUCTURE: %s must point to a source checkout", key)
	dir = absPath(t, dir)
	require.DirExists(t, dir, "INFRASTRUCTURE: %s", key)
	return dir
}

type childProcess struct {
	cmd      *exec.Cmd
	done     chan error
	logPath  string
	stopOnce sync.Once
}

func startChild(t *testing.T, name, bin, dir string, args []string, extraEnv ...string) *childProcess {
	t.Helper()
	artifactDir := envDefault("E2E_ARTIFACT_DIR", "artifacts")
	require.NoError(t, os.MkdirAll(artifactDir, 0755))
	absolute, err := filepath.Abs(artifactDir)
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(t.Name() + name + fmt.Sprint(time.Now().UnixNano())))
	logFile, err := os.Create(filepath.Join(absolute, fmt.Sprintf("%s-%x.log", name, digest[:6])))
	require.NoError(t, err)
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	p := &childProcess{cmd: cmd, done: make(chan error, 1), logPath: logFile.Name()}
	require.NoError(t, cmd.Start())
	go func() { p.done <- cmd.Wait(); _ = logFile.Close(); close(p.done) }()
	t.Cleanup(func() { p.stop() })
	return p
}
func (p *childProcess) stop() {
	p.stopOnce.Do(func() {
		_ = p.cmd.Process.Signal(os.Interrupt)
		select {
		case <-p.done:
		case <-time.After(3 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
		}
	})
}

func writeTOML(t *testing.T, path string, value any) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, toml.NewEncoder(&buf).Encode(value))
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0600))
	return path
}
func secretFile(t *testing.T, dir, name, value string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(value), 0600))
	return path
}

func writeStaticConfig(t *testing.T, cfg staticRunnerConfig) string {
	t.Helper()
	if backendFor(t) == "go-livepeer" {
		data, err := json.Marshal(cfg)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "runners.json")
		require.NoError(t, os.WriteFile(path, data, 0600))
		return path
	}
	var buf bytes.Buffer
	for _, r := range cfg.Runners {
		fmt.Fprintf(&buf, "[[Runners]]\nID = %q\nRunnerURL = %q\nApp = %q\nMetadata = %q\nMode = %q\nCapacity = %d\nProxy = %t\nHealthURL = %q\n", r.Label, r.RunnerURL, r.App, r.Metadata, r.Mode, r.Capacity, r.Proxy, r.HealthURL)
		fmt.Fprintf(&buf, "[Runners.PriceInfo]\nPrice = %s\nCurrency = %q\nUnit = %q\n", envPrice(r.PriceInfo), r.PriceInfo.Currency, r.PriceInfo.Unit)
	}
	path := filepath.Join(t.TempDir(), "runners.toml")
	require.NoError(t, os.WriteFile(path, buf.Bytes(), 0600))
	return path
}
func envPrice(p priceInfo) string {
	if p.Price == "" {
		return "0"
	}
	return string(p.Price)
}

func startOrchestrator(t *testing.T, opts orchestratorOptions) *testOrchestrator {
	t.Helper()
	backend := opts.backend
	if backend == "" {
		backend = backendFor(t)
	}
	httpAddr, cliAddr := freeAddr(t), freeAddr(t)
	base := "http://" + httpAddr
	template := opts.proxyURLTemplate
	if template != "" {
		_, port, err := net.SplitHostPort(httpAddr)
		require.NoError(t, err)
		template = strings.ReplaceAll(template, "{http_port}", port)
	}
	orch := &testOrchestrator{backend: backend, httpBase: base, cliBase: "http://" + cliAddr, proxyURLTemplate: template}
	var args []string
	var dir string
	stateDir := opts.dir
	if stateDir == "" {
		stateDir = t.TempDir()
	}
	opts.dir = stateDir
	if backend == "node" {
		dir = opts.dir
		if dir == "" {
			dir = t.TempDir()
		}
		orch.cliBase = base
		config := map[string]any{"Listen": httpAddr, "MetricsListen": cliAddr, "ServiceURL": base, "BootstrapSecretFile": secretFile(t, dir, "bootstrap", liveRunnerSecret), "HeartbeatInterval": "200ms", "HeartbeatTTL": "30s", "ProxyURLTemplate": template, "RunnerGrants": []string{}, "HealthGrants": []string{}, "SessionProxyGrants": []string{}}
		// Grants require exact host:port. Reserve a loopback range through configured
		// test fixture URLs, which start before the orchestrator.
		grants := fixtureGrants(t)
		config["RunnerGrants"] = grants
		config["HealthGrants"] = grants
		config["SessionProxyGrants"] = grants
		if opts.staticConfigPath != "" {
			if strings.HasSuffix(opts.staticConfigPath, ".json") {
				data, err := os.ReadFile(opts.staticConfigPath)
				require.NoError(t, err)
				var cfg staticRunnerConfig
				require.NoError(t, json.Unmarshal(data, &cfg))
				opts.staticConfigPath = writeStaticConfig(t, cfg)
			}
			config["RunnerConfig"] = opts.staticConfigPath
		}
		if opts.account != nil {
			config["RedeemerDB"] = filepath.Join(dir, "redeemer.sqlite")
			config["KeystoreFile"] = opts.account.keystore
			config["KeystorePasswordFile"] = opts.account.passwordFile
			config["PaymentRPCURLFile"] = secretFile(t, dir, "rpc", opts.chain.url)
			config["PaymentChainID"] = opts.chain.chainID.Uint64()
			config["PaymentController"] = opts.chain.controller.Hex()
			config["WeiPerUSD"] = envDefault("E2E_WEI_PER_USD", "1000000000000000")
			if opts.useOracle {
				delete(config, "WeiPerUSD")
				config["ETHUSDFeed"] = opts.chain.feed.Hex()
				config["PriceMaxAge"] = envDefault("E2E_ORACLE_MAX_AGE", "2h")
			}
			config["TicketFaceValue"] = envDefault("E2E_TICKET_FACE_VALUE", "400000000000")
			config["TicketWinProb"] = envDefault("E2E_TICKET_WIN_PROB", "57896044618658097711785492504343953926634992332820282019728792003956564819967")
		}
		for key, value := range opts.extra {
			config[key] = value
		}
		args = []string{"orchestrator", "--config", writeTOML(t, filepath.Join(dir, "orchestrator.toml"), config)}
	} else {
		dir = stateDir
		args = []string{"-orchestrator", "-useLiveRunners", "-orchSecret", liveRunnerSecret, "-serviceAddr", base, "-httpAddr", base, "-cliAddr", cliAddr, "-rtmpAddr", freeAddr(t), "-datadir", filepath.Join(opts.dir, "data"), "-startupAvailabilityCheck=false"}
		if opts.account == nil {
			args = append(args, "-network", "offchain")
		} else {
			args = append(args, chainFlags(opts.chain, opts.account)...)
			args = append(args, "-pricePerUnit", envDefault("E2E_GO_BASE_PRICE_WEI", "1"), "-pixelsPerUnit", "1", "-priceFeedAddr", opts.chain.feed.Hex(), "-ticketEV", envDefault("E2E_TICKET_EV", "200000000000"))
		}
		if template != "" {
			args = append(args, "-liveRunnerProxyUrl", template)
		}
		if opts.staticConfigPath != "" {
			args = append(args, "-liveRunnerConfig", opts.staticConfigPath)
		}
	}
	if backend == "go-livepeer" {
		args = append(args, opts.goExtra...)
	}
	orch.dir = stateDir
	orch.opts = opts
	orch.process = startChild(t, "orchestrator-"+backend, binary(t, backend), dir, args)
	orch.logPath = orch.process.logPath
	if opts.noReady {
		return orch
	}
	if backend == "go-livepeer" {
		waitForHTTP(t, "GET", orch.cliBase+"/status", 200, orch.logPath)
	}
	waitForHTTP(t, "GET", orch.httpBase+"/discovery", 200, orch.logPath)
	return orch
}

func waitDiscovered(t *testing.T, orch *testOrchestrator, metadata string) {
	t.Helper()
	eventually(t, func() bool {
		for _, e := range discover(t, orch) {
			for _, r := range e.Runners {
				if r.Metadata == metadata {
					return true
				}
			}
		}
		return false
	}, envDuration(t, "E2E_READY_TIMEOUT", "20s"), 50*time.Millisecond)
}

// Reserve fixture destinations before Node starts, so its destination policy
// can grant exact ports even when a runner starts later in a scenario.
type portPool struct {
	mu        sync.Mutex
	listeners []net.Listener
	addresses []string
	next      int
}

var poolsMu sync.Mutex
var pools = map[string]*portPool{}

func fixturePoolFor(t *testing.T) *portPool {
	t.Helper()
	poolsMu.Lock()
	defer poolsMu.Unlock()
	var chosen *portPool
	longest := 0
	for name, pool := range pools {
		if (t.Name() == name || strings.HasPrefix(t.Name(), name+"/")) && len(name) > longest {
			chosen = pool
			longest = len(name)
		}
	}
	if chosen != nil {
		return chosen
	}
	p := &portPool{}
	for range 256 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		p.listeners = append(p.listeners, ln)
		p.addresses = append(p.addresses, ln.Addr().String())
	}
	pools[t.Name()] = p
	t.Cleanup(func() {
		poolsMu.Lock()
		delete(pools, t.Name())
		poolsMu.Unlock()
		for _, ln := range p.listeners {
			_ = ln.Close()
		}
	})
	return p
}
func (p *portPool) take(t *testing.T) string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.Less(t, p.next, len(p.addresses), "fixture port pool exhausted")
	i := p.next
	p.next++
	require.NoError(t, p.listeners[i].Close())
	return p.addresses[i]
}
func fixtureGrants(t *testing.T) []string { return fixturePoolFor(t).addresses }
func newFixtureServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	_ = srv.Listener.Close()
	ln, err := net.Listen("tcp", freeAddr(t))
	require.NoError(t, err)
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

// Poll synchronously so assertions in fixture callbacks remain in the test
// goroutine and a failure cannot strand a background assertion goroutine.
func eventually(t *testing.T, condition func() bool, timeout, interval time.Duration, message ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if !time.Now().Before(deadline) {
			require.FailNow(t, "condition not satisfied before deadline", message...)
			return
		}
		time.Sleep(interval)
	}
}

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.buffer.String() }

func fixturePort(t *testing.T) string {
	t.Helper()
	_, port, err := net.SplitHostPort(freeAddr(t))
	require.NoError(t, err)
	return port
}
