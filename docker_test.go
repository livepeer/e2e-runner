package e2escope

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func composeMode() bool { return os.Getenv("E2E_MODE") == "compose" }

var composeCleanupFailed atomic.Bool

type tunnelSlot struct {
	url, targetFile string
	inUse           bool
	stop            func()
}

var tunnelPool struct {
	sync.Mutex
	slots   []*tunnelSlot
	control *tunnelSlot
	routes  map[string]string
}

func setGatewayTarget(path, origin string) error {
	temp := path + ".next"
	if err := os.WriteFile(temp, []byte(origin), 0600); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

func pooledTunnel(origin string) (*tunnelSlot, error) {
	tunnelPool.Lock()
	defer tunnelPool.Unlock()
	for _, slot := range tunnelPool.slots {
		if !slot.inUse {
			if err := setGatewayTarget(slot.targetFile, origin); err != nil {
				return nil, err
			}
			slot.inUse = true
			return slot, nil
		}
	}
	return createTunnelSlot(origin)
}

// Caller holds tunnelPool's lock. Gateways live until TestMain exits; leases
// live only for the fixture, so later fixtures reuse the same public hostname.
func createTunnelSlot(origin string) (*tunnelSlot, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	id := uuid.NewString()
	file := filepath.Join(os.Getenv("TMPDIR"), "gateway-"+id)
	if err := setGatewayTarget(file, origin); err != nil {
		return nil, err
	}
	log, err := os.Create(filepath.Join(os.Getenv("E2E_ARTIFACT_DIR"), "gateway-"+id+".log"))
	if err != nil {
		return nil, err
	}
	cmd, container := componentCommand(context.Background(), "gateway", filepath.Join(os.Getenv("E2E_RUN_ROOT"), "bin/e2e-bridge"), "", []string{"-mode", "gateway", "-listen", addr, "-target-file", file})
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); log.Close() }()
	stopGateway := func() {
		removeContainer(container)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
		}
	}
	endpoint, stopTunnel, err := launchTunnel("http://"+addr, filepath.Join(os.Getenv("E2E_ARTIFACT_DIR"), "tunnel-"+id+".log"))
	if err != nil {
		stopGateway()
		return nil, err
	}
	slot := &tunnelSlot{url: endpoint, targetFile: file, inUse: true, stop: func() { stopTunnel(); stopGateway() }}
	tunnelPool.slots = append(tunnelPool.slots, slot)
	return slot, nil
}

func publicControlURL(t *testing.T, origin string) string {
	t.Helper()
	if !composeMode() {
		return origin
	}
	tunnelPool.Lock()
	if tunnelPool.control == nil {
		slot, err := createTunnelSlot("")
		if err != nil {
			tunnelPool.Unlock()
			require.NoError(t, err)
			return ""
		}
		tunnelPool.control, tunnelPool.routes = slot, map[string]string{}
	}
	id := uuid.NewString()
	tunnelPool.routes[id] = origin
	data, err := json.Marshal(tunnelPool.routes)
	if err == nil {
		err = setGatewayTarget(tunnelPool.control.targetFile, string(data))
	}
	endpoint := tunnelPool.control.url + "/" + id
	tunnelPool.Unlock()
	require.NoError(t, err)
	t.Cleanup(func() {
		tunnelPool.Lock()
		defer tunnelPool.Unlock()
		delete(tunnelPool.routes, id)
		data, err := json.Marshal(tunnelPool.routes)
		if err == nil {
			err = setGatewayTarget(tunnelPool.control.targetFile, string(data))
		}
		if err != nil {
			t.Errorf("clear TLS control route: %v", err)
		}
	})
	recordEndpoint(t, origin, endpoint)
	return endpoint
}

func stopPooledTunnels() {
	tunnelPool.Lock()
	defer tunnelPool.Unlock()
	for _, slot := range tunnelPool.slots {
		slot.stop()
	}
	tunnelPool.slots = nil
	tunnelPool.control, tunnelPool.routes = nil, nil
}

func composeArgs(service, name, dir string, command []string, env ...string) []string {
	args := []string{"compose", "-f", os.Getenv("E2E_COMPOSE_FILE"), "run", "--rm", "-T", "--name", name}
	if dir != "" {
		args = append(args, "--workdir", dir)
	}
	for _, value := range env {
		args = append(args, "-e", value)
	}
	return append(append(args, service), command...)
}

func componentService(name, bin string, args []string) string {
	switch {
	case name == "tunnel":
		return "tunnel"
	case strings.Contains(name, "runner-fixture"):
		return "runner"
	case strings.Contains(bin, "e2e-kafka-proxy"):
		return "kafka-proxy"
	case strings.Contains(bin, "e2e-bridge"):
		return "bridge"
	case strings.Contains(bin, "e2e-runner") || bin == "uv":
		return "runner"
	case strings.Contains(name, "clearinghouse"):
		return "clearinghouse"
	case strings.Contains(name, "orchestrator-go-livepeer"):
		return "go-livepeer-orchestrator"
	case strings.Contains(name, "signer-go-livepeer"):
		return "go-livepeer-signer"
	case strings.Contains(name, "orchestrator-node"):
		return "node-orchestrator"
	case strings.Contains(name, "signer-node") || (len(args) > 0 && args[0] == "signer"):
		return "node-signer"
	case strings.Contains(bin, "/node/"):
		return "node-chain"
	default:
		return "go-livepeer-control"
	}
}

func componentCommand(ctx context.Context, name, bin, dir string, args []string, env ...string) (*exec.Cmd, string) {
	if !composeMode() {
		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Dir, cmd.Env = dir, append(os.Environ(), env...)
		return cmd, ""
	}
	id := os.Getenv("COMPOSE_PROJECT_NAME") + "-" + uuid.NewString()
	cmd := exec.CommandContext(ctx, "docker", composeArgs(componentService(name, bin, args), id, dir, append([]string{bin}, args...), env...)...)
	cmd.Env = os.Environ()
	return cmd, id
}

// Direct subprocess clients need explicit cleanup when CommandContext kills
// the Docker CLI. The container may otherwise continue running.
func fixtureCommand(t *testing.T, ctx context.Context, name, bin, dir string, args ...string) *exec.Cmd {
	t.Helper()
	cmd, id := componentCommand(ctx, name, bin, dir, args)
	if id != "" {
		t.Cleanup(func() { removeContainer(id) })
	}
	return cmd
}

func removeContainer(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var output []byte
	var err error
	for {
		output, err = exec.CommandContext(ctx, "docker", "rm", "-f", id).CombinedOutput()
		// Compose --rm can already be deleting a fixture when cleanup runs.
		// Wait for removal to finish rather than reporting that race as a leak.
		if err == nil || !strings.Contains(string(output), "removal") || !strings.Contains(string(output), "already in progress") || ctx.Err() != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil && !strings.Contains(string(output), "No such container") {
		composeCleanupFailed.Store(true)
		message := fmt.Sprintf("INFRASTRUCTURE: remove container %s: %v: %s\n", id, err, output)
		fmt.Fprint(os.Stderr, message)
		if f, e := os.OpenFile(filepath.Join(os.Getenv("E2E_ARTIFACT_DIR"), "cleanup-errors.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600); e == nil {
			_, _ = f.WriteString(message)
			_ = f.Close()
		}
	}
}

var quickURL = regexp.MustCompile(`https://[a-z0-9-]+\.trycloudflare\.com`)

type quickTunnelRateLimitError struct {
	err       error
	notBefore time.Time
}

func (e *quickTunnelRateLimitError) Error() string {
	return fmt.Sprintf("Quick Tunnel provisioning returned HTTP 429: %v", e.err)
}
func (e *quickTunnelRateLimitError) Unwrap() error { return e.err }

func retryAfterTime(value string, now time.Time) time.Time {
	value = strings.TrimSpace(value)
	if value != "" && strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
		seconds, err := strconv.ParseUint(value, 10, 64)
		const maxDelay time.Duration = 1<<63 - 1
		// Saturate large values rather than overflowing into an early retry.
		if err != nil || seconds > uint64(maxDelay/time.Second) {
			return now.Add(maxDelay)
		}
		return now.Add(time.Duration(seconds) * time.Second)
	}
	deadline, _ := http.ParseTime(value)
	return deadline
}

func quickTunnelRetryDelay(err error, attempt int, now time.Time) (time.Duration, bool) {
	var limited *quickTunnelRateLimitError
	if !errors.As(err, &limited) {
		return 0, false
	}
	backoff := min(30*time.Second*time.Duration(1<<attempt), 2*time.Minute)
	return max(backoff, limited.notBefore.Sub(now)), true
}

// cloudflared does not expose provisioning response headers in its errors.
// Relay only that request over loopback, preserving HTTPS to the upstream API.
func tunnelProvisioningProxy(target *url.URL, transport http.RoundTripper) (*httptest.Server, <-chan time.Time) {
	limited := make(chan time.Time, 1)
	proxy := &httputil.ReverseProxy{
		Rewrite:   func(r *httputil.ProxyRequest) { r.SetURL(target) },
		Transport: transport,
		ModifyResponse: func(r *http.Response) error {
			if r.StatusCode == http.StatusTooManyRequests {
				select {
				case limited <- retryAfterTime(r.Header.Get("Retry-After"), time.Now()):
				default:
				}
			}
			return nil
		},
	}
	mux := http.NewServeMux()
	mux.Handle("POST /tunnel", proxy)
	return httptest.NewServer(mux), limited
}

func launchTunnel(origin, logPath string) (string, func(), error) {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		endpoint, stop, failure := launchTunnelOnce(origin, fmt.Sprintf("%s.attempt-%d", logPath, attempt+1))
		if failure == nil {
			return endpoint, stop, nil
		}
		err = failure
		delay, retry := quickTunnelRetryDelay(err, attempt, time.Now())
		if !retry || attempt == 4 {
			return "", nil, err
		}
		fmt.Fprintf(os.Stderr, "Quick Tunnel provisioning rate limited; retrying in %s (attempt %d/5)\n", delay, attempt+2)
		time.Sleep(delay)
	}
	return "", nil, err
}

func launchTunnelOnce(origin, logPath string) (string, func(), error) {
	service, limited := tunnelProvisioningProxy(&url.URL{Scheme: "https", Host: "api.trycloudflare.com"}, http.DefaultTransport)
	defer service.Close()
	failure := func(err error) (string, func(), error) {
		select {
		case notBefore := <-limited:
			err = &quickTunnelRateLimitError{err: err, notBefore: notBefore}
		default:
		}
		return "", nil, err
	}
	id := os.Getenv("COMPOSE_PROJECT_NAME") + "-tunnel-" + uuid.NewString()
	log, err := os.Create(logPath)
	if err != nil {
		return "", nil, err
	}
	cmd := exec.Command("docker", composeArgs("tunnel", id, "", []string{"tunnel", "--no-autoupdate", "--protocol", "http2", "--quick-service", service.URL, "--url", origin})...)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		return "", nil, err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait(); log.Close() }()
	stop := func() {
		removeContainer(id)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
		}
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(logPath)
		if endpoint := quickURL.FindString(string(data)); endpoint != "" && strings.Contains(string(data), "Registered tunnel connection") {
			// Verify DNS and the public certificate without issuing a fixture
			// request, which could alter counters or consume a one-shot response.
			u, _ := url.Parse(endpoint)
			conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", u.Host+":443", &tls.Config{MinVersion: tls.VersionTLS12, ServerName: u.Hostname()})
			if err == nil {
				conn.Close()
				return endpoint, stop, nil
			}
		}
		select {
		case err := <-done:
			removeContainer(id)
			data, _ = os.ReadFile(logPath)
			return failure(fmt.Errorf("tunnel exited: %v: %s", err, data))
		default:
		}
		time.Sleep(500 * time.Millisecond)
	}
	stop()
	return failure(fmt.Errorf("Quick Tunnel readiness timed out: %s", logPath))
}

func publicURL(t *testing.T, origin string) string {
	t.Helper()
	if !composeMode() {
		return origin
	}
	slot, err := pooledTunnel(origin)
	require.NoError(t, err, "INFRASTRUCTURE: TLS tunnel for %s", origin)
	endpoint := slot.url
	t.Cleanup(func() {
		tunnelPool.Lock()
		defer tunnelPool.Unlock()
		if err := setGatewayTarget(slot.targetFile, ""); err != nil {
			t.Errorf("clear TLS gateway: %v", err)
		}
		slot.inUse = false
	})
	recordEndpoint(t, origin, endpoint)
	return endpoint
}

func recordEndpoint(t *testing.T, origin, endpoint string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(os.Getenv("E2E_ARTIFACT_DIR"), "endpoints.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(f).Encode(map[string]string{"test": t.Name(), "origin": origin, "url": endpoint}))
	require.NoError(t, f.Close())
}

func initializeCompose() (func(), error) {
	if !composeMode() {
		return func() {}, nil
	}
	origin := "http://127.0.0.1:" + os.Getenv("E2E_GETH_PORT")
	endpoint, stop, err := launchTunnel(origin, filepath.Join(os.Getenv("E2E_ARTIFACT_DIR"), "geth-tunnel.log"))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (func(), error) { stop(); return nil, err }
	rpcCall := func(method string, params any, out any) error {
		status, data, err := requestJSON("POST", endpoint, map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, nil)
		if err != nil {
			return err
		}
		if status != 200 {
			return fmt.Errorf("geth HTTP %d: %s", status, data)
		}
		var response struct {
			Result json.RawMessage
			Error  any
		}
		if err := json.Unmarshal(data, &response); err != nil {
			return err
		}
		if response.Error != nil {
			return fmt.Errorf("geth %s: %v", method, response.Error)
		}
		return json.Unmarshal(response.Result, out)
	}
	var chainID string
	deadline := time.Now().Add(time.Minute)
	for {
		err = rpcCall("eth_chainId", []any{}, &chainID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fail(err)
		}
		time.Sleep(time.Second)
	}
	id, ok := new(big.Int).SetString(strings.TrimPrefix(chainID, "0x"), 16)
	if !ok {
		return fail(fmt.Errorf("invalid chain ID %s", chainID))
	}
	var logs []struct{ Address string }
	err = rpcCall("eth_getLogs", []any{map[string]any{"fromBlock": "0x0", "toBlock": "latest", "topics": []string{"0xf9a6cf519167d81bc5cb3d26c60c0c9a19704aa908c148e82a861b570f4cd2d7"}}}, &logs)
	if err != nil {
		return fail(err)
	}
	if len(logs) == 0 {
		return fail(fmt.Errorf("go-livepeer devnet has no Controller deployment logs"))
	}
	for key, value := range map[string]string{"E2E_RPC_URL": endpoint, "E2E_ETH_CONTROLLER": logs[0].Address, "E2E_EXPECTED_CHAIN_ID": id.String()} {
		if err := os.Setenv(key, value); err != nil {
			return fail(err)
		}
	}
	data, _ := json.MarshalIndent(map[string]string{"rpc": endpoint, "controller": logs[0].Address, "chain_id": id.String()}, "", "  ")
	if err := os.WriteFile(filepath.Join(os.Getenv("E2E_ARTIFACT_DIR"), "devnet.json"), data, 0600); err != nil {
		return fail(err)
	}
	fmt.Printf("Fresh go-livepeer devnet: chain=%s controller=%s RPC=%s\n", id, logs[0].Address, endpoint)
	return stop, nil
}
