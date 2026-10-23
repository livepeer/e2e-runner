package e2escope

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	liverunner "github.com/livepeer/golang-runner"
	"github.com/stretchr/testify/require"
)

type runnerSDK interface {
	RunnerID() string
	OrchestratorURL() string
	HeartbeatInterval() time.Duration
	Close(context.Context) error
	CreateTrickleChannels(context.Context, *http.Request, []liverunner.ChannelRequest) ([]liverunner.Channel, error)
	DeleteTrickleChannels(context.Context, *http.Request, []string) ([]string, error)
	CreateProxy(context.Context, *http.Request, string) (liverunner.Proxy, error)
}

type containerRunnerSDK struct {
	base, token, id, orch string
	interval              time.Duration
	closed                bool
}

func (r *containerRunnerSDK) RunnerID() string                 { return r.id }
func (r *containerRunnerSDK) OrchestratorURL() string          { return r.orch }
func (r *containerRunnerSDK) HeartbeatInterval() time.Duration { return r.interval }
func (r *containerRunnerSDK) call(ctx context.Context, kind string, payload map[string]any, out any) error {
	payload["Kind"] = kind
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", r.base+"/control", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := testHTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("container Go SDK %s: HTTP %d", kind, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
func (r *containerRunnerSDK) Close(ctx context.Context) error {
	if r.closed {
		return nil
	}
	err := r.call(ctx, "close", map[string]any{}, nil)
	if err == nil {
		r.closed = true
	}
	return err
}
func (r *containerRunnerSDK) CreateTrickleChannels(ctx context.Context, req *http.Request, channels []liverunner.ChannelRequest) (out []liverunner.Channel, err error) {
	err = r.call(ctx, "channels", map[string]any{"Headers": req.Header, "Channels": channels}, &out)
	return
}
func (r *containerRunnerSDK) DeleteTrickleChannels(ctx context.Context, req *http.Request, names []string) (out []string, err error) {
	err = r.call(ctx, "delete-channels", map[string]any{"Headers": req.Header, "Names": names}, &out)
	return
}
func (r *containerRunnerSDK) CreateProxy(ctx context.Context, req *http.Request, target string) (out liverunner.Proxy, err error) {
	err = r.call(ctx, "proxy", map[string]any{"Headers": req.Header, "Target": target}, &out)
	return
}

func startContainerGoRunner(t *testing.T, ctx context.Context, orch *testOrchestrator, cfg runnerConfig) *goRunnerRegistration {
	t.Helper()
	addr, token := freeAddr(t), uuid.NewString()
	base := publicURL(t, "http://"+addr)
	price, err := liverunner.NewPriceInfo(string(runnerPrice(cfg).Price), "USD", runnerPrice(cfg).Unit)
	require.NoError(t, err)
	// Options also contains callbacks and an HTTP client. Serialize only the
	// wire-safe registration fields; callbacks remain inside the SDK container.
	options := map[string]any{"OrchestratorURL": orch.httpBase, "BootstrapSecret": liveRunnerSecret, "RunnerURL": cfg.runnerURL, "App": liveRunnerApp, "Metadata": cfg.metadata, "Mode": cfg.mode, "Proxy": cfg.proxy, "Capacity": cfg.capacity, "PriceInfo": price}
	data, err := json.Marshal(map[string]any{"Listen": addr, "Token": token, "Options": options})
	require.NoError(t, err)
	dir := t.TempDir()
	path := secretFile(t, dir, "runner.json", string(data))
	process := startChild(t, "golang-runner", filepath.Join(os.Getenv("E2E_RUN_ROOT"), "bin/e2e-runner"), dir, []string{path})
	remote := &containerRunnerSDK{base: base, token: token}
	eventually(t, func() bool {
		status, data, err := requestJSON("GET", base+"/meta", nil, map[string]string{"Authorization": "Bearer " + token})
		if err != nil || status != 200 {
			return false
		}
		var meta struct {
			ID, URL  string
			Interval time.Duration
		}
		require.NoError(t, json.Unmarshal(data, &meta))
		remote.id, remote.orch, remote.interval = meta.ID, meta.URL, meta.Interval
		return true
	}, envDuration(t, "E2E_READY_TIMEOUT", "30s"), 100*time.Millisecond, readLog(t, process.logPath))
	require.NotEmpty(t, remote.id)
	require.Equal(t, orch.httpBase, remote.orch)
	require.NotZero(t, remote.interval)
	runner := &goRunnerRegistration{ctx: ctx, reg: remote}
	t.Cleanup(func() { runner.Close(t) })
	return runner
}

func tunneledKafka(t *testing.T, broker string) string {
	t.Helper()
	server, client := freeAddr(t), freeAddr(t)
	bin := filepath.Join(os.Getenv("E2E_RUN_ROOT"), "bin/e2e-bridge")
	startChild(t, "kafka-bridge-server", bin, "", []string{"-mode", "server", "-listen", server, "-target", broker})
	url := publicURL(t, "http://"+server)
	startChild(t, "kafka-bridge-client", bin, "", []string{"-mode", "client", "-listen", client, "-target", "wss" + url[5:]})
	return client
}
