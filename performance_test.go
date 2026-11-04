package e2escope

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

// This proxy delays an exchange once, including when it contains a JSON-RPC
// batch. Its records count all worker traffic, including idle/background work.
// A request window does not imply that a call came from the foreground.
type performanceRPC struct {
	delay     atomic.Int64
	mu        sync.Mutex
	window    string
	role      string
	encoder   *json.Encoder
	err       error
	selectors map[string]string
}

func (p *performanceRPC) handler(target *url.URL) http.Handler {
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{MaxIdleConns: 128, MaxIdleConnsPerHost: 128}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid RPC body", 400)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var calls []struct {
			Method string
			Params []json.RawMessage
		}
		batch := len(bytes.TrimSpace(body)) > 0 && bytes.TrimSpace(body)[0] == '['
		if batch {
			err = json.Unmarshal(body, &calls)
		} else {
			calls = make([]struct {
				Method string
				Params []json.RawMessage
			}, 1)
			err = json.Unmarshal(body, &calls[0])
		}
		if err != nil {
			http.Error(w, "invalid RPC JSON", 400)
			return
		}
		methods, contracts := map[string]int{}, map[string]int{}
		for _, call := range calls {
			methods[call.Method]++
			if call.Method == "eth_call" && len(call.Params) > 0 {
				var input struct{ Input, Data string }
				_ = json.Unmarshal(call.Params[0], &input)
				data := input.Input
				if data == "" {
					data = input.Data
				}
				name := "unknown"
				if len(data) >= 10 {
					if v := p.selectors[strings.ToLower(data[:10])]; v != "" {
						name = v
					}
				}
				contracts[name]++
			}
		}
		p.mu.Lock()
		window := p.window
		p.mu.Unlock()
		delay := time.Duration(p.delay.Load())
		cancelled := false
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-r.Context().Done():
				cancelled = true
			}
			timer.Stop()
		}
		if !cancelled {
			proxy.ServeHTTP(w, r)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		if err := p.encoder.Encode(map[string]any{"at": start.UTC(), "role": p.role, "window": window, "batch": batch,
			"methods": methods, "contracts": contracts, "delay_ms": float64(delay) / float64(time.Millisecond),
			"duration_ms": float64(time.Since(start)) / float64(time.Millisecond), "cancelled": cancelled}); err != nil {
			p.err = err
		}
	})
}

func (p *performanceRPC) setWindow(window string) { p.mu.Lock(); p.window = window; p.mu.Unlock() }

func performanceFile(t *testing.T, name string) *os.File {
	t.Helper()
	dir := envDefault("E2E_ARTIFACT_DIR", "artifacts")
	require.NoError(t, os.MkdirAll(dir, 0755))
	f, err := os.Create(filepath.Join(dir, name))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return f
}

type performanceSample struct {
	ID          string  `json:"id"`
	Window      string  `json:"window"`
	ChallengeMS float64 `json:"challenge_ms"`
	SignerMS    float64 `json:"signer_ms"`
	ReceiveMS   float64 `json:"receive_ms"`
	DispatchMS  float64 `json:"dispatch_ms"`
	FirstByteMS float64 `json:"first_byte_ms"`
	TotalMS     float64 `json:"total_ms"`
	Status      int     `json:"status"`
	Error       string  `json:"error,omitempty"`
	Tickets     int     `json:"tickets"`
	FaceValue   string  `json:"face_value_wei,omitempty"`
	WinProb     string  `json:"win_probability,omitempty"`
}

func performanceRequest(ctx context.Context, client *http.Client, signer, orch, route, payer, id string) performanceSample {
	s := performanceSample{ID: id}
	start := time.Now()
	payload := map[string]any{"prompt": "hello", "perf_id": id}
	target := orch + "/apps/" + route + "/app"
	post := func(target string, payload any, headers map[string]string, trace *httptrace.ClientTrace) ([]byte, error) {
		body, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, "POST", target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		if trace != nil {
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		s.Status = resp.StatusCode
		return io.ReadAll(resp.Body)
	}
	fail := func(err error) performanceSample {
		s.Error = err.Error()
		s.TotalMS = float64(time.Since(start)) / 1e6
		return s
	}
	body, err := post(target, payload, map[string]string{"Livepeer-Payer-Address": payer}, nil)
	s.ChallengeMS = float64(time.Since(start)) / 1e6
	if err != nil {
		return fail(err)
	}
	if s.Status != 402 {
		return fail(fmt.Errorf("challenge HTTP %d: %.240s", s.Status, body))
	}
	var challenge paymentChallenge
	if err := json.Unmarshal(body, &challenge); err != nil {
		return fail(err)
	}
	if challenge.PaymentParams == "" {
		return fail(fmt.Errorf("missing challenge fields"))
	}
	info, err := base64.StdEncoding.DecodeString(challenge.PaymentParams)
	if err != nil {
		return fail(err)
	}
	fields, _, err := performanceWire(info)
	if err != nil || len(fields[2]) != 1 {
		return fail(fmt.Errorf("invalid challenge protobuf"))
	}
	if challenge.ManifestID == "" && len(fields[6]) == 1 {
		auth, _, err := performanceWire(fields[6][0])
		if err == nil && len(auth[2]) == 1 {
			challenge.ManifestID = string(auth[2][0])
		}
	}
	if challenge.ManifestID == "" {
		return fail(fmt.Errorf("missing challenge manifest"))
	}
	params, _, err := performanceWire(fields[2][0])
	if err != nil || len(params[2]) != 1 || len(params[3]) != 1 {
		return fail(fmt.Errorf("invalid ticket parameters"))
	}
	s.FaceValue = new(big.Int).SetBytes(params[2][0]).String()
	s.WinProb = new(big.Int).SetBytes(params[3][0]).String()
	phase := time.Now()
	body, err = post(signer+"/generate-live-payment", map[string]any{"orchestrator": challenge.PaymentParams,
		"ManifestID": challenge.ManifestID, "app": liveRunnerApp, "type": "fixed"}, nil, nil)
	s.SignerMS = float64(time.Since(phase)) / 1e6
	if err != nil {
		return fail(err)
	}
	if s.Status != 200 {
		return fail(fmt.Errorf("signer HTTP %d: %.240s", s.Status, body))
	}
	var payment signedPayment
	if err := json.Unmarshal(body, &payment); err != nil {
		return fail(err)
	}
	if payment.Payment == "" || payment.Segment == "" {
		return fail(fmt.Errorf("missing payment credentials"))
	}
	encoded, err := base64.StdEncoding.DecodeString(payment.Payment)
	if err != nil {
		return fail(err)
	}
	_, counts, err := performanceWire(encoded)
	if err != nil {
		return fail(err)
	}
	s.Tickets = counts[4]
	phase = time.Now()
	var firstByte time.Time
	body, err = post(target, payload, map[string]string{"Livepeer-Payer-Address": payer, "Livepeer-Payment": payment.Payment,
		"Livepeer-Segment": payment.Segment}, &httptrace.ClientTrace{GotFirstResponseByte: func() { firstByte = time.Now() }})
	s.ReceiveMS = float64(time.Since(phase)) / 1e6
	if !firstByte.IsZero() {
		s.FirstByteMS = float64(firstByte.Sub(phase)) / 1e6
	}
	if err != nil {
		return fail(err)
	}
	if s.Status != 200 {
		return fail(fmt.Errorf("receive HTTP %d: %.240s", s.Status, body))
	}
	var result struct {
		ID   string `json:"perf_id"`
		Paid bool   `json:"paid"`
		At   int64  `json:"runner_at_ns"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fail(err)
	}
	if result.ID != id || !result.Paid || result.At < start.UnixNano() {
		return fail(fmt.Errorf("runner response identity mismatch"))
	}
	s.DispatchMS = float64(result.At-start.UnixNano()) / 1e6
	s.TotalMS = float64(time.Since(start)) / 1e6
	return s
}

func performanceWire(data []byte) (map[protowire.Number][][]byte, map[protowire.Number]int, error) {
	fields, counts := map[protowire.Number][][]byte{}, map[protowire.Number]int{}
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, nil, protowire.ParseError(n)
		}
		data = data[n:]
		counts[number]++
		if kind == protowire.BytesType {
			value, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			fields[number] = append(fields[number], value)
			data = data[n:]
		} else {
			n := protowire.ConsumeFieldValue(number, kind, data)
			if n < 0 {
				return nil, nil, protowire.ParseError(n)
			}
			data = data[n:]
		}
	}
	return fields, counts, nil
}

func TestPerformanceChainPrerequisites(t *testing.T) {
	if os.Getenv("E2E_PERF_TARGET") == "" {
		t.Skip("opt-in payment comparison")
	}
	c := preflightChain(t)
	c.requireCanonicalHeaders(t)
	c.requireFinality(t)
	c.checkControl(t)
	var snapshot string
	c.request(t, &snapshot, "evm_snapshot")
	var ignored any
	c.request(t, &ignored, "evm_mine")
	var head struct{ Hash common.Hash }
	c.request(t, &head, "eth_getBlockByNumber", "latest", false)
	var restored bool
	c.request(t, &restored, "evm_revert", snapshot)
	require.True(t, restored)
	data, err := c.contracts["controller"].Pack("getContract", common.Hash{})
	require.NoError(t, err)
	ctx, cancel := chainContext(t)
	defer cancel()
	var out hexutil.Bytes
	err = c.rpc.CallContext(ctx, &out, "eth_call", map[string]any{"to": c.controller, "data": hexutil.Bytes(data)}, map[string]any{"blockHash": head.Hash, "requireCanonical": true})
	require.Error(t, err, "orphaned hash-pinned reads must fail")
	var nonce hexutil.Uint64
	c.request(t, &head, "eth_getBlockByNumber", "finalized", false)
	c.request(t, &nonce, "eth_getTransactionCount", common.Address{}, map[string]any{"blockHash": head.Hash, "requireCanonical": true})
}

func TestPaymentPerformance(t *testing.T) {
	if os.Getenv("E2E_PERF_TARGET") == "" {
		t.Skip("opt-in payment comparison")
	}
	backend := envDefault("E2E_PERF_BACKEND", "node")
	workers, err := strconv.Atoi(envDefault("E2E_PERF_WORKERS", "1"))
	require.NoError(t, err)
	require.Contains(t, []int{1, 32}, workers)
	n, err := strconv.Atoi(envDefault("E2E_PERF_SAMPLES", "128"))
	require.NoError(t, err)
	require.Positive(t, n)
	delay, err := time.ParseDuration(envDefault("E2E_PERF_DELAY", "0ms"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, delay, time.Duration(0))
	c := preflightChain(t)
	var sharedRecipient *chainAccount
	if dir := os.Getenv("E2E_PERF_RECIPIENT_DIR"); dir != "" {
		sharedRecipient = performanceRecipient(t, c, dir)
	}
	var proxies []*performanceRPC
	workerStarts := map[string]time.Time{}
	opts := paidSetupOptions{recipient: sharedRecipient, workerChain: func(role string, chain *chainFixture) *chainFixture {
		p := &performanceRPC{role: role, window: "startup", encoder: json.NewEncoder(performanceFile(t, "rpc-"+role+".jsonl")), selectors: map[string]string{}}
		for contract, a := range chain.contracts {
			for name, method := range a.Methods {
				p.selectors[hexutil.Encode(method.ID)] = contract + "." + name
			}
		}
		target, err := url.Parse(chain.url)
		require.NoError(t, err)
		server := newFixtureServer(t, p.handler(target))
		clone := *chain
		clone.url = server.URL
		proxies = append(proxies, p)
		workerStarts[role] = time.Now()
		return &clone
	}}
	webhook := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(t, w, map[string]any{"status": 200, "auth_id": "performance", "expiry": 0})
	}))
	f := setupPaid(t, c, backend, backend, sameWallet, webhook.URL, "", nil, opts)
	startup := map[string]float64{"orchestrator_http_ready_seconds": workerStarts["signer"].Sub(workerStarts["orchestrator"]).Seconds(), "signer_http_ready_seconds": time.Since(workerStarts["signer"]).Seconds()}
	upstream := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			return
		}
		at := time.Now().UnixNano()
		var payload struct {
			ID string `json:"perf_id"`
		}
		if json.NewDecoder(r.Body).Decode(&payload) != nil || payload.ID == "" {
			http.Error(w, "missing identity", 400)
			return
		}
		respondJSON(t, w, map[string]any{"paid": true, "perf_id": payload.ID, "runner_at_ns": at})
	}))
	price := priceInfo{Price: "0.00000001", Currency: "usd", Unit: "fixed"}
	reg := registerRunnerHTTP(t, f.orch, heartbeatPayload{RunnerURL: upstream.URL, Mode: "single-shot", App: liveRunnerApp, Metadata: "performance", Capacity: 4096, PriceInfo: price}, liveRunnerSecret)
	stop := maintainRunner(t, f.orch, reg, heartbeatPayload{RunnerID: reg.RunnerID, RunnerURL: upstream.URL, Mode: "single-shot", App: liveRunnerApp, Status: "ready", Capacity: 4096, PriceInfo: price})
	t.Cleanup(stop)
	transport := &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 128}
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	setWindow := func(name string) {
		for _, p := range proxies {
			p.setWindow(name)
		}
	}
	for _, p := range proxies {
		p.delay.Store(int64(delay))
	}
	encoder := json.NewEncoder(performanceFile(t, "requests.jsonl"))
	request := func(id string) performanceSample {
		return performanceRequest(t.Context(), client, f.signer.base, f.orch.httpBase, reg.RunnerID, f.payer.address.Hex(), id)
	}
	setWindow("idle-before-first-job")
	idle, err := time.ParseDuration(envDefault("E2E_PERF_IDLE", "0s"))
	require.NoError(t, err)
	if idle > 0 {
		timer := time.NewTimer(idle)
		select {
		case <-timer.C:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	setWindow("cold")
	cold := request("cold")
	cold.Window = "cold"
	require.NoError(t, encoder.Encode(cold))
	// Cold failures remain in the artifact; readiness retries are never measured
	// as successful warm requests or silently substituted for the cold sample.
	setWindow("warmup")
	ready := cold.Error == ""
	for attempt := 0; !ready && attempt < 12; attempt++ {
		time.Sleep(time.Second)
		sample := request(fmt.Sprintf("ready-%d", attempt))
		sample.Window = "warmup"
		require.NoError(t, encoder.Encode(sample))
		ready = sample.Error == ""
	}
	require.True(t, ready, "no successful payment flow; inspect requests.jsonl and worker logs")
	for i := range 8 {
		sample := request(fmt.Sprintf("warmup-%d", i))
		sample.Window = "warmup"
		require.NoError(t, encoder.Encode(sample))
	}
	setWindow("warm")
	resources := performanceResources([]int{f.orch.process.cmd.Process.Pid, f.signer.process.cmd.Process.Pid})
	start := time.Now()
	results := make(chan performanceSample, workers)
	var next atomic.Int64
	var running sync.WaitGroup
	for range workers {
		running.Go(func() {
			for {
				index := int(next.Add(1)) - 1
				if index >= n {
					return
				}
				sample := request(fmt.Sprintf("warm-%d", index))
				sample.Window = "warm"
				results <- sample
			}
		})
	}
	go func() { running.Wait(); close(results) }()
	successes := 0
	for sample := range results {
		require.NoError(t, encoder.Encode(sample))
		if sample.Error == "" {
			successes++
		}
	}
	elapsed := time.Since(start)
	usage := resources()
	usage["process_roles"] = []string{"orchestrator", "signer"}
	setWindow("idle-known")
	if idle > 0 {
		timer := time.NewTimer(idle)
		select {
		case <-timer.C:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	metadata := map[string]any{"target": os.Getenv("E2E_PERF_TARGET"), "revision": os.Getenv("E2E_PERF_REVISION"), "backend": backend, "workers": workers, "samples": n,
		"delay_ms": float64(delay) / 1e6, "repetition": os.Getenv("E2E_PERF_REPETITION"), "successes": successes,
		"elapsed_seconds": elapsed.Seconds(), "successful_requests_per_second": float64(successes) / elapsed.Seconds(), "resources": usage, "startup": startup,
		"idle_seconds": idle.Seconds(), "price_usd": price.Price, "ticket_ev_wei": os.Getenv("E2E_TICKET_EV"),
		"node_face_value_wei": os.Getenv("E2E_TICKET_FACE_VALUE"), "node_win_probability": os.Getenv("E2E_TICKET_WIN_PROB")}
	require.NoError(t, json.NewEncoder(performanceFile(t, "cell.json")).Encode(metadata))
	for _, p := range proxies {
		p.mu.Lock()
		err := p.err
		p.mu.Unlock()
		require.NoError(t, err)
	}
	t.Logf("%s delay=%s workers=%d successful=%d/%d elapsed=%s", os.Getenv("E2E_PERF_TARGET"), delay, workers, successes, n, elapsed)
	require.Positive(t, successes, "all measured requests failed")
}

// Reuse one active recipient across serialized performance cells, as the normal
// harness does inside one test process. Every cell still gets fresh payer keys,
// signer/orchestrator state, and worker processes. Private material stays outside
// the retained results directory and belongs to the outer comparison runner.
func performanceRecipient(t *testing.T, c *chainFixture, dir string) *chainAccount {
	t.Helper()
	password, err := os.ReadFile(filepath.Join(dir, "password"))
	require.NoError(t, err)
	encoded, err := os.ReadFile(filepath.Join(dir, "account.json"))
	require.NoError(t, err)
	key, err := keystore.DecryptKey(encoded, string(password))
	require.NoError(t, err)
	a := &chainAccount{key: key.PrivateKey, address: crypto.PubkeyToAddress(key.PrivateKey.PublicKey), keystore: filepath.Join(dir, "account.json"), passwordFile: filepath.Join(dir, "password"), runScoped: true}
	c.cliAccounts = map[common.Address]*chainAccount{a.address: a}
	require.True(t, c.recipientActive(t, a), "shared performance recipient must already be active")
	return a
}

func TestPerformancePrepareRecipient(t *testing.T) {
	if os.Getenv("E2E_PERF_TARGET") == "" {
		t.Skip("opt-in payment comparison")
	}
	dir := os.Getenv("E2E_PERF_RECIPIENT_DIR")
	require.NotEmpty(t, dir)
	c := preflightChain(t)
	c.configureFunding(t)
	if _, err := os.Stat(filepath.Join(dir, "account.json")); err == nil {
		_ = performanceRecipient(t, c, dir)
		return
	}
	require.NoError(t, os.MkdirAll(dir, 0700))
	a := c.createAccountIn(t, "node", dir)
	c.send(t, c.funder, c.fundingKey, common.Address{}, a.address, nil, ethAmount(t, "E2E_ACCOUNT_ETH", "1"))
	c.activate(t, a)
	require.True(t, c.recipientActive(t, a))
}

// Linux /proc counters describe actual child processes, not the load generator.
// Raw CPU ticks are retained; the runner records SC_CLK_TCK in its manifest.
func performanceResources(pids []int) func() map[string]any {
	type usage struct{ ticks, rss int64 }
	read := func(pid int) usage {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil {
			return usage{}
		}
		i := strings.LastIndexByte(string(data), ')')
		if i < 0 {
			return usage{}
		}
		fields := strings.Fields(string(data[i+1:]))
		if len(fields) < 22 {
			return usage{}
		}
		u, _ := strconv.ParseInt(fields[11], 10, 64)
		s, _ := strconv.ParseInt(fields[12], 10, 64)
		rss, _ := strconv.ParseInt(fields[21], 10, 64)
		return usage{u + s, rss * int64(os.Getpagesize())}
	}
	initial, peak := make([]usage, len(pids)), make([]int64, len(pids))
	for i, pid := range pids {
		initial[i] = read(pid)
		peak[i] = initial[i].rss
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				for i, pid := range pids {
					peak[i] = max(peak[i], read(pid).rss)
				}
			}
		}
	}()
	return func() map[string]any {
		close(stop)
		<-done
		rows := make([]map[string]int64, len(pids))
		for i, pid := range pids {
			last := read(pid)
			rows[i] = map[string]int64{"pid": int64(pid), "cpu_ticks": last.ticks - initial[i].ticks, "sampled_peak_rss_bytes": max(peak[i], last.rss)}
		}
		return map[string]any{"processes": rows}
	}
}

// Retain the expected-value arithmetic with the fixture rather than rounding
// its economic settings independently in scripts.
func TestPerformanceTicketEconomics(t *testing.T) {
	maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	probability := new(big.Int).Div(new(big.Int).Add(maximum, big.NewInt(99999999)), big.NewInt(100000000))
	ev := new(big.Int).Div(new(big.Int).Mul(big.NewInt(1000000000000000), probability), maximum)
	require.Equal(t, int64(10000000), ev.Int64())
}

func TestPerformanceRPCPreservesBatchAndCancellation(t *testing.T) {
	var received atomic.Int64
	input := `[{"jsonrpc":"2.0","id":7,"method":"eth_call","params":[{"input":"0x12345678"},"latest"]},{"jsonrpc":"2.0","id":8,"method":"eth_blockNumber","params":[]}]`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != input {
			http.Error(w, "body changed", 400)
			return
		}
		_, _ = io.WriteString(w, `[{"jsonrpc":"2.0","id":7,"result":"0x01"},{"jsonrpc":"2.0","id":8,"result":"0x02"}]`)
	}))
	defer upstream.Close()
	var records bytes.Buffer
	p := &performanceRPC{role: "test", window: "batch", encoder: json.NewEncoder(&records), selectors: map[string]string{"0x12345678": "Broker.read"}}
	p.delay.Store(int64(25 * time.Millisecond))
	target, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	proxy := httptest.NewServer(p.handler(target))
	defer proxy.Close()
	start := time.Now()
	response, err := http.Post(proxy.URL, "application/json", strings.NewReader(input))
	require.NoError(t, err)
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode)
	require.JSONEq(t, `[{"jsonrpc":"2.0","id":7,"result":"0x01"},{"jsonrpc":"2.0","id":8,"result":"0x02"}]`, string(body))
	require.GreaterOrEqual(t, time.Since(start), 25*time.Millisecond)
	require.EqualValues(t, 1, received.Load(), "a batch must remain one upstream exchange")
	p.mu.Lock()
	var record struct {
		Batch              bool
		Methods, Contracts map[string]int
	}
	err = json.NewDecoder(bytes.NewReader(records.Bytes())).Decode(&record)
	p.mu.Unlock()
	require.NoError(t, err)
	require.True(t, record.Batch)
	require.Equal(t, map[string]int{"eth_call": 1, "eth_blockNumber": 1}, record.Methods)
	require.Equal(t, map[string]int{"Broker.read": 1}, record.Contracts)
	p.delay.Store(int64(time.Second))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", proxy.URL, strings.NewReader(input))
	require.NoError(t, err)
	_, err = http.DefaultClient.Do(req)
	require.Error(t, err)
	proxy.Close()
	require.EqualValues(t, 1, received.Load(), "cancelled delay must not forward work")
}
