package e2escope

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

//go:embed testdata/python_paid_job.py
var pythonPaidJob string

type signerOptions struct {
	backend        string
	chain          *chainFixture
	account        *chainAccount
	orch           *testOrchestrator
	webhook        string
	webhookHeaders string
	kafka          map[string]any
	dir            string
	extra          map[string]any
	useOracle      bool
	maxPriceWei    string
}
type testSigner struct {
	backend, base, metricsBase, logPath string
	account                             *chainAccount
	process                             *childProcess
	opts                                signerOptions
}

func startSigner(t *testing.T, opts signerOptions) *testSigner {
	t.Helper()
	dir := opts.dir
	if dir == "" {
		dir = t.TempDir()
	}
	addr := freeAddr(t)
	base := publicURL(t, "http://"+addr)
	metricsAddr := freeAddr(t)
	var args []string
	if opts.backend == "node" {
		config := map[string]any{"Listen": addr, "MetricsListen": metricsAddr, "KeystoreFile": opts.account.keystore, "KeystorePasswordFile": opts.account.passwordFile, "RPCURLFile": secretFile(t, dir, "rpc", opts.chain.url), "ChainID": opts.chain.chainID.Uint64(), "Controller": opts.chain.controller.Hex(), "MaxHourlyPrice": envDefault("E2E_SIGNER_MAX_HOURLY_USD", "100"), "MaxFixedPrice": envDefault("E2E_SIGNER_MAX_FIXED_USD", "1"), "WeiPerUSD": envDefault("E2E_WEI_PER_USD", "1000000000000000"), "Orchestrators": []string{opts.orch.httpBase}, "DiscoveryGrants": []string{opts.orch.httpAddr}}
		if opts.useOracle {
			delete(config, "WeiPerUSD")
			config["ETHUSDFeed"] = opts.chain.feed.Hex()
			config["ETHUSDMaxAge"] = envDefault("E2E_ORACLE_MAX_AGE", "2h")
		}
		for key, value := range opts.extra {
			config[key] = value
		}
		if opts.webhook != "" {
			config["AuthWebhookFile"] = secretFile(t, dir, "webhook", opts.webhook)
			config["AuthWebhookHeadersFile"] = secretFile(t, dir, "headers", opts.webhookHeaders)
		}
		if opts.kafka != nil {
			config["Kafka"] = opts.kafka
		}
		args = []string{"signer", "--config", writeTOML(t, filepath.Join(dir, "signer.toml"), config)}
	} else {
		limit := opts.maxPriceWei
		if limit == "" {
			limit = envDefault("E2E_GO_SIGNER_MAX_PRICE_WEI", "1000000000000000")
		}
		args = []string{"-remoteSigner", "-httpAddr", addr, "-cliAddr", freeAddr(t), "-datadir", filepath.Join(dir, "data"), "-priceFeedAddr", opts.chain.feed.Hex(), "-maxPricePerUnit", limit, "-remoteDiscovery", "-orchAddr", opts.orch.httpBase}
		args = append(args, chainFlags(opts.chain, opts.account)...)
		if opts.webhook != "" {
			args = append(args, "-remoteSignerWebhookUrl", opts.webhook, "-remoteSignerWebhookHeaders", opts.webhookHeaders)
		}
		if opts.kafka != nil {
			args = append(args, "-monitor=true", "-kafkaBootstrapServers", strings.TrimPrefix(opts.kafka["Broker"].(string), "kafka://"), "-kafkaGatewayTopic", opts.kafka["Topic"].(string))
		}
	}
	opts.dir = dir
	s := &testSigner{backend: opts.backend, base: base, metricsBase: publicURL(t, "http://"+metricsAddr), account: opts.account, opts: opts}
	s.process = startChild(t, "signer-"+opts.backend, binary(t, opts.backend), dir, args)
	s.logPath = s.process.logPath
	waitForHTTP(t, http.MethodPost, s.base+"/sign-orchestrator-info", 200, s.logPath)
	return s
}
func httpJSON(t *testing.T, method, target string, payload any, headers map[string]string) (int, []byte) {
	t.Helper()
	status, data, err := requestJSON(method, target, payload, headers)
	require.NoError(t, err, "%s %s", method, target)
	return status, data
}

// requestJSON can be used by concurrent fixtures without calling testing.T from
// worker goroutines. Assertions stay on the test goroutine after collection.
func requestJSON(method, target string, payload any, headers map[string]string) (int, []byte, error) {
	defaults := map[string]string{"Content-Type": "application/json", "Accept": "application/json"}
	for key, value := range headers {
		defaults[key] = value
	}
	return requestHTTP(method, target, payload, defaults)
}

// requestHTTP accepts JSON values or raw readers and preserves caller headers.
func requestHTTP(method, target string, payload any, headers map[string]string) (int, []byte, error) {
	var body io.Reader
	var contentType string
	switch value := payload.(type) {
	case nil:
	case io.Reader:
		body = value
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(data)
		contentType = "application/json"
	}
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := testHTTPClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

type paymentChallenge struct {
	PaymentParams string `json:"payment_params"`
	ManifestID    string `json:"manifest_id"`
}

// Session payment acknowledgments may encode OrchestratorInfo directly or in
// protobuf PaymentResult.info; challenges and refreshes expose a JSON envelope.
func decodeChallenge(t *testing.T, body []byte) paymentChallenge {
	t.Helper()
	var challenge paymentChallenge
	if json.Valid(body) {
		require.NoError(t, json.Unmarshal(body, &challenge))
	} else {
		if info := wireBytes(t, body, 16); len(info) > 0 {
			body = info
		}
		require.NotEmpty(t, wireBytes(t, body, 2), "payment acknowledgment ticket parameters")
		challenge.PaymentParams = base64.StdEncoding.EncodeToString(body)
		challenge.ManifestID = string(wireBytes(t, wireBytes(t, body, 6), 2))
	}
	require.NotEmpty(t, challenge.PaymentParams)
	return challenge
}

type signedPayment struct {
	Payment string          `json:"payment"`
	Segment string          `json:"segCreds"`
	State   json.RawMessage `json:"state"`
}
type paymentJob struct {
	signer                 *testSigner
	orch                   *testOrchestrator
	route, mode, unit, app string
	challenge              paymentChallenge
	payment                signedPayment
	closeRunner            func()
}

func (j *paymentJob) target() string {
	if j.mode == "persistent" {
		return j.orch.httpBase + "/apps/" + j.route + "/session"
	}
	return j.orch.httpBase + "/apps/" + j.route + "/app"
}
func (j *paymentJob) obtainChallenge(t *testing.T, payerOverride ...common.Address) {
	t.Helper()
	payer := j.signer.account.address
	if len(payerOverride) > 0 {
		payer = payerOverride[0]
	}
	status, body := httpJSON(t, http.MethodPost, j.target(), map[string]any{"prompt": "hello"}, map[string]string{"Livepeer-Payer-Address": payer.Hex()})
	require.Equal(t, 402, status, string(body))
	require.NoError(t, json.Unmarshal(body, &j.challenge))
	require.NotEmpty(t, j.challenge.PaymentParams, string(body))
	info := decode64(t, j.challenge.PaymentParams)
	require.Equal(t, j.orch.infoAddress().Bytes(), wireBytes(t, info, 4), "orchestrator service identity")
	require.Equal(t, j.orch.paymentRecipient().Bytes(), wireBytes(t, wireBytes(t, info, 2), 1), "challenge ticket recipient")
	if j.challenge.ManifestID == "" {
		j.challenge.ManifestID = string(wireBytes(t, wireBytes(t, info, 6), 2))
	}
	require.NotEmpty(t, j.challenge.ManifestID)
}
func (j *paymentJob) request() map[string]any {
	kind := "fixed"
	if j.unit == "hour" {
		kind = "live"
	}
	req := map[string]any{"orchestrator": j.challenge.PaymentParams, "ManifestID": j.challenge.ManifestID, "app": j.app, "type": kind}
	if len(j.payment.State) > 0 {
		req["state"] = j.payment.State
	}
	return req
}
func (j *paymentJob) generate(t *testing.T, headers map[string]string) (int, []byte) {
	return httpJSON(t, http.MethodPost, j.signer.base+"/generate-live-payment", j.request(), headers)
}
func (j *paymentJob) sign(t *testing.T, headers map[string]string) {
	t.Helper()
	status, body := j.generate(t, headers)
	require.Equal(t, 200, status, string(body))
	require.NoError(t, json.Unmarshal(body, &j.payment))
	require.NotEmpty(t, j.payment.Payment)
	require.NotEmpty(t, j.payment.Segment)
	require.NotEmpty(t, j.payment.State)
	payment := decode64(t, j.payment.Payment)
	require.Equal(t, j.signer.account.address.Bytes(), wireBytes(t, payment, 2), "payment payer identity")
	require.Equal(t, j.orch.paymentRecipient().Bytes(), wireBytes(t, wireBytes(t, payment, 1), 1), "signed ticket recipient")
}
func (j *paymentJob) headers() map[string]string {
	return map[string]string{"Livepeer-Payer-Address": j.signer.account.address.Hex(), "Livepeer-Payment": j.payment.Payment, "Livepeer-Segment": j.payment.Segment}
}
func (j *paymentJob) submit(t *testing.T) sessionResponse {
	t.Helper()
	status, body := httpJSON(t, http.MethodPost, j.target(), map[string]any{"prompt": "hello"}, j.headers())
	require.Equal(t, 200, status, string(body))
	var session sessionResponse
	if j.mode == "persistent" {
		require.NoError(t, json.Unmarshal(body, &session))
		require.NotEmpty(t, session.SessionID)
		status, body = httpJSON(t, http.MethodPost, session.AppURL, map[string]any{"prompt": "hello"}, nil)
		require.Equal(t, 200, status, string(body))
	}
	require.JSONEq(t, `{"paid":true,"prompt":"hello"}`, string(body))
	return session
}
func decode64(t *testing.T, s string) []byte {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(s)
	require.NoError(t, err)
	return data
}

// Field operations preserve all unmodified protobuf bytes, including unknown
// fields. Test assertions use the public wire field numbers, not Node codecs.
func wireFields(t *testing.T, data []byte, visit func(protowire.Number, protowire.Type, []byte)) {
	t.Helper()
	for len(data) > 0 {
		field, kind, n := protowire.ConsumeTag(data)
		require.Positive(t, n)
		m := protowire.ConsumeFieldValue(field, kind, data[n:])
		require.NotEqual(t, -1, m)
		require.GreaterOrEqual(t, m, 0)
		visit(field, kind, data[n:n+m])
		data = data[n+m:]
	}
}
func wireBytes(t *testing.T, data []byte, field protowire.Number) []byte {
	t.Helper()
	var value []byte
	wireFields(t, data, func(n protowire.Number, k protowire.Type, raw []byte) {
		if n == field && k == protowire.BytesType {
			v, m := protowire.ConsumeBytes(raw)
			require.Positive(t, m)
			value = v
		}
	})
	return value
}
func wireVarint(t *testing.T, data []byte, field protowire.Number) uint64 {
	t.Helper()
	var value uint64
	wireFields(t, data, func(n protowire.Number, k protowire.Type, raw []byte) {
		if n == field && k == protowire.VarintType {
			v, m := protowire.ConsumeVarint(raw)
			require.Positive(t, m)
			value = v
		}
	})
	return value
}
func replaceWire(t *testing.T, data []byte, field protowire.Number, kind protowire.Type, value []byte) []byte {
	t.Helper()
	var result []byte
	wireFields(t, data, func(n protowire.Number, k protowire.Type, raw []byte) {
		if n != field {
			result = protowire.AppendTag(result, n, k)
			result = append(result, raw...)
		}
	})
	result = protowire.AppendTag(result, field, kind)
	if kind == protowire.BytesType {
		return protowire.AppendBytes(result, value)
	}
	return append(result, value...)
}
func stateObject(t *testing.T, p signedPayment) map[string]any {
	t.Helper()
	var state struct {
		State string `json:"state"`
	}
	require.NoError(t, json.Unmarshal(p.State, &state))
	var obj map[string]any
	require.NoError(t, json.Unmarshal(decode64(t, state.State), &obj))
	return obj
}

type paidFixture struct {
	owner                      *testing.T
	chain                      *chainFixture
	recipient, redeemer, payer *chainAccount
	orch                       *testOrchestrator
	signer                     *testSigner
	upstream                   *httptestServerCount
}
type httptestServerCount struct {
	url   string
	calls atomic.Int64
}

type paidSetupOptions struct {
	orchestrator      orchestratorOptions
	isolatedRecipient bool // Required when a case deliberately changes staking.
	snapshot          bool // Snapshot after R setup, before fixture workers start.
}

func setupPaid(t *testing.T, c *chainFixture, orchBackend, signerBackend string, wallets walletMode, webhook, headers string, kafka map[string]any, overrides ...paidSetupOptions) *paidFixture {
	t.Helper()
	fixturePoolFor(t)
	require.Contains(t, []walletMode{sameWallet, splitWallet}, wallets)
	options := paidSetupOptions{}
	if len(overrides) > 0 {
		options = overrides[0]
	}
	c.configureFunding(t)
	recipient := c.paidRecipient(t, options.isolatedRecipient)
	if options.snapshot {
		c.requireControl(t, recipient)
	}
	require.True(t, c.recipientActive(t, recipient), "paid recipient must be active")
	redeemer := recipient
	if wallets == splitWallet {
		redeemer = c.newAccount(t, false)
		gas := ethAmount(t, "E2E_REDEEMER_ETH", "0.01")
		require.Positive(t, gas.Sign(), "E2E_REDEEMER_ETH")
		c.send(t, c.funder, c.fundingKey, common.Address{}, redeemer.address, nil, gas)
		require.NotEqual(t, recipient.address, redeemer.address, "split recipient and redeemer")
	}
	payer := c.newAccount(t, true)
	c.fundPayer(t, payer, ethAmount(t, "E2E_PAYER_DEPOSIT_ETH", "0.1"), ethAmount(t, "E2E_PAYER_RESERVE_ETH", "0.1"))
	if wallets == splitWallet {
		require.False(t, c.call(t, "bondingManager", c.bonding, "isActiveTranscoder", redeemer.address)[0].(bool), "hot wallet must not be an active orchestrator")
		require.Zero(t, c.call(t, "livepeerToken", c.token, "balanceOf", redeemer.address)[0].(*big.Int).Sign(), "hot wallet must receive no LPT funding")
	}
	c.setupFeed(t, payer)
	t.Logf("wallets=%s recipient=%s redeemer=%s payer=%s", wallets, recipient.address, redeemer.address, payer.address)
	f := &paidFixture{owner: t, chain: c, recipient: recipient, redeemer: redeemer, payer: payer, upstream: &httptestServerCount{}}
	server := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			return
		}
		f.upstream.calls.Add(1)
		respondJSON(t, w, map[string]any{"paid": true, "prompt": "hello"})
	}))
	f.upstream.url = server.URL
	opts := options.orchestrator
	opts.backend, opts.account, opts.chain = orchBackend, redeemer, c
	opts.recipient = common.Address{}
	if wallets == splitWallet {
		opts.recipient = recipient.address
	}
	f.orch = startOrchestrator(t, opts)
	f.signer = startSigner(t, signerOptions{backend: signerBackend, chain: c, account: payer, orch: f.orch, webhook: webhook, webhookHeaders: headers, kafka: kafka})
	return f
}

// Fresh secondary processes share wallet roles, but get their own state directory.
func (f *paidFixture) orchestratorOptions() orchestratorOptions {
	return orchestratorOptions{backend: f.orch.backend, account: f.redeemer, recipient: f.orch.opts.recipient, chain: f.chain}
}

func (orch *testOrchestrator) paymentRecipient() common.Address {
	if orch.opts.recipient != (common.Address{}) {
		return orch.opts.recipient
	}
	return orch.opts.account.address
}

// go-livepeer advertises its service signing account separately from the ticket
// recipient; Node advertises the registered recipient in both fields.
func (orch *testOrchestrator) infoAddress() common.Address {
	if orch.backend == "go-livepeer" {
		return orch.opts.account.address
	}
	return orch.paymentRecipient()
}
func (f *paidFixture) register(t *testing.T, label, mode, unit string, proxy bool) heartbeatResponse {
	t.Helper()
	price := "0.001"
	if unit == "hour" {
		price = "3.6"
	}
	return registerRunnerHTTP(t, f.orch, heartbeatPayload{Label: label, Proxy: proxy, RunnerURL: f.upstream.url, Mode: mode, App: liveRunnerApp, Metadata: label, Capacity: 2, PriceInfo: priceInfo{Price: json.Number(price), Currency: "usd", Unit: unit}}, liveRunnerSecret)
}
func (f *paidFixture) job(t *testing.T, unit, mode string) *paymentJob {
	t.Helper()
	reg := f.register(t, fmt.Sprintf("case-%d", time.Now().UnixNano()), mode, unit, false)
	price := "0.001"
	if unit == "hour" {
		price = "3.6"
	}
	stop := maintainRunner(t, f.orch, reg, heartbeatPayload{RunnerID: reg.RunnerID, RunnerURL: f.upstream.url, Mode: mode, App: liveRunnerApp, Status: "ready", Capacity: 2, PriceInfo: priceInfo{Price: json.Number(price), Currency: "usd", Unit: unit}})
	orch := f.orch
	var once sync.Once
	closeRunner := func() { once.Do(func() { stop(); unregisterPaidRunner(t, orch, reg.RunnerID, reg.HeartbeatSecret) }) }
	t.Cleanup(closeRunner)
	return &paymentJob{signer: f.signer, orch: orch, route: reg.RunnerID, mode: mode, unit: unit, app: liveRunnerApp, closeRunner: closeRunner}
}

func maintainRunner(t *testing.T, orch *testOrchestrator, reg heartbeatResponse, payload heartbeatPayload) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	failures := make(chan error, 1)
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	go func() {
		defer close(done)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				req, err := http.NewRequestWithContext(ctx, "POST", orch.httpBase+"/runners/heartbeat", bytes.NewReader(data))
				if err != nil {
					return
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", reg.HeartbeatSecret)
				resp, err := testHTTPClient.Do(req)
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode != 200 {
						err = fmt.Errorf("fixture heartbeat: HTTP %d", resp.StatusCode)
					}
				}
				if err != nil && ctx.Err() == nil {
					select {
					case failures <- err:
					default:
					}
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
		select {
		case err := <-failures:
			t.Errorf("%v", err)
		default:
		}
	}
}

func TestOnchainPayments(t *testing.T) {
	forEachPair(t, func(t *testing.T, orchBackend, signerBackend string, wallets walletMode) {
		c := preflightChain(t)
		f := setupPaid(t, c, orchBackend, signerBackend, wallets, "", "", nil)
		t.Run("identity", func(t *testing.T) {
			status, body := httpJSON(t, "POST", f.signer.base+"/sign-orchestrator-info", nil, nil)
			require.Equal(t, 200, status)
			var v struct{ Address, Signature string }
			require.NoError(t, json.Unmarshal(body, &v))
			require.Equal(t, strings.ToLower(f.payer.address.Hex()), strings.ToLower(v.Address))
			sig := unhex(t, v.Signature)
			require.Len(t, sig, 65)
			if sig[64] >= 27 {
				sig[64] -= 27
			}
			msg := []byte(v.Address)
			if f.signer.backend == "go-livepeer" {
				msg = crypto.Keccak256(msg)
			}
			hash := crypto.Keccak256([]byte(fmt.Sprintf("\x19Ethereum Signed Message:\n%d", len(msg))), msg)
			key, err := crypto.SigToPub(hash, sig)
			require.NoError(t, err)
			require.Equal(t, f.payer.address, crypto.PubkeyToAddress(*key))
		})
		runPaidMatrix(t, f)
		runPaymentBoundaries(t, f)
		runPriceRegistration(t, f)
		runExtendedPayments(t, f)
		runPaidLifecycle(t, f)
		t.Run("paid multitrack media", func(t *testing.T) { runMediaRoundTrip(t, f.orch, f.signer) })
	})
}
func runPaidMatrix(t *testing.T, f *paidFixture) {
	for _, impl := range runnerImplementations() {
		t.Run(impl.Name(), func(t *testing.T) {
			for _, mode := range []string{"persistent", "single-shot"} {
				for _, unit := range []string{"fixed", "hour"} {
					for _, proxy := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/%s/proxy-%t", mode, unit, proxy), func(t *testing.T) {
							price := "0.001"
							if unit == "hour" {
								price = "3.6"
							}
							label := fmt.Sprintf("sdk-%d", time.Now().UnixNano())
							reg := impl.Start(t, context.Background(), f.orch, runnerConfig{runnerURL: f.upstream.url, label: label, metadata: label, mode: mode, capacity: 2, proxy: proxy, price: priceInfo{Price: json.Number(price), Currency: "usd", Unit: unit}})
							discovered := waitRunnerByMetadata(t, f.orch, label)
							require.NotNil(t, discovered.PriceInfo)
							require.Equal(t, "wei", discovered.PriceInfo.Currency)
							expectedUnit := "fixed"
							if unit == "hour" {
								expectedUnit = "seconds"
							}
							require.Equal(t, expectedUnit, discovered.PriceInfo.Unit)
							before := f.upstream.calls.Load()
							runPythonPaidJob(t, f.signer, discovered, f.orch)
							require.Greater(t, f.upstream.calls.Load(), before)
							reg.Close(t)
							requireRunnerMetadataAbsent(t, f.orch, label)
						})
					}
				}
			}
		})
	}
}
func runPythonPaidJob(t *testing.T, signer *testSigner, runner discoveryRunner, orch *testOrchestrator) {
	t.Helper()
	input, err := json.Marshal(map[string]any{"runner": runner, "signer": signer.base, "orchestrator": orch.httpBase, "headers": map[string]string{}})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_JOB_TIMEOUT", "30s"))
	defer cancel()
	cmd := fixtureCommand(t, ctx, "python-paid-job", "uv", repoDir(t, envPythonRunnerDir), "run", "--frozen", "python", "-u", "-c", pythonPaidJob)
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Python SDK paid job: %s\n%s\n%s", output, readLog(t, orch.logPath), readLog(t, signer.logPath))
	require.Contains(t, string(output), "paid job passed")
}
func runPaymentBoundaries(t *testing.T, f *paidFixture) {
	t.Run("challenge and admission", func(t *testing.T) {
		j := f.job(t, "fixed", "persistent")
		before := f.upstream.calls.Load()
		j.obtainChallenge(t)
		require.Equal(t, before, f.upstream.calls.Load())
		requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 0, 2)
		info := decode64(t, j.challenge.PaymentParams)
		require.Equal(t, f.orch.infoAddress().Bytes(), wireBytes(t, info, 4))
		j.sign(t, nil)
		s := j.submit(t)
		requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 1, 1)
		stopPaidSession(t, f.orch, j.route, s.SessionID)
		requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 0, 2)
		requireStatus(t, "POST", s.AppURL, nil, 404)
	})
	for _, unit := range []string{"fixed", "hour"} {
		t.Run("ceilings/"+unit, func(t *testing.T) {
			j := f.job(t, unit, "persistent")
			j.obtainChallenge(t)
			info := decode64(t, j.challenge.PaymentParams)
			p := wireBytes(t, info, 3)
			price := wireVarint(t, p, 1)
			denom := wireVarint(t, p, 2)
			require.Positive(t, price)
			require.Positive(t, denom)
			for _, delta := range []int64{-1, 0, 1} {
				t.Run(fmt.Sprint(delta), func(t *testing.T) {
					req := j.request()
					u := "fixed"
					if unit == "hour" {
						u = "seconds"
					}
					req["maxPrice"] = map[string]any{"price": int64(price/denom) + delta, "currency": "wei", "unit": u}
					status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
					expected := 200
					if delta < 0 {
						expected = 481
					}
					require.Equal(t, expected, status, string(body))
				})
			}
		})
	}
	t.Run("malformed signer requests", func(t *testing.T) {
		for _, req := range []any{map[string]any{}, map[string]any{"orchestrator": "!!!", "type": "fixed"}, map[string]any{"orchestrator": "AA==", "type": "unknown"}} {
			status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
			require.Equal(t, 400, status, string(body))
		}
	})
	t.Run("state continuation and binding", func(t *testing.T) {
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		j.sign(t, nil)
		old := stateObject(t, j.payment)
		j.sign(t, nil)
		next := stateObject(t, j.payment)
		require.Greater(t, next["SequenceNumber"].(float64), old["SequenceNumber"].(float64))
		require.Equal(t, old["StateID"], next["StateID"])
		require.Equal(t, old["AuthID"], next["AuthID"])
		for _, change := range []string{"app", "type", "ManifestID", "state"} {
			t.Run(change, func(t *testing.T) {
				req := j.request()
				if change == "state" {
					var state map[string]any
					require.NoError(t, json.Unmarshal(j.payment.State, &state))
					key := stateEnvelopeKey(t, state)
					raw := decode64(t, state[key].(string))
					raw[0] ^= 1
					state[key] = base64.StdEncoding.EncodeToString(raw)
					req["state"] = state
				} else {
					req[change] = "mismatch"
				}
				status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
				require.Equal(t, 400, status, string(body))
			})
		}
	})
	t.Run("bad payment credentials", func(t *testing.T) {
		for _, field := range []string{"Livepeer-Payment", "Livepeer-Segment"} {
			t.Run(field, func(t *testing.T) {
				j := f.job(t, "fixed", "persistent")
				j.obtainChallenge(t)
				j.sign(t, nil)
				headers := j.headers()
				raw := decode64(t, headers[field])
				raw[len(raw)-1] ^= 1
				headers[field] = base64.StdEncoding.EncodeToString(raw)
				before := f.upstream.calls.Load()
				status, body := httpJSON(t, "POST", j.target(), nil, headers)
				require.GreaterOrEqual(t, status, 400, string(body))
				require.Equal(t, before, f.upstream.calls.Load())
				requireRunnerCapacity(t, requireDiscoveredRunner(t, f.orch, j.route), 2, 0, 2)
			})
		}
	})
	t.Run("ticket parameter validation", func(t *testing.T) {
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		original := decode64(t, j.challenge.PaymentParams)
		params := wireBytes(t, original, 2)
		for _, field := range []protowire.Number{1, 2, 3, 4, 6} {
			t.Run(fmt.Sprint(field), func(t *testing.T) {
				bad := replaceWire(t, params, field, protowire.BytesType, []byte{0})
				req := j.request()
				req["orchestrator"] = base64.StdEncoding.EncodeToString(replaceWire(t, original, 2, protowire.BytesType, bad))
				status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
				require.GreaterOrEqual(t, status, 400, string(body))
				var response signedPayment
				_ = json.Unmarshal(body, &response)
				require.Empty(t, response.Payment)
			})
		}
	})
	t.Run("batch count and exposure", func(t *testing.T) {
		for _, count := range []int64{100, 101} {
			t.Run(fmt.Sprint(count), func(t *testing.T) {
				j := f.job(t, "fixed", "persistent")
				j.obtainChallenge(t)
				info := decode64(t, j.challenge.PaymentParams)
				params := wireBytes(t, info, 2)
				maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
				// Explicit protocol fixture: keep EV well below the batch exposure
				// cap so this tests the 100-ticket bound independently of rounding
				// in each orchestrator's dynamically generated ticket parameters.
				face := big.NewInt(2000000000)
				probability := new(big.Int).Quo(maximum, big.NewInt(2))
				params = replaceWire(t, params, 2, protowire.BytesType, face.Bytes())
				params = replaceWire(t, params, 3, protowire.BytesType, probability.Bytes())
				info = replaceWire(t, info, 2, protowire.BytesType, params)
				fee := new(big.Int).Quo(new(big.Int).Mul(new(big.Int).Mul(face, probability), big.NewInt(count-1)), maximum)
				fee.Add(fee, big.NewInt(1))
				require.True(t, fee.IsUint64())
				price := protowire.AppendTag(nil, 1, protowire.VarintType)
				price = protowire.AppendVarint(price, fee.Uint64())
				price = protowire.AppendTag(price, 2, protowire.VarintType)
				price = protowire.AppendVarint(price, 1)
				req := j.request()
				req["orchestrator"] = base64.StdEncoding.EncodeToString(replaceWire(t, info, 3, protowire.BytesType, price))
				status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
				if count == 100 {
					require.Equal(t, 200, status, string(body))
					var payment signedPayment
					require.NoError(t, json.Unmarshal(body, &payment))
					require.Equal(t, 100, ticketCount(t, payment))
				} else {
					require.GreaterOrEqual(t, status, 400, string(body))
					assertNoCredentials(t, body)
				}
			})
		}
		for _, exposure := range []struct {
			name      string
			face, fee int64
		}{{"single ticket EV", 8000000000000, 1}, {"batch EV", 4000000000000, 20000000000000}} {
			t.Run(exposure.name, func(t *testing.T) {
				j := f.job(t, "fixed", "persistent")
				j.obtainChallenge(t)
				info := decode64(t, j.challenge.PaymentParams)
				params := wireBytes(t, info, 2)
				maximum := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
				params = replaceWire(t, params, 2, protowire.BytesType, big.NewInt(exposure.face).Bytes())
				params = replaceWire(t, params, 3, protowire.BytesType, new(big.Int).Quo(maximum, big.NewInt(2)).Bytes())
				info = replaceWire(t, info, 2, protowire.BytesType, params)
				price := protowire.AppendTag(nil, 1, protowire.VarintType)
				price = protowire.AppendVarint(price, uint64(exposure.fee))
				price = protowire.AppendTag(price, 2, protowire.VarintType)
				price = protowire.AppendVarint(price, 1)
				req := j.request()
				req["orchestrator"] = base64.StdEncoding.EncodeToString(replaceWire(t, info, 3, protowire.BytesType, price))
				status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
				require.GreaterOrEqual(t, status, 400, string(body))
				assertNoCredentials(t, body)
			})
		}
		t.Run("face exceeds escrow", func(t *testing.T) {
			j := f.job(t, "fixed", "persistent")
			j.obtainChallenge(t)
			info := decode64(t, j.challenge.PaymentParams)
			params := wireBytes(t, info, 2)
			deposit, reserve, _ := senderEscrow(t, f.chain, f.payer.address)
			face := new(big.Int).Add(new(big.Int).Add(deposit, reserve), big.NewInt(1))
			params = replaceWire(t, params, 2, protowire.BytesType, face.Bytes())
			req := j.request()
			req["orchestrator"] = base64.StdEncoding.EncodeToString(replaceWire(t, info, 2, protowire.BytesType, params))
			status, body := httpJSON(t, "POST", f.signer.base+"/generate-live-payment", req, nil)
			require.GreaterOrEqual(t, status, 400, string(body))
			assertNoCredentials(t, body)
		})
	})
	t.Run("refresh authorization", func(t *testing.T) {
		j := f.job(t, "hour", "persistent")
		j.obtainChallenge(t)
		j.sign(t, nil)
		session := j.submit(t)
		defer stopPaidSession(t, f.orch, j.route, session.SessionID)
		status, body := httpJSON(t, "POST", f.orch.httpBase+"/refresh-payment", map[string]any{"sender": f.payer.address.Hex(), "manifest_id": j.challenge.ManifestID}, nil)
		require.Equal(t, 200, status, string(body))
		status, _ = httpJSON(t, "POST", f.orch.httpBase+"/refresh-payment", map[string]any{"sender": common.HexToAddress("0xdead").Hex(), "manifest_id": j.challenge.ManifestID}, nil)
		require.GreaterOrEqual(t, status, 400)
	})
	t.Run("unfunded signer", func(t *testing.T) {
		account := f.chain.newAccount(t, true)
		s := startSigner(t, signerOptions{backend: f.signer.backend, chain: f.chain, account: account, orch: f.orch})
		j := f.job(t, "fixed", "persistent")
		j.signer = s
		j.obtainChallenge(t, f.payer.address)
		status, body := j.generate(t, nil)
		require.GreaterOrEqual(t, status, 400, string(body))
		var p signedPayment
		_ = json.Unmarshal(body, &p)
		require.Empty(t, p.Payment)
	})
	for _, lifecycle := range []string{"replica", "restart"} {
		t.Run("signer "+lifecycle+" continuation", func(t *testing.T) {
			j := f.job(t, "hour", "persistent")
			j.obtainChallenge(t)
			j.sign(t, nil)
			session := j.submit(t)
			defer stopPaidSession(t, f.orch, j.route, session.SessionID)
			old := stateObject(t, j.payment)
			if lifecycle == "restart" {
				f.signer.process.stop()
				f.signer = startSigner(f.owner, f.signer.opts)
				j.signer = f.signer
			} else {
				opts := f.signer.opts
				opts.dir = ""
				j.signer = startSigner(t, opts)
			}
			j.sign(t, nil)
			next := stateObject(t, j.payment)
			require.Equal(t, old["StateID"], next["StateID"])
			require.Greater(t, next["SequenceNumber"].(float64), old["SequenceNumber"].(float64))
			status, body := httpJSON(t, "POST", f.orch.httpBase+"/apps/"+j.route+"/session/"+session.SessionID+"/payment", nil, j.headers())
			require.Equal(t, 200, status, string(body))
			requireStatus(t, "POST", session.AppURL, nil, 200)
		})
	}
}

func TestOnchainInfrastructure(t *testing.T) {
	t.Run("canonical headers", func(t *testing.T) { preflightChain(t).requireCanonicalHeaders(t) })
	t.Run("finality", func(t *testing.T) { preflightChain(t).requireFinality(t) })
	t.Run("chain controls", func(t *testing.T) {
		c := preflightChain(t)
		c.requireControl(t)
		before := c.block(t)
		var result any
		c.request(t, &result, "evm_mine")
		require.Greater(t, c.block(t), before)
	})
}

// New payment scenarios accept either successful cleanup response. The original
// off-chain API parity checks retain their required status codes.
func stopPaidSession(t *testing.T, orch *testOrchestrator, runner, session string) {
	t.Helper()
	status, body := httpJSON(t, "POST", orch.httpBase+"/apps/"+runner+"/session/"+session+"/stop", nil, nil)
	require.Contains(t, []int{200, 204}, status, string(body))
}
func unregisterPaidRunner(t *testing.T, orch *testOrchestrator, runner, secret string) {
	t.Helper()
	status, body := httpJSON(t, "POST", orch.httpBase+"/runners/"+runner+"/unregister", nil, map[string]string{"Authorization": secret})
	require.Contains(t, []int{200, 204}, status, string(body))
}
func waitRunnerByMetadata(t *testing.T, orch *testOrchestrator, metadata string) discoveryRunner {
	t.Helper()
	waitDiscovered(t, orch, metadata)
	return requireDiscoveredRunnerByMetadata(t, orch, metadata)
}

func stateEnvelopeKey(t *testing.T, state map[string]any) string {
	t.Helper()
	for key, value := range state {
		if strings.EqualFold(key, "state") {
			_, ok := value.(string)
			require.True(t, ok, "encoded state must be base64 text")
			return key
		}
	}
	t.Fatal("signed state missing encoded state")
	return ""
}
