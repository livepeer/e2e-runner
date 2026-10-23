package e2escope

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

//go:embed testdata/python_streaming_upload.py
var pythonStreamingUpload string

// Pause a real recipient collateral read, after signing and before admission.
// Signer and setup RPCs bypass this proxy, so a slow signer cannot satisfy the test.
type uploadPaymentGate struct {
	armed, passed atomic.Bool
	checks, reads atomic.Int64
}

func (g *uploadPaymentGate) arm() {
	g.passed.Store(false)
	g.armed.Store(true)
}

func streamingPaymentRPC(t *testing.T, c *chainFixture, gate *uploadPaymentGate) string {
	t.Helper()
	target, err := url.Parse(c.url)
	require.NoError(t, err)
	proxy := fixtureProxy(target)
	selector := hex.EncodeToString(c.contracts["ticketBroker"].Methods["getSenderInfo"].ID)
	server := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		var request struct {
			Method string
			Params []json.RawMessage
		}
		var call struct{ To, Data, Input string }
		if json.Unmarshal(data, &request) == nil && request.Method == "eth_call" && len(request.Params) > 0 {
			_ = json.Unmarshal(request.Params[0], &call)
		}
		if strings.EqualFold(call.To, c.broker.Hex()) &&
			(strings.HasPrefix(call.Data, "0x"+selector) || strings.HasPrefix(call.Input, "0x"+selector)) &&
			gate.armed.CompareAndSwap(true, false) {
			gate.checks.Add(1)
			// Exceed net/http's usual one-second ExpectContinueTimeout. An
			// intermediate proxy must not start reading the upload on timeout.
			timer := time.NewTimer(1500 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				gate.passed.Store(true)
			case <-r.Context().Done():
				return
			}
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
		proxy.ServeHTTP(w, r)
	}))
	eventually(t, func() bool {
		status, body, err := requestJSON("POST", server.URL, map[string]any{
			"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []any{},
		}, nil)
		if err != nil || status != 200 {
			return false
		}
		var response struct{ Result string }
		return json.Unmarshal(body, &response) == nil && response.Result == "0x"+c.chainID.Text(16)
	}, envDuration(t, "E2E_READY_TIMEOUT", "30s"), 250*time.Millisecond,
		"INFRASTRUCTURE: payment RPC proxy must reach the selected chain")
	return server.URL
}

type uploadSource struct {
	io.Reader
	reads atomic.Int64
	early atomic.Bool
	gate  *uploadPaymentGate
}

func (s *uploadSource) Read(p []byte) (int, error) {
	s.reads.Add(1)
	if !s.gate.passed.Load() {
		s.early.Store(true)
	}
	return s.Reader.Read(p)
}

func expectUpload(t *testing.T, target string, headers map[string]string, payload []byte, gate *uploadPaymentGate, want int) []byte {
	t.Helper()
	source := &uploadSource{Reader: bytes.NewReader(payload), gate: gate}
	var interim []int
	var early atomic.Bool
	trace := &httptrace.ClientTrace{Got1xxResponse: func(code int, _ textproto.MIMEHeader) error {
		interim = append(interim, code)
		if !gate.passed.Load() {
			early.Store(true)
		}
		return nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_JOB_TIMEOUT", "30s"))
	defer cancel()
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), "POST", target, source)
	require.NoError(t, err)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Content-Type", "application/octet-stream")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// There is no upload-on-timeout fallback; the request context bounds the wait.
	transport.ExpectContinueTimeout = time.Duration(1<<63 - 1)
	defer transport.CloseIdleConnections()
	response, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Logf("request_error=%v interim=%v source_reads=%d early_continue=%t early_read=%t", err, interim, source.reads.Load(), early.Load(), source.early.Load())
	}
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	t.Logf("status=%d interim=%v source_reads=%d early_continue=%t early_read=%t", response.StatusCode, interim, source.reads.Load(), early.Load(), source.early.Load())
	require.Equal(t, want, response.StatusCode, string(body))
	if want == 200 {
		require.NotEmpty(t, interim, "accepted uploads need an explicit 100 Continue")
		for _, code := range interim {
			require.Equal(t, 100, code)
		}
		require.Positive(t, source.reads.Load())
	} else {
		require.Empty(t, interim, "rejected uploads must receive only a final response")
		require.Zero(t, source.reads.Load(), "rejected uploads must leave the source untouched")
	}
	require.False(t, early.Load(), "interim response before payment validation was released")
	require.False(t, source.early.Load(), "source read before payment validation was released")
	return body
}

func TestOnchainStreamingUploads(t *testing.T) {
	forEachPair(t, func(t *testing.T, orchBackend, signerBackend string, wallets walletMode) {
		if orchBackend != "node" {
			t.Skip("go-livepeer does not support paid Expect: 100-continue uploads")
		}
		c := preflightChain(t)
		gate := &uploadPaymentGate{}
		rpcURL := streamingPaymentRPC(t, c, gate)
		f := setupPaid(t, c, orchBackend, signerBackend, wallets, "", "", nil, paidSetupOptions{
			orchestrator: orchestratorOptions{extra: map[string]any{
				"RPCURLFile": secretFile(t, t.TempDir(), "rpc", rpcURL),
			}},
		})
		payload := bytes.Repeat([]byte("streaming upload\x00\xff"), 8192)
		digest := sha256.Sum256(payload)
		expected := map[string]any{"size": len(payload), "sha256": hex.EncodeToString(digest[:]), "content_type": "application/octet-stream"}
		var calls atomic.Int64
		app := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(200)
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			calls.Add(1)
			digest := sha256.Sum256(body)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"size": len(body), "sha256": hex.EncodeToString(digest[:]), "content_type": r.Header.Get("Content-Type")})
		}))
		control := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/arm" {
				gate.arm()
			} else {
				gate.reads.Add(1)
				if !gate.passed.Load() {
					http.Error(w, "source read before payment validation", 409)
					return
				}
			}
			w.WriteHeader(204)
		}))
		signerURL, err := url.Parse(f.signer.base)
		require.NoError(t, err)
		rejectedSigner := fixtureProxy(signerURL)
		rejectedSigner.ModifyResponse = func(response *http.Response) error {
			if response.Request.URL.Path != "/generate-live-payment" || response.StatusCode != 200 {
				return nil
			}
			defer response.Body.Close()
			var payment map[string]any
			if err := json.NewDecoder(response.Body).Decode(&payment); err != nil {
				return err
			}
			payment["payment"] = "invalid"
			body, err := json.Marshal(payment)
			response.Body = io.NopCloser(bytes.NewReader(body))
			response.ContentLength = int64(len(body))
			response.Header.Del("Content-Length")
			return err
		}
		rejectedSignerURL := newFixtureServer(t, rejectedSigner).URL
		for _, impl := range runnerImplementations() {
			t.Run(impl.Name(), func(t *testing.T) {
				for _, proxy := range []bool{false, true} {
					t.Run(map[bool]string{false: "direct", true: "proxy"}[proxy], func(t *testing.T) {
						label := t.Name()
						reg := impl.Start(t, context.Background(), f.orch, runnerConfig{runnerURL: app.URL, mode: "single-shot", metadata: label, capacity: 1, proxy: proxy, price: priceInfo{Price: "0.001", Currency: "usd", Unit: "fixed"}})
						runner := waitRunnerByMetadata(t, f.orch, label)
						j := &paymentJob{signer: f.signer, orch: f.orch, route: reg.RunnerID(), mode: "single-shot", unit: "fixed", app: liveRunnerApp}
						t.Run("protocol", func(t *testing.T) {
							before := calls.Load()
							gate.passed.Store(false)
							body := expectUpload(t, runner.URL, map[string]string{"Livepeer-Payer-Address": f.payer.address.Hex()}, payload, gate, 402)
							j.challenge = decodeChallenge(t, body)
							j.sign(t, nil)
							bad := j.headers()
							payment := decode64(t, j.payment.Payment)
							ticket := wireBytes(t, payment, 4)
							signature := wireBytes(t, ticket, 2)
							signature[0] ^= 1
							ticket = replaceWire(t, ticket, 2, protowire.BytesType, signature)
							bad["Livepeer-Payment"] = base64.StdEncoding.EncodeToString(replaceWire(t, payment, 4, protowire.BytesType, ticket))
							gate.arm()
							checks := gate.checks.Load()
							expectUpload(t, runner.URL, bad, payload, gate, 403)
							require.Equal(t, checks+1, gate.checks.Load(), "payment rejection must reach the delayed collateral check")
							require.Equal(t, before, calls.Load(), "unpaid and rejected uploads must not reach the application")
							gate.arm()
							checks = gate.checks.Load()
							body = expectUpload(t, runner.URL, j.headers(), payload, gate, 200)
							require.Equal(t, checks+1, gate.checks.Load())
							expectedJSON, err := json.Marshal(expected)
							require.NoError(t, err)
							require.JSONEq(t, string(expectedJSON), string(body))
							require.Equal(t, before+1, calls.Load())
						})
						for _, source := range []string{"iterator", "file"} {
							for _, outcome := range []string{"accepted", "rejected"} {
								t.Run("python-"+source+"/"+outcome, func(t *testing.T) {
									checks, reads, before := gate.checks.Load(), gate.reads.Load(), calls.Load()
									signer := f.signer.base
									if outcome == "rejected" {
										signer = rejectedSignerURL
										gate.passed.Store(false)
									}
									input, err := json.Marshal(map[string]any{"runner": runner, "signer": signer, "orchestrator": f.orch.httpBase, "control": control.URL, "payload": payload, "expected": expected, "source": source, "reject": outcome == "rejected"})
									require.NoError(t, err)
									ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_JOB_TIMEOUT", "30s"))
									defer cancel()
									cmd := fixtureCommand(t, ctx, "python-streaming-upload", "uv", repoDir(t, envPythonRunnerDir), "run", "--frozen", "python", "-u", "-c", pythonStreamingUpload)
									cmd.Stdin = bytes.NewReader(input)
									output, err := cmd.CombinedOutput()
									t.Logf("Python %s: %s", source, output)
									require.NoError(t, err, "Python streaming upload: %s\n%s\n%s", output, readLog(t, f.orch.logPath), readLog(t, f.signer.logPath))
									if outcome == "accepted" {
										require.Equal(t, checks+1, gate.checks.Load())
										require.Greater(t, gate.reads.Load(), reads, "source consumption must be observed")
										require.Equal(t, before+1, calls.Load(), "only the paid retry reaches the application")
									} else {
										require.Equal(t, reads, gate.reads.Load(), "rejected Python uploads leave the source unread")
										require.Equal(t, before, calls.Load())
									}
								})
							}
						}
						reg.Close(t)
					})
				}
			})
		}
	})
}
