package e2escope

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed testdata/python_streaming_upload.py
var pythonStreamingUpload string

func TestOnchainStreamingUploads(t *testing.T) {
	forEachPair(t, func(t *testing.T, orchBackend, signerBackend string, wallets walletMode) {
		if orchBackend != "node" {
			t.Skip("streaming upload coverage currently targets Node")
		}
		c := preflightChain(t)
		f := setupPaid(t, c, orchBackend, signerBackend, wallets, "", "", nil)
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
		signerURL, err := url.Parse(f.signer.base)
		require.NoError(t, err)
		rejectedSigner := &httputil.ReverseProxy{Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(signerURL)
			// The response is decoded and rewritten below; request plain JSON
			// rather than forwarding the Python client's compression preferences.
			r.Out.Header.Set("Accept-Encoding", "identity")
		}}
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
						for _, source := range []string{"iterator", "file"} {
							for _, outcome := range []string{"accepted", "rejected"} {
								t.Run("python-"+source+"/"+outcome, func(t *testing.T) {
									before := calls.Load()
									signer := f.signer.base
									if outcome == "rejected" {
										signer = rejectedSignerURL
									}
									input, err := json.Marshal(map[string]any{"runner": runner, "signer": signer, "orchestrator": f.orch.httpBase, "payload": payload, "expected": expected, "source": source, "reject": outcome == "rejected"})
									require.NoError(t, err)
									ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_JOB_TIMEOUT", "30s"))
									defer cancel()
									cmd := fixtureCommand(t, ctx, "python-streaming-upload", "uv", repoDir(t, envPythonRunnerDir), "run", "--frozen", "python", "-u", "-c", pythonStreamingUpload)
									cmd.Stdin = bytes.NewReader(input)
									output, err := cmd.CombinedOutput()
									t.Logf("Python %s: %s", source, output)
									require.NoError(t, err, "Python streaming upload: %s\n%s\n%s", output, readLog(t, f.orch.logPath), readLog(t, f.signer.logPath))
									if outcome == "accepted" {
										require.Equal(t, before+1, calls.Load(), "only the paid retry reaches the application")
									} else {
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
