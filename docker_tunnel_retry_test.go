package e2escope

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFixtureProxyUsesUpstreamHost(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Host", r.Host)
		if r.Host == "control.trycloudflare.com" {
			http.Error(w, "fixture endpoint inactive", 503)
			return
		}
		_, _ = io.Copy(w, r.Body)
	}))
	defer upstream.Close()
	target, err := url.Parse(upstream.URL)
	require.NoError(t, err)
	proxy := httptest.NewServer(fixtureProxy(target))
	defer proxy.Close()
	const payload = `{"method":"eth_chainId"}`
	req, err := http.NewRequest("POST", proxy.URL, strings.NewReader(payload))
	require.NoError(t, err)
	req.Host = "control.trycloudflare.com"
	response, err := proxy.Client().Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode, string(body))
	require.Equal(t, target.Host, response.Header.Get("X-Upstream-Host"))
	require.Equal(t, payload, string(body))
}

func TestQuickTunnelRetryAfter(t *testing.T) {
	received := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	const maxDelay time.Duration = 1<<63 - 1
	for _, tc := range []struct {
		name, header string
		attempt      int
		want         time.Duration
	}{
		{"seconds", "300", 0, 290 * time.Second},
		{"HTTP date", received.Add(5 * time.Minute).Format(http.TimeFormat), 0, 290 * time.Second},
		{"whitespace", "\t600 ", 0, 590 * time.Second},
		{"above backoff cap", "3600", 3, 3590 * time.Second},
		{"short delay", "10", 0, 30 * time.Second},
		{"zero", "0", 0, 30 * time.Second},
		{"past date", received.Add(-time.Hour).Format(http.TimeFormat), 0, 30 * time.Second},
		{"absent", "", 0, 30 * time.Second},
		{"invalid", "not a date", 1, 60 * time.Second},
		{"negative", "-300", 0, 30 * time.Second},
		{"fractional", "1.5", 0, 30 * time.Second},
		{"signed integer", "+300", 0, 30 * time.Second},
		{"bounded fallback", "invalid", 3, 2 * time.Minute},
		{"duration overflow", "9223372037", 0, maxDelay - 10*time.Second},
		{"integer overflow", "18446744073709551616", 0, maxDelay - 10*time.Second},
		{"invalid large number", "18446744073709551616x", 0, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New("cloudflared exited")
			err := fmt.Errorf("launch: %w", &quickTunnelRateLimitError{err: cause, notBefore: retryAfterTime(tc.header, received)})
			delay, retry := quickTunnelRetryDelay(err, tc.attempt, received.Add(10*time.Second))
			require.True(t, retry)
			require.Equal(t, tc.want, delay)
			require.ErrorIs(t, err, cause)
		})
	}
	_, retry := quickTunnelRetryDelay(errors.New("unrelated error body mentions status 429"), 0, received)
	require.False(t, retry, "only an observed provisioning HTTP 429 should be retried")
}

func TestQuickTunnelProvisioningHeaders(t *testing.T) {
	date := time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second)
	for _, tc := range []struct {
		name, header string
		status       int
	}{
		{"seconds", "600", http.StatusTooManyRequests},
		{"HTTP date", date.Format(http.TimeFormat), http.StatusTooManyRequests},
		{"absent", "", http.StatusTooManyRequests},
		{"invalid", "invalid", http.StatusTooManyRequests},
		{"success", "600", http.StatusOK},
		{"other failure", "600", http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type request struct {
				method, path, host, userAgent, body string
				err                                 error
			}
			requests := make(chan request, 1)
			const responseBody = `{"opaque":"fixture response mentions status 429"}`
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				requests <- request{r.Method, r.URL.Path, r.Host, r.UserAgent(), string(body), err}
				w.Header().Set("Retry-After", tc.header)
				w.Header().Set("X-Request-ID", "fixture")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, responseBody)
			}))
			defer api.Close()
			target, err := url.Parse(api.URL)
			require.NoError(t, err)
			proxy, limited := tunnelProvisioningProxy(target, api.Client().Transport)
			defer proxy.Close()
			req, err := http.NewRequest(http.MethodPost, proxy.URL+"/tunnel", strings.NewReader(`{"auth_mode":"otp"}`))
			require.NoError(t, err)
			req.Header.Set("User-Agent", "cloudflared-fixture")
			before := time.Now()
			resp, err := proxy.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			after := time.Now()
			require.NoError(t, err)
			require.Equal(t, tc.status, resp.StatusCode)
			require.Equal(t, responseBody, string(body))
			require.Equal(t, tc.header, resp.Header.Get("Retry-After"))
			require.Equal(t, "fixture", resp.Header.Get("X-Request-ID"))
			got := <-requests
			require.NoError(t, got.err)
			require.Equal(t, request{http.MethodPost, "/tunnel", target.Host, "cloudflared-fixture", `{"auth_mode":"otp"}`, nil}, got)
			select {
			case deadline := <-limited:
				require.Equal(t, http.StatusTooManyRequests, tc.status)
				switch tc.name {
				case "seconds":
					require.False(t, deadline.Before(before.Add(10*time.Minute)))
					require.False(t, deadline.After(after.Add(10*time.Minute)))
				case "HTTP date":
					require.Equal(t, date, deadline)
				default:
					require.Zero(t, deadline)
				}
			default:
				require.NotEqual(t, http.StatusTooManyRequests, tc.status, "must capture 429 even without a usable Retry-After")
			}
		})
	}
}
