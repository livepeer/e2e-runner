// e2e-runner hosts the real Go SDK in its own fixture container. The control
// endpoint lets the test goroutine assert results without moving SDK work back
// into the harness process.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	runner "github.com/livepeer/golang-runner"
)

type config struct {
	Listen, Token string
	Options       runner.Options
}
type operation struct {
	Kind, Target string
	Headers      http.Header
	Channels     []runner.ChannelRequest
	Names        []string
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reg, err := runner.Register(ctx, cfg.Options)
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{Addr: cfg.Listen, ReadHeaderTimeout: 10 * time.Second}
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+cfg.Token {
			http.Error(w, "unauthorized", 401)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/meta" {
			_ = json.NewEncoder(w).Encode(map[string]any{"id": reg.RunnerID(), "url": reg.OrchestratorURL(), "interval": reg.HeartbeatInterval()})
			return
		}
		var op operation
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&op); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), "POST", cfg.Options.RunnerURL, nil)
		req.Header = op.Headers
		var result any
		var err error
		switch op.Kind {
		case "channels":
			result, err = reg.CreateTrickleChannels(r.Context(), req, op.Channels)
		case "delete-channels":
			result, err = reg.DeleteTrickleChannels(r.Context(), req, op.Names)
		case "proxy":
			result, err = reg.CreateProxy(r.Context(), req, op.Target)
		case "close":
			err = reg.Close(r.Context())
		default:
			http.Error(w, "unknown SDK operation", 400)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
	})
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Print(err)
			stop()
		}
	}()
	<-ctx.Done()
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = reg.Close(closeCtx)
	_ = server.Shutdown(closeCtx)
}
