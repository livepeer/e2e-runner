// e2e-kafka-proxy accepts anonymous Kafka Produce requests and forwards the
// original records to Clearinghouse using its authenticated producer role.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type config struct {
	Listen, Broker, Topic, Username, Password string
}

// The memory store supplies single-topic metadata only. Produce acknowledgments
// and offsets come from the upstream broker; the proxy has no event queue or
// durable outbox, and does not serve consumers.
type forwardingStore struct {
	minikafka.Store
	mu        sync.Mutex
	cfg       config
	transport *kafka.Transport
}

func (s *forwardingStore) Append(ctx context.Context, req minikafka.AppendRequest) (minikafka.AppendResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Topic != s.cfg.Topic {
		return minikafka.AppendResult{}, minikafka.ErrTopicNotFound
	}
	if req.Partition != 0 {
		return minikafka.AppendResult{}, minikafka.ErrInvalidPartition
	}
	messages := make([]kafka.Message, len(req.Records))
	for i, record := range req.Records {
		messages[i] = kafka.Message{Key: record.Key, Value: record.Value, Time: record.Timestamp}
		for _, header := range record.Headers {
			messages[i].Headers = append(messages[i].Headers, kafka.Header{Key: header.Key, Value: header.Value})
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result := minikafka.AppendResult{BaseOffset: -1, LastOffset: -1}
	writer := &kafka.Writer{Addr: kafka.TCP(s.cfg.Broker), Topic: s.cfg.Topic, Transport: s.transport,
		RequiredAcks: kafka.RequireAll, MaxAttempts: 1, BatchSize: len(messages), BatchTimeout: time.Millisecond,
		WriteTimeout: 10 * time.Second}
	writer.Completion = func(batch []kafka.Message, err error) {
		if err == nil && len(batch) > 0 {
			if result.BaseOffset == -1 {
				result.BaseOffset = batch[0].Offset
			}
			result.LastOffset = batch[len(batch)-1].Offset
		}
	}
	err := writer.WriteMessages(ctx, messages...)
	_ = writer.Close() // Wait for completion even when the request context expires.
	if err != nil {
		return minikafka.AppendResult{}, err
	}
	return result, nil
}

func openProxy(cfg config) (*minikafka.Broker, *kafka.Transport, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, cfg.Username, cfg.Password)
	if err != nil {
		return nil, nil, err
	}
	transport := &kafka.Transport{SASL: mechanism}
	store := &forwardingStore{Store: memory.Open(), cfg: cfg, transport: transport}
	if err := store.CreateTopic(context.Background(), cfg.Topic, minikafka.TopicOptions{Partitions: 1}); err != nil {
		return nil, transport, err
	}
	broker, err := minikafka.Open(minikafka.Config{Addr: cfg.Listen, Store: store})
	return broker, transport, err
}

func run(ctx context.Context, cfg config) error {
	if cfg.Listen == "" || cfg.Broker == "" || cfg.Topic == "" || cfg.Username == "" || cfg.Password == "" {
		return fmt.Errorf("listen, broker, topic and upstream credentials are required")
	}
	broker, transport, err := openProxy(cfg)
	if transport != nil {
		defer transport.CloseIdleConnections()
	}
	if err != nil {
		return err
	}
	defer broker.Close()
	return broker.Serve(ctx)
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: e2e-kafka-proxy config.json")
	}
	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	var cfg config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}
