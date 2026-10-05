package main

import (
	"context"
	"testing"
	"time"

	"github.com/j0sh/minikafka"
	"github.com/j0sh/minikafka/storage/memory"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/stretchr/testify/require"
)

func TestAnonymousForwarding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := memory.Open()
	require.NoError(t, store.CreateTopic(ctx, "signing", minikafka.TopicOptions{}))
	upstream, err := minikafka.Open(minikafka.Config{Store: store, SASL: &minikafka.SASLConfig{Users: map[string]string{"producer": "secret"}}})
	require.NoError(t, err)
	defer upstream.Close()
	go func() { _ = upstream.Serve(ctx) }()
	proxy, transport, err := openProxy(config{Listen: "127.0.0.1:0", Broker: upstream.Addr(), Topic: "signing", Username: "producer", Password: "secret"})
	require.NoError(t, err)
	defer transport.CloseIdleConnections()
	defer proxy.Close()
	go func() { _ = proxy.Serve(ctx) }()

	// Match go-livepeer's legacy WriterConfig, anonymous dialer and CRC32 balancer.
	client := kafka.NewWriter(kafka.WriterConfig{Brokers: []string{proxy.Addr()}, Topic: "signing", Balancer: kafka.CRC32Balancer{}, BatchTimeout: time.Millisecond})
	defer client.Close()
	payload := []byte(`{"id":"actual-event","type":"create_signed_ticket","data":{"auth_id":"session"}}`)
	message := kafka.Message{Key: []byte("actual-event"), Value: payload, Time: time.UnixMilli(1700000000000), Headers: []kafka.Header{{Key: "source", Value: []byte("go-livepeer")}}}
	for range 2 {
		require.NoError(t, client.WriteMessages(ctx, message))
	}
	mechanism, err := scram.Mechanism(scram.SHA512, "producer", "secret")
	require.NoError(t, err)
	conn, err := (&kafka.Dialer{SASLMechanism: mechanism}).DialLeader(ctx, "tcp", upstream.Addr(), "signing", 0)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	for i := range 2 {
		got, err := conn.ReadMessage(1 << 20)
		require.NoError(t, err)
		require.Equal(t, int64(i), got.Offset)
		require.Equal(t, message.Key, got.Key)
		require.Equal(t, payload, got.Value)
		require.Equal(t, message.Headers, got.Headers)
		require.True(t, message.Time.Equal(got.Time))
	}

	// A forwarding failure must fail Produce rather than acknowledge lost usage.
	badProxy, badTransport, err := openProxy(config{Listen: "127.0.0.1:0", Broker: upstream.Addr(), Topic: "signing", Username: "producer", Password: "wrong"})
	require.NoError(t, err)
	defer badProxy.Close()
	defer badTransport.CloseIdleConnections()
	go func() { _ = badProxy.Serve(ctx) }()
	badClient := &kafka.Writer{Addr: kafka.TCP(badProxy.Addr()), Topic: "signing", RequiredAcks: kafka.RequireAll, MaxAttempts: 1, BatchTimeout: time.Millisecond}
	defer badClient.Close()
	require.Error(t, badClient.WriteMessages(ctx, message))
	_, err = badProxy.Publish(ctx, "other-topic", nil, payload)
	require.Error(t, err)
	last, err := store.LatestOffset(ctx, "signing", 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), last)
}
