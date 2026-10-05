package e2escope

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func (ch *clearinghouseFixture) signerKafkaConfig(t *testing.T, backend string) map[string]any {
	t.Helper()
	if backend == "node" {
		return ch.kafkaConfig(t)
	}
	addr := freeAddr(t)
	bin := filepath.Join(os.Getenv("E2E_RUN_ROOT"), "bin/e2e-kafka-proxy")
	if !composeMode() {
		bin = filepath.Join(buildRoot, "e2e-kafka-proxy")
		value, _ := builds.LoadOrStore("kafka-proxy", &buildResult{})
		result := value.(*buildResult)
		result.once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_BUILD_TIMEOUT", "3m"))
			defer cancel()
			result.output, result.err = exec.CommandContext(ctx, envDefault("E2E_GO_BINARY", "go"), "build", "-mod=readonly", "-o", bin, "./cmd/e2e-kafka-proxy").CombinedOutput()
		})
		require.NoError(t, result.err, "build Kafka proxy: %s", result.output)
	}
	recordBinary(t, "kafka-proxy", bin)
	raw, err := json.Marshal(map[string]string{"Listen": addr, "Broker": ch.broker, "Topic": ch.topic, "Username": "producer", "Password": ch.producerPassword})
	require.NoError(t, err)
	path := secretFile(t, ch.dir, "kafka-proxy.json", string(raw))
	process := startChild(t, "kafka-proxy", bin, ch.dir, []string{path})
	if composeMode() {
		addr = tunneledKafka(t, addr)
	}
	eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, err := (&kafka.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return false
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		_, err = conn.ReadPartitions(ch.topic)
		return err == nil
	}, envDuration(t, "E2E_READY_TIMEOUT", "30s"), 100*time.Millisecond, "Kafka proxy readiness: %s", readLog(t, process.logPath))
	return map[string]any{"Broker": "kafka://" + addr, "Topic": ch.topic}
}

// This chain-free fixture exercises the same subprocess and Compose topology
// used by real go-livepeer signers. The injected event is deliberately ignored.
func TestClearinghouseKafkaProxy(t *testing.T) {
	ch := startClearinghouse(t, nil, true)
	cfg := ch.signerKafkaConfig(t, "go-livepeer")
	writer := kafka.NewWriter(kafka.WriterConfig{Brokers: []string{strings.TrimPrefix(cfg["Broker"].(string), "kafka://")}, Topic: ch.topic, Balancer: kafka.CRC32Balancer{}, BatchTimeout: time.Millisecond})
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw := []byte(`{"id":"proxy-smoke","type":"proxy_smoke_test","timestamp":"1700000000000","data":{}}`)
	require.NoError(t, writer.WriteMessages(ctx, kafka.Message{Key: []byte("proxy-smoke"), Value: raw}))
	ch.waitUsage(t, "ignored", 1)
	db := openReadDB(t, ch.db)
	var got []byte
	require.NoError(t, db.QueryRow("SELECT raw_payload FROM usage_events WHERE status='ignored'").Scan(&got))
	require.Equal(t, raw, got)
}
