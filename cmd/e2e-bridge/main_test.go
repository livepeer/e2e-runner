package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/metadata"
	"github.com/stretchr/testify/require"
)

func TestKafkaDiscoveryUsesBridge(t *testing.T) {
	for _, test := range []struct {
		name     string
		version  int16
		response protocol.Message
	}{
		{"metadata-v0", 0, &metadata.Response{Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: "original", Port: 9092}}}},
		{"metadata-v8", 8, &metadata.Response{Brokers: []metadata.ResponseBroker{{NodeID: 1, Host: "original", Port: 9092}}}},
		{"coordinator", 2, &findcoordinator.Response{Host: "original", Port: 9092}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var input bytes.Buffer
			require.NoError(t, protocol.WriteResponse(&input, test.version, 7, test.response))
			frame, err := rewriteResponse(input.Bytes(), test.response.ApiKey(), test.version, "127.0.0.1:32123")
			require.NoError(t, err)
			id, response, err := protocol.ReadResponse(bytes.NewReader(frame), test.response.ApiKey(), test.version)
			require.NoError(t, err)
			require.EqualValues(t, 7, id)
			switch r := response.(type) {
			case *metadata.Response:
				require.Equal(t, "127.0.0.1", r.Brokers[0].Host)
				require.EqualValues(t, 32123, r.Brokers[0].Port)
			case *findcoordinator.Response:
				require.Equal(t, "127.0.0.1", r.Host)
				require.EqualValues(t, 32123, r.Port)
			}
		})
	}
}

// Exercise framing across independently fragmented WebSocket messages and
// enforce the rewritten endpoint on the actual producer-facing byte stream.
func TestKafkaRelayFragmentedResponse(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		_, frame, err := ws.ReadMessage()
		if err != nil {
			return
		}
		id := int32(binary.BigEndian.Uint32(frame[8:12]))
		var out bytes.Buffer
		if protocol.WriteResponse(&out, 0, id, &metadata.Response{Brokers: []metadata.ResponseBroker{{Host: "original", Port: 9092}}}) != nil {
			return
		}
		data := out.Bytes()
		for _, fragment := range [][]byte{data[:2], data[2:7], data[7:]} {
			if ws.WriteMessage(websocket.BinaryMessage, fragment) != nil {
				return
			}
		}
		_, _, _ = ws.ReadMessage()
	}))
	defer server.Close()
	dialer := websocket.Dialer{TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig}
	ws, _, err := dialer.Dial("wss"+strings.TrimPrefix(server.URL, "https"), nil)
	require.NoError(t, err)
	producer, bridge := net.Pipe()
	defer producer.Close()
	done := make(chan struct{})
	go func() { kafkaRelay(bridge, ws, "127.0.0.1:32123"); close(done) }()
	require.NoError(t, producer.SetDeadline(time.Now().Add(5*time.Second)))
	var request bytes.Buffer
	require.NoError(t, protocol.WriteRequest(&request, 0, 77, "e2e", &metadata.Request{}))
	_, err = io.Copy(producer, &request)
	require.NoError(t, err)
	frame, err := readFrame(producer)
	require.NoError(t, err)
	id, response, err := protocol.ReadResponse(bytes.NewReader(frame), protocol.Metadata, 0)
	require.NoError(t, err)
	require.EqualValues(t, 77, id)
	require.EqualValues(t, 32123, response.(*metadata.Response).Brokers[0].Port)
	producer.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay failed to shut down")
	}
}

func TestRejectOversizedKafkaFrame(t *testing.T) {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], 65<<20)
	_, err := readFrame(bytes.NewReader(header[:]))
	require.ErrorContains(t, err, "64 MiB")
}

func TestGatewayReusesTunnelWithoutChangingPublicHost(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "first:"+r.Host+":"+r.URL.Path) }))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "second:"+r.Host+":"+r.URL.Path)
	}))
	defer second.Close()
	path := filepath.Join(t.TempDir(), "target")
	server := httptest.NewTLSServer(gatewayHandler(path))
	defer server.Close()
	for _, test := range []struct {
		target string
		status int
		body   string
	}{
		{first.URL, 200, "first:public.trycloudflare.com:/e2e-proxy/session"},
		{"", 503, "fixture endpoint inactive"},
		{second.URL, 200, "second:public.trycloudflare.com:/e2e-proxy/session"},
		{"http://203.0.113.1:1234", 503, "fixture endpoint inactive"},
		{fmt.Sprintf(`{"e2e-proxy":%q}`, first.URL), 200, "first:public.trycloudflare.com:/session"},
		{fmt.Sprintf(`{"other-route":%q}`, second.URL), 503, "fixture endpoint inactive"},
	} {
		require.NoError(t, os.WriteFile(path, []byte(test.target), 0600))
		req, err := http.NewRequest("GET", server.URL+"/e2e-proxy/session", nil)
		require.NoError(t, err)
		req.Host = "public.trycloudflare.com"
		resp, err := server.Client().Do(req)
		require.NoError(t, err)
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, test.status, resp.StatusCode)
		require.Contains(t, string(data), test.body)
	}
}
