// e2e-bridge carries the unchanged Kafka byte stream over a Quick Tunnel WSS
// connection. The client binds only loopback; TLS verifies the Cloudflare edge.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/segmentio/kafka-go/protocol/findcoordinator"
	"github.com/segmentio/kafka-go/protocol/metadata"
)

func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(header[:])
	if n > 64<<20 {
		return nil, fmt.Errorf("Kafka frame exceeds 64 MiB")
	}
	frame := make([]byte, 4+int(n))
	copy(frame, header[:])
	_, err := io.ReadFull(r, frame[4:])
	return frame, err
}

type wsStream struct {
	ws     *websocket.Conn
	reader io.Reader
}

func (s *wsStream) Read(buf []byte) (int, error) {
	for {
		if s.reader == nil {
			typ, reader, err := s.ws.NextReader()
			if err != nil {
				return 0, err
			}
			if typ != websocket.BinaryMessage {
				return 0, fmt.Errorf("expected binary stream")
			}
			s.reader = reader
		}
		n, err := s.reader.Read(buf)
		if err == io.EOF {
			s.reader = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

// Kafka metadata must advertise the local WSS client. Otherwise its producer
// would discover the original broker and bypass the encrypted transport.
func rewriteResponse(frame []byte, key protocol.ApiKey, version int16, address string) ([]byte, error) {
	if key != protocol.Metadata && key != protocol.FindCoordinator {
		return frame, nil
	}
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		return nil, err
	}
	id, message, err := protocol.ReadResponse(bytes.NewReader(frame), key, version)
	if err != nil {
		return nil, err
	}
	switch response := message.(type) {
	case *metadata.Response:
		for i := range response.Brokers {
			response.Brokers[i].Host, response.Brokers[i].Port = host, int32(port)
		}
	case *findcoordinator.Response:
		response.Host, response.Port = host, int32(port)
	}
	var out bytes.Buffer
	err = protocol.WriteResponse(&out, version, id, message)
	return out.Bytes(), err
}

func kafkaRelay(tcp net.Conn, ws *websocket.Conn, address string) {
	defer tcp.Close()
	defer ws.Close()
	type request struct {
		key     protocol.ApiKey
		version int16
	}
	var mu sync.Mutex
	pending := map[uint32]request{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer ws.Close()
		for {
			frame, err := readFrame(tcp)
			if err != nil {
				return
			}
			if len(frame) < 12 {
				return
			}
			id := binary.BigEndian.Uint32(frame[8:12])
			mu.Lock()
			pending[id] = request{protocol.ApiKey(binary.BigEndian.Uint16(frame[4:6])), int16(binary.BigEndian.Uint16(frame[6:8]))}
			mu.Unlock()
			if err := ws.WriteMessage(websocket.BinaryMessage, frame); err != nil {
				return
			}
		}
	}()
	stream := &wsStream{ws: ws}
	for {
		frame, err := readFrame(stream)
		if err != nil || len(frame) < 8 {
			break
		}
		id := binary.BigEndian.Uint32(frame[4:8])
		mu.Lock()
		req, ok := pending[id]
		delete(pending, id)
		mu.Unlock()
		if !ok {
			break
		}
		frame, err = rewriteResponse(frame, req.key, req.version, address)
		if err != nil {
			log.Print(err)
			break
		}
		if _, err := io.Copy(tcp, bytes.NewReader(frame)); err != nil {
			break
		}
	}
	tcp.Close()
	<-done
}

func gatewayHandler(targetFile string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := os.ReadFile(targetFile)
		origin := strings.TrimSpace(string(data))
		if strings.HasPrefix(origin, "{") {
			var routes map[string]string
			if e := json.Unmarshal(data, &routes); e != nil {
				http.Error(w, "invalid control routes", 503)
				return
			}
			id, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
			origin = routes[id]
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/"+id)
			if r.URL.RawPath != "" {
				r.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, "/"+id)
			}
			if r.URL.Path == "" {
				r.URL.Path = "/"
			}
		}
		u, parseErr := url.Parse(origin)
		if err != nil || parseErr != nil || u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback() {
			http.Error(w, "fixture endpoint inactive", http.StatusServiceUnavailable)
			return
		}
		proxy := httputil.NewSingleHostReverseProxy(u)
		// Preserve the public Host for native proxy-template matching.
		proxy.FlushInterval = -1
		proxy.ServeHTTP(w, r)
	})
}

func relay(tcp net.Conn, ws *websocket.Conn) {
	defer tcp.Close()
	defer ws.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer ws.Close()
		buf := make([]byte, 32*1024)
		for {
			n, err := tcp.Read(buf)
			if n > 0 {
				if e := ws.WriteMessage(websocket.BinaryMessage, buf[:n]); e != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		typ, reader, err := ws.NextReader()
		if err != nil {
			break
		}
		if typ != websocket.BinaryMessage {
			break
		}
		if _, err := io.Copy(tcp, reader); err != nil {
			break
		}
	}
	tcp.Close()
	wg.Wait()
}

func main() {
	mode := flag.String("mode", "server", "server, client, or proxy")
	listen := flag.String("listen", "", "loopback listener")
	target := flag.String("target", "", "upstream TCP address or TLS URL")
	targetFile := flag.String("target-file", "", "gateway's current loopback origin")
	flag.Parse()
	switch *mode {
	case "server":
		upgrader := websocket.Upgrader{}
		log.Fatal(http.ListenAndServe(*listen, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tcp, err := net.Dial("tcp", *target)
			if err != nil {
				http.Error(w, err.Error(), 502)
				return
			}
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				tcp.Close()
				return
			}
			relay(tcp, ws)
		})))
	case "client":
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			log.Fatal(err)
		}
		for {
			tcp, err := ln.Accept()
			if err != nil {
				log.Fatal(err)
			}
			go func() {
				ws, _, err := websocket.DefaultDialer.Dial(*target, nil)
				if err != nil {
					log.Print(err)
					tcp.Close()
					return
				}
				kafkaRelay(tcp, ws, *listen)
			}()
		}
	case "proxy":
		u, err := url.Parse(*target)
		if err != nil || u.Scheme != "https" {
			log.Fatal("proxy target must use HTTPS")
		}
		proxy := httputil.NewSingleHostReverseProxy(u)
		original := proxy.Director
		proxy.Director = func(r *http.Request) { original(r); r.Host = u.Host }
		log.Fatal(http.ListenAndServe(*listen, proxy))
	case "gateway":
		// A stable Quick Tunnel can serve successive isolated fixtures without
		// provisioning a new hostname for every registration or restart.
		log.Fatal(http.ListenAndServe(*listen, gatewayHandler(*targetFile)))
	default:
		log.Fatal("unknown mode")
	}
}
