package e2escope

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

func TestRedemptionRPCBarrierAndAcknowledgement(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	address, broker := crypto.PubkeyToAddress(key.PublicKey), common.HexToAddress("0x1234")
	var reads, sends atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Method == "eth_getTransactionCount" {
			reads.Add(1)
			rpcReply(w, request, "0x7", nil)
			return
		}
		var raw hexutil.Bytes
		_ = json.Unmarshal(request.Params[0], &raw)
		tx := new(types.Transaction)
		if err := tx.UnmarshalBinary(raw); err != nil {
			t.Error(err)
			return
		}
		if sends.Add(1) == 1 {
			rpcReply(w, request, tx.Hash(), nil)
		} else {
			_, _ = io.WriteString(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"replacement transaction underpriced"}}`)
		}
	}))
	t.Cleanup(upstream.Close)
	client, err := rpc.DialHTTP(upstream.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	c := &chainFixture{url: upstream.URL, rpc: client, broker: broker}
	g := newRedemptionRPC(c, 2)
	g.account = address
	g.releaseNonceReads()
	urls := make([]string, 2)
	for i := range urls {
		worker := *c
		faultRPC(t, &worker, g.hook(i))
		urls[i] = worker.url
	}
	type response struct {
		body []byte
		err  error
	}
	results := make(chan response, 2)
	for i := range urls {
		go func() {
			_, body, err := requestJSON(http.MethodPost, urls[i], map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionCount", "params": []any{address, "pending"}}, nil)
			results <- response{body, err}
		}()
	}
	for range urls {
		result := <-results
		require.NoError(t, result.err)
		require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"0x7"}`, string(result.body))
	}
	require.EqualValues(t, 2, reads.Load(), "each independent proxy must read the real upstream")
	require.Equal(t, map[int]uint64{0: 7, 1: 7}, g.firstNonces())
	// Later work must wait for its own eligibility checks, even though both
	// processes passed the initial collision barrier already.
	g.holdNonceReads()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, urls[0], strings.NewReader(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":[%q,"pending"]}`, address.Hex())))
	require.NoError(t, err)
	heldResponse, err := testHTTPClient.Do(request)
	if heldResponse != nil {
		heldResponse.Body.Close()
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 2, reads.Load(), "the later batch must not reach the chain before release")
	g.releaseNonceReads()
	_, body, err := requestJSON(http.MethodPost, urls[0], map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionCount", "params": []any{address, "pending"}}, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"jsonrpc":"2.0","id":1,"result":"0x7"}`, string(body))
	require.EqualValues(t, 3, reads.Load())
	for i := range urls {
		tx, err := types.SignTx(types.NewTransaction(7, broker, new(big.Int), 100000, big.NewInt(1), []byte{byte(i)}), types.LatestSignerForChainID(big.NewInt(1)), key)
		require.NoError(t, err)
		raw, err := tx.MarshalBinary()
		require.NoError(t, err)
		if i == 0 {
			g.mu.Lock()
			g.loseAck = true
			g.mu.Unlock()
		} else {
			g.mu.Lock()
			g.paused = false
			g.mu.Unlock()
		}
		status, body, err := requestJSON(http.MethodPost, urls[i], map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_sendRawTransaction", "params": []any{hexutil.Bytes(raw)}}, nil)
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, http.StatusServiceUnavailable, status)
		} else {
			require.Equal(t, http.StatusOK, status)
			require.Contains(t, string(body), "replacement transaction underpriced")
		}
	}
	items := g.transactions()
	require.Len(t, items, 2)
	require.True(t, items[0].Accepted)
	require.True(t, items[0].LostReply, "suppression must happen after actual upstream acceptance")
	require.False(t, items[1].Accepted)
	require.NotEqual(t, items[0].Hash, items[1].Hash)
	require.EqualValues(t, 2, sends.Load())
}

func TestRedemptionRPCBarrierCancellation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		rpcReply(w, request, "0x7", nil)
	}))
	t.Cleanup(upstream.Close)
	client, err := rpc.DialHTTP(upstream.URL)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	c := &chainFixture{url: upstream.URL, rpc: client}
	g := newRedemptionRPC(c, 2)
	g.releaseNonceReads()
	worker := *c
	faultRPC(t, &worker, g.hook(0))
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, worker.url, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["0x0000000000000000000000000000000000000000","pending"]}`))
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		response, err := testHTTPClient.Do(request)
		if response != nil {
			response.Body.Close()
		}
		result <- err
	}()
	require.Eventually(t, func() bool { return len(g.firstNonces()) == 1 }, time.Second, time.Millisecond)
	done()
	require.ErrorIs(t, <-result, context.Canceled)
	require.Equal(t, map[int]uint64{0: 7}, g.firstNonces(), "the first real response was held waiting for an absent peer")
}
