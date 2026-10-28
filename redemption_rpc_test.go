package e2escope

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
)

// The barrier holds genuine pending-nonce responses, rather than inventing a
// stale value or coordinating the processes' independent nonce allocators.
type redemptionRPC struct {
	chain   *chainFixture
	account common.Address
	ready   chan struct{}
	all     chan struct{}
	winner  chan struct{}
	lost    chan struct{}
	resume  chan struct{}
	count   int

	mu        sync.Mutex
	nonces    map[int]uint64
	sends     []redemptionSend
	first     map[int]bool
	finalized int
	loseAck   bool
	paused    bool
	external  func(context.Context, *types.Transaction) error
}

type redemptionSend struct {
	Process   int            `json:"process"`
	Hash      common.Hash    `json:"hash"`
	Nonce     uint64         `json:"nonce"`
	From      common.Address `json:"from"`
	To        common.Address `json:"to"`
	FeeCap    string         `json:"fee_cap"`
	TipCap    string         `json:"tip_cap"`
	Calldata  common.Hash    `json:"calldata_keccak256"`
	Accepted  bool           `json:"rpc_accepted"`
	LostReply bool           `json:"reply_suppressed"`
	Error     string         `json:"error,omitempty"`
	Receipt   *types.Receipt `json:"receipt,omitempty"`
	tx        *types.Transaction
}

func newRedemptionRPC(c *chainFixture, count int) *redemptionRPC {
	return &redemptionRPC{chain: c, count: count, ready: make(chan struct{}), all: make(chan struct{}),
		winner: make(chan struct{}), lost: make(chan struct{}), resume: make(chan struct{}),
		nonces: make(map[int]uint64), first: make(map[int]bool)}
}

func (g *redemptionRPC) holdNonceReads() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ready = make(chan struct{})
}

func (g *redemptionRPC) releaseNonceReads() {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.ready)
}

func awaitRPC(ctx context.Context, gate <-chan struct{}) error {
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func rpcReply(w http.ResponseWriter, request rpcRequest, result any, err error) {
	response := map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result}
	if err != nil {
		code := -32000
		var remote rpc.Error
		if errors.As(err, &remote) {
			code = remote.ErrorCode()
		}
		delete(response, "result")
		response["error"] = map[string]any{"code": code, "message": err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (g *redemptionRPC) hook(process int) rpcHook {
	return func(w http.ResponseWriter, r *http.Request, request rpcRequest) bool {
		ctx := r.Context()
		if request.Method == "eth_getBlockByNumber" && len(request.Params) > 0 && string(request.Params[0]) == `"finalized"` {
			g.mu.Lock()
			g.finalized++
			g.mu.Unlock()
		}
		if request.Method == "eth_getTransactionCount" && len(request.Params) == 2 && string(request.Params[1]) == `"pending"` {
			var address common.Address
			if json.Unmarshal(request.Params[0], &address) != nil || address != g.account {
				return false
			}
			var nonce hexutil.Uint64
			g.mu.Lock()
			ready := g.ready
			g.mu.Unlock()
			err := awaitRPC(ctx, ready)
			if err == nil {
				err = g.chain.rpc.CallContext(ctx, &nonce, request.Method, address, "pending")
			}
			if err == nil {
				g.mu.Lock()
				if _, seen := g.nonces[process]; !seen {
					g.nonces[process] = uint64(nonce)
					if len(g.nonces) == g.count {
						close(g.all)
					}
				}
				g.mu.Unlock()
				err = awaitRPC(ctx, g.all)
			}
			rpcReply(w, request, nonce, err)
			return true
		}
		if request.Method != "eth_sendRawTransaction" || len(request.Params) != 1 {
			return false
		}
		var raw hexutil.Bytes
		tx := new(types.Transaction)
		if json.Unmarshal(request.Params[0], &raw) != nil || tx.UnmarshalBinary(raw) != nil {
			return false
		}
		from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
		if err != nil || from != g.account || tx.To() == nil || *tx.To() != g.chain.broker {
			return false
		}
		g.mu.Lock()
		first, paused := !g.first[process], g.paused
		g.first[process] = true
		g.mu.Unlock()
		if paused {
			err = awaitRPC(ctx, g.resume)
		}
		if err == nil && first && process != 0 {
			err = awaitRPC(ctx, g.winner)
		}
		if err == nil && first && g.external != nil {
			err = g.external(ctx, tx)
		}
		var hash common.Hash
		if err == nil {
			err = g.chain.rpc.CallContext(ctx, &hash, request.Method, raw)
		}
		accepted := err == nil && hash == tx.Hash()
		g.mu.Lock()
		lost := accepted && g.loseAck
		if lost {
			g.loseAck, g.paused = false, true
		}
		item := redemptionSend{Process: process, Hash: tx.Hash(), Nonce: tx.Nonce(), From: from, To: *tx.To(),
			FeeCap: tx.GasFeeCap().String(), TipCap: tx.GasTipCap().String(), Calldata: crypto.Keccak256Hash(tx.Data()),
			Accepted: accepted, LostReply: lost, tx: tx}
		if err != nil {
			item.Error = err.Error()
		}
		g.sends = append(g.sends, item)
		if first && process == 0 {
			close(g.winner)
		}
		if lost {
			close(g.lost)
		}
		g.mu.Unlock()
		if lost {
			http.Error(w, "injected loss of acknowledged submission response", http.StatusServiceUnavailable)
		} else {
			rpcReply(w, request, hash, err)
		}
		return true
	}
}

func (g *redemptionRPC) transactions() []redemptionSend {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]redemptionSend(nil), g.sends...)
}

func (g *redemptionRPC) firstNonces() map[int]uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	copy := make(map[int]uint64, len(g.nonces))
	for process, nonce := range g.nonces {
		copy[process] = nonce
	}
	return copy
}

// A nil receipt is a pending observation, never a successful settlement.
func awaitRedemptionReceipt(ctx context.Context, c *chainFixture, hash common.Hash) (*types.Receipt, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var receipt *types.Receipt
		if err := c.rpc.CallContext(ctx, &receipt, "eth_getTransactionReceipt", hash); err != nil {
			return nil, err
		} else if receipt != nil {
			return receipt, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("receipt %s: %w", hash, ctx.Err())
		case <-ticker.C:
		}
	}
}
