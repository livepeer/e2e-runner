package e2escope

import (
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"
)

// Reuse must outlive the first fixture, use the next fixture's RPC, and avoid
// faucet or transaction calls when the already registered R is still active.
func TestPaidRecipientReuse(t *testing.T) {
	for _, name := range []string{"E2E_FUNDER_KEY_FILE", "E2E_FUNDER_ADDRESS", "E2E_LPT_FUNDER_ADDRESS"} {
		t.Setenv(name, "")
	}
	dir, err := os.MkdirTemp("", "e2e-scope-recipient-test-")
	require.NoError(t, err)
	a := &chainAccount{address: common.HexToAddress("0x1234"), runScoped: true,
		keystore: secretFile(t, dir, "account.json", "fixture"), passwordFile: secretFile(t, dir, "password", "fixture")}
	identity := recipientChain{"1", common.HexToHash("0x5678"), common.HexToAddress("0x9abc")}
	require.NotContains(t, paidRecipients.accounts, identity)
	paidRecipients.accounts[identity] = a
	t.Cleanup(func() {
		delete(paidRecipients.accounts, identity)
		require.NoError(t, os.RemoveAll(dir))
	})
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(contractABIs, &raw))
	contracts := make(map[string]abi.ABI)
	for name, data := range raw {
		contracts[name], err = abi.JSON(strings.NewReader(string(data)))
		require.NoError(t, err)
	}
	answers := make(map[string]hexutil.Bytes)
	answer := func(kind, method string, values ...any) {
		m := contracts[kind].Methods[method]
		data, err := m.Outputs.Pack(values...)
		require.NoError(t, err)
		answers[hexutil.Encode(m.ID)] = data
	}
	zero, one := new(big.Int), big.NewInt(1)
	answer("livepeerToken", "balanceOf", zero)
	answer("roundsManager", "currentRoundInitialized", true)
	answer("roundsManager", "lastInitializedRound", one)
	answer("bondingManager", "isActiveTranscoder", true)
	answer("bondingManager", "getTranscoderPoolSize", one)
	answer("bondingManager", "getFirstTranscoderInPool", a.address)
	answer("bondingManager", "getDelegator", one, zero, a.address, one, one, one, zero)
	answer("bondingManager", "getTranscoder", zero, zero, zero, one, one, zero, zero, zero, zero, zero)
	for _, name := range []string{"first fixture", "different fault proxy"} {
		t.Run(name, func(t *testing.T) {
			var pendingReads atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					ID     json.RawMessage
					Method string
					Params []json.RawMessage
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Errorf("decode RPC request: %v", err)
					return
				}
				var result any
				switch request.Method {
				case "eth_accounts":
					result = []common.Address{common.HexToAddress("0xf00d")}
				case "eth_getBalance":
					result = "0x1"
				case "eth_getTransactionCount":
					result = "0x1"
					if string(request.Params[1]) == `"pending"` && pendingReads.Add(1) == 1 {
						result = "0x2" // Drain an outstanding transaction before reuse.
					}
				case "eth_call":
					var call struct{ Data string }
					_ = json.Unmarshal(request.Params[0], &call)
					if len(call.Data) >= 10 {
						if data, ok := answers[call.Data[:10]]; ok {
							result = data
						}
					}
				}
				if result == nil {
					t.Errorf("unexpected RPC during active recipient reuse: %s %s", request.Method, request.Params)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
			}))
			t.Cleanup(srv.Close)
			client, err := rpc.DialHTTP(srv.URL)
			require.NoError(t, err)
			t.Cleanup(client.Close)
			c := &chainFixture{url: srv.URL, setupURL: srv.URL, rpc: client, chainID: one, genesis: identity.genesis, controller: identity.controller, contracts: contracts}
			c.configureFunding(t) // Zero LPT must not trigger a refill here.
			require.Same(t, a, c.paidRecipient(t, false))
			require.Same(t, a, c.cliAccounts[a.address])
			require.GreaterOrEqual(t, pendingReads.Load(), int64(2))
			require.FileExists(t, a.keystore)
			require.FileExists(t, a.passwordFile)
		})
	}
}

func TestOnchainSetupRPCIsolation(t *testing.T) {
	c := preflightChain(t)
	faults := faultRPC(t, c)
	faults.down.Store(true)
	status, _ := httpJSON(t, http.MethodPost, c.url, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []any{}}, nil)
	require.Equal(t, http.StatusServiceUnavailable, status, "fixture workers cannot reach RPC through the fault proxy")
	a := c.createAccount(t, "node") // Identity/CLI coverage keeps an isolated account.
	output, err := c.nodeCLI(t, a, false, "account", "get")
	require.NoError(t, err, "setup CLI must use the real fixture RPC: %s", output)
	var account map[string]any
	require.NoError(t, json.Unmarshal(output, &account))
	require.Equal(t, strings.ToLower(a.address.Hex()), account["address"])
	require.Equal(t, "0", account["balance_wei"])
	require.Equal(t, "0", account["lpt_balance_base_units"])
}
