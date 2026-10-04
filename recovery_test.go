package e2escope

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

type rpcFaults struct {
	down          atomic.Bool
	finalizedDown atomic.Bool
	receiptsDown  atomic.Bool
	highFee       atomic.Bool
}

func faultRPC(t *testing.T, c *chainFixture) *rpcFaults {
	t.Helper()
	target, err := url.Parse(c.url)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	fault := &rpcFaults{}
	srv := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fault.down.Load() {
			http.Error(w, "test RPC interruption", 503)
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		r.Body = io.NopCloser(strings.NewReader(string(data)))
		var request struct {
			Method string
			ID     json.RawMessage
			Params []json.RawMessage
		}
		_ = json.Unmarshal(data, &request)
		if fault.receiptsDown.Load() && request.Method == "eth_getTransactionReceipt" {
			respondJSON(t, w, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": nil})
			return
		}
		if fault.highFee.Load() && (request.Method == "eth_gasPrice" || request.Method == "eth_maxPriorityFeePerGas") {
			respondJSON(t, w, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "0x2540be400"})
			return
		}
		if fault.finalizedDown.Load() && request.Method == "eth_getBlockByNumber" && len(request.Params) > 0 && string(request.Params[0]) == `"finalized"` {
			respondJSON(t, w, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": nil})
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	c.url = srv.URL
	return fault
}

func (c *chainFixture) mineTo(t *testing.T, target uint64) {
	t.Helper()
	start := c.block(t)
	if start > target {
		return
	}
	require.LessOrEqual(t, target-start, uint64(1000), "INFRASTRUCTURE: bounded mine distance")
	for c.block(t) <= target {
		var result any
		c.request(t, &result, "evm_mine")
	}
}

func (c *chainFixture) advanceTo(t *testing.T, target uint64) {
	t.Helper()
	var modules map[string]string
	c.request(t, &modules, "rpc_modules")
	if _, controlled := modules["evm"]; controlled {
		c.mineTo(t, target)
		return
	}
	eventually(t, func() bool { return c.block(t) > target }, envDuration(t, "E2E_BLOCK_TIMEOUT", "3m"), 500*time.Millisecond, "INFRASTRUCTURE: chain must naturally advance beyond block %d", target)
}

type observedTicket struct {
	Recipient, Sender               common.Address
	FaceValue, WinProb, SenderNonce *big.Int
	RecipientRandHash               common.Hash
	AuxData                         []byte
	Signature                       []byte
	Random                          *big.Int
	Expiry                          uint64
	Tx                              common.Hash
}

func paymentDB(orch *testOrchestrator) string {
	if orch.backend == "node" {
		return filepath.Join(orch.dir, "redeemer.sqlite")
	}
	return filepath.Join(orch.dir, "data", "lpdb.sqlite3")
}
func observedWinning(t *testing.T, orch *testOrchestrator, payer common.Address) *observedTicket {
	t.Helper()
	db := openReadDB(t, paymentDB(orch))
	defer db.Close()
	query := "SELECT recipient,payer_address,face_value,win_prob,ticket_nonce,recipient_rand_hash,creation_round,creation_round_block_hash,sig,recipient_rand,params_expiration_block,coalesce(tx_hash,'') FROM winning_tickets WHERE payer_address=? ORDER BY seq ASC LIMIT 1"
	if orch.backend == "go-livepeer" {
		query = "SELECT recipient,sender,faceValue,winProb,senderNonce,recipientRandHash,creationRound,creationRoundBlockHash,sig,recipientRand,paramsExpirationBlock,coalesce(txHash,'') FROM ticketQueue WHERE sender=? ORDER BY createdAt ASC LIMIT 1"
	}
	var recipient, sender, hash, roundHash, tx string
	var face, prob, rand, sig []byte
	var nonce, round int64
	var expiry uint64
	err := db.QueryRow(query, payer.Hex()).Scan(&recipient, &sender, &face, &prob, &nonce, &hash, &round, &roundHash, &sig, &rand, &expiry, &tx)
	if err == sql.ErrNoRows {
		return nil
	}
	require.NoError(t, err)
	aux := append(common.LeftPadBytes(big.NewInt(round).Bytes(), 32), common.HexToHash(roundHash).Bytes()...)
	return &observedTicket{Recipient: common.HexToAddress(recipient), Sender: common.HexToAddress(sender), FaceValue: new(big.Int).SetBytes(face), WinProb: new(big.Int).SetBytes(prob), SenderNonce: big.NewInt(nonce), RecipientRandHash: common.HexToHash(hash), AuxData: aux, Signature: sig, Random: new(big.Int).SetBytes(rand), Expiry: expiry, Tx: common.HexToHash(tx)}
}
func makeWinning(t *testing.T, f *paidFixture, headers map[string]string) *observedTicket {
	t.Helper()
	for range 16 {
		j := f.job(t, "fixed", "persistent")
		j.obtainChallenge(t)
		j.sign(t, headers)
		s := j.submit(t)
		stopPaidSession(t, f.orch, j.route, s.SessionID)
		j.closeRunner()
		if ticket := observedWinning(t, f.orch, f.payer.address); ticket != nil {
			return ticket
		}
	}
	t.Fatal("no observed winning ticket in 16 bounded jobs; do not assume every probabilistic ticket wins")
	return nil
}
func waitSettlement(t *testing.T, f *paidFixture, ticket *observedTicket) *types.Receipt {
	t.Helper()
	var receipt *types.Receipt
	eventually(t, func() bool {
		latest := observedWinning(t, f.orch, f.payer.address)
		if latest == nil || latest.Tx == (common.Hash{}) {
			return false
		}
		ctx, cancel := chainContext(t)
		defer cancel()
		err := f.chain.rpc.CallContext(ctx, &receipt, "eth_getTransactionReceipt", latest.Tx)
		return err == nil && receipt != nil
	}, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 300*time.Millisecond, "redemption receipt: %s", readLog(t, f.orch.logPath))
	require.Equal(t, uint64(1), receipt.Status)
	require.Greater(t, receipt.BlockNumber.Uint64(), ticket.Expiry, "randomness revealed after ticket acceptance window")
	return receipt
}

func TestOnchainRecovery(t *testing.T) {
	forEachPair(t, func(t *testing.T, orch, signer string) {
		for _, scenario := range []string{"expired parameters", "redemption and duplicate", "pending restart", "RPC interruption", "failed transaction", "fee limit", "delayed finality", "reorg recovery"} {
			t.Run(scenario, func(t *testing.T) {
				c := preflightChain(t)
				if scenario == "expired parameters" || scenario == "reorg recovery" {
					c.requireControl(t)
				}
				if orch == "node" && scenario != "expired parameters" {
					c.requireFinality(t)
				}
				faults := faultRPC(t, c)
				opts := orchestratorOptions{}
				if orch == "go-livepeer" {
					opts.goExtra = []string{"-maxFaceValue", envDefault("E2E_TICKET_FACE_VALUE", "400000000000")}
				}
				f := setupPaid(t, c, orch, signer, "", "", nil, opts)
				if scenario == "expired parameters" {
					j := f.job(t, "hour", "persistent")
					j.obtainChallenge(t)
					j.sign(t, nil)
					session := j.submit(t)
					defer stopPaidSession(t, f.orch, j.route, session.SessionID)
					params := wireBytes(t, decode64(t, j.challenge.PaymentParams), 2)
					expiry := new(big.Int).SetBytes(wireBytes(t, params, 6)).Uint64()
					c.mineTo(t, expiry+1)
					status, body := j.generate(t, nil)
					require.Equal(t, 480, status, string(body))
					assertNoCredentials(t, body)
					status, body = httpJSON(t, "POST", f.orch.httpBase+"/refresh-payment", map[string]string{"sender": f.payer.address.Hex(), "manifest_id": j.challenge.ManifestID}, nil)
					require.Equal(t, 200, status, string(body))
					j.challenge = decodeChallenge(t, body)
					j.sign(t, nil)
					status, body = httpJSON(t, "POST", f.orch.httpBase+"/apps/"+j.route+"/session/"+session.SessionID+"/payment", nil, j.headers())
					require.Equal(t, 200, status, string(body))
					requireStatus(t, "POST", session.AppURL, nil, 200)
					return
				}
				beforeDeposit, beforeReserve, _ := senderEscrow(t, c, f.payer.address)
				ticket := makeWinning(t, f, nil)
				var snapshot string
				if scenario == "reorg recovery" {
					c.request(t, &snapshot, "evm_snapshot")
				}
				if scenario == "RPC interruption" {
					faults.down.Store(true)
				}
				if (scenario == "delayed finality" || scenario == "reorg recovery") && orch == "node" {
					faults.finalizedDown.Store(true)
				}
				if scenario == "delayed finality" && orch == "go-livepeer" {
					// go-livepeer confirms from mined receipts rather than the
					// finalized tag. Hold that observation at its real boundary.
					faults.receiptsDown.Store(true)
				}
				if scenario == "fee limit" {
					faults.highFee.Store(true)
					f.orch.process.stop()
					opts := f.orch.opts
					opts.extra = map[string]any{"RedeemerMaxFeePerGas": "1"}
					if orch == "go-livepeer" {
						opts.goExtra = []string{"-maxGasPrice", "1"}
					}
					f.orch = startOrchestrator(t, opts)
				}
				if scenario == "pending restart" {
					f.orch.process.stop()
					pending := observedWinning(t, f.orch, f.payer.address)
					require.NotNil(t, pending)
					require.Equal(t, common.Hash{}, pending.Tx, "restart begins with persisted, unsubmitted redemption")
				}
				c.advanceTo(t, ticket.Expiry+2)
				if scenario == "delayed finality" && orch == "go-livepeer" {
					time.Sleep(1200 * time.Millisecond)
					pending := observedWinning(t, f.orch, f.payer.address)
					require.NotNil(t, pending)
					require.Equal(t, common.Hash{}, pending.Tx, "unobserved receipt remains pending")
					faults.receiptsDown.Store(false)
				}
				if scenario == "fee limit" || scenario == "RPC interruption" {
					// Keep the fault through several redemption polls. A restart alone
					// is not evidence that the outage/cap actually held the transaction.
					time.Sleep(1200 * time.Millisecond)
					pending := observedWinning(t, f.orch, f.payer.address)
					require.NotNil(t, pending)
					require.Equal(t, common.Hash{}, pending.Tx, "fault must leave the ticket pending")
				}
				if scenario == "pending restart" || scenario == "RPC interruption" || scenario == "fee limit" {
					f.orch.process.stop()
					faults.down.Store(false)
					faults.highFee.Store(false)
					opts := f.orch.opts
					opts.extra = nil
					opts.goExtra = nil
					f.orch = startOrchestrator(t, opts)
				}
				receipt := waitSettlement(t, f, ticket)
				if scenario == "delayed finality" && orch == "node" {
					db := openReadDB(t, paymentDB(f.orch))
					var count int
					require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets WHERE redeemed_at IS NOT NULL").Scan(&count))
					require.Zero(t, count, "mined is not finalized")
					faults.finalizedDown.Store(false)
					eventually(t, func() bool {
						err := db.QueryRow("SELECT count(*) FROM winning_tickets WHERE redeemed_at IS NOT NULL").Scan(&count)
						return err == nil && count > 0
					}, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 200*time.Millisecond)
				}
				deposit, reserve, _ := senderEscrow(t, c, f.payer.address)
				require.Negative(t, new(big.Int).Sub(new(big.Int).Add(deposit, reserve), new(big.Int).Add(beforeDeposit, beforeReserve)).Sign(), "redemption debits escrow")
				if scenario == "reorg recovery" {
					var restored bool
					c.request(t, &restored, "evm_revert", snapshot)
					require.True(t, restored)
					f.orch.process.stop()
					f.orch = startOrchestrator(t, f.orch.opts)
					c.mineTo(t, ticket.Expiry+3)
					_ = waitSettlement(t, f, ticket)
					faults.finalizedDown.Store(false)
					deposit2, reserve2, _ := senderEscrow(t, c, f.payer.address)
					require.Equal(t, new(big.Int).Add(deposit, reserve).String(), new(big.Int).Add(deposit2, reserve2).String(), "one canonical settlement after reorg")
				}
				if scenario == "redemption and duplicate" || scenario == "failed transaction" {
					data, err := c.contracts["ticketBroker"].Pack("redeemWinningTicket", ticket, ticket.Signature, ticket.Random)
					require.NoError(t, err)
					if scenario == "failed transaction" {
						ticket.Signature[0] ^= 1
						data, err = c.contracts["ticketBroker"].Pack("redeemWinningTicket", ticket, ticket.Signature, ticket.Random)
						require.NoError(t, err)
					}
					var output hexutil.Bytes
					ctx, cancel := chainContext(t)
					defer cancel()
					err = c.rpc.CallContext(ctx, &output, "eth_call", map[string]any{"from": f.recipient.address, "to": c.broker, "data": hexutil.Bytes(data)}, "latest")
					require.Error(t, err, "duplicate or invalid redemption must revert")
					failed := c.submit(t, f.recipient.address, f.recipient.key, common.Address{}, c.broker, data, new(big.Int))
					require.Equal(t, uint64(0), failed.Status, "failed redemption has an actual reverted receipt")
					d, r, _ := senderEscrow(t, c, f.payer.address)
					require.Equal(t, new(big.Int).Add(deposit, reserve).String(), new(big.Int).Add(d, r).String(), "reverted redemption cannot debit escrow twice")
					tx, err := c.rpcTransaction(t, receipt.TxHash)
					require.NoError(t, err)
					require.NotNil(t, tx)
				}
			})
		}
	})
}

func (c *chainFixture) rpcTransaction(t *testing.T, hash common.Hash) (*types.Transaction, error) {
	var tx *types.Transaction
	ctx, cancel := chainContext(t)
	defer cancel()
	err := c.rpc.CallContext(ctx, &tx, "eth_getTransactionByHash", hash)
	return tx, err
}

func TestOnchainOracle(t *testing.T) {
	forEachPair(t, func(t *testing.T, orch, signer string) {
		c := preflightChain(t)
		f := setupPaid(t, c, orch, signer, "", "", nil)
		runOracleObservations(t, f)
	})
}

func runOracleObservations(t *testing.T, f *paidFixture) {
	require.Empty(t, os.Getenv("E2E_PRICE_FEED_ADDRESS"), "INFRASTRUCTURE: oracle mutation requires the harness-owned feed")
	maxAge := envDuration(t, "E2E_ORACLE_TEST_MAX_AGE", "45s")
	timeout := envDuration(t, "E2E_ORACLE_TIMEOUT", "90s")
	poll := envDuration(t, "E2E_ORACLE_POLL_INTERVAL", "1h")
	require.Positive(t, maxAge)
	require.Greater(t, timeout, maxAge, "INFRASTRUCTURE: oracle timeout must cover cached-observation expiry")
	signerBackend := f.signer.backend
	f.signer.process.stop()
	opts := f.orch.opts
	opts.useOracle = true
	opts.staticConfigPath = writeStaticConfig(t, staticRunnerConfig{Runners: []staticRunnerConfigEntry{{Label: "oracle-runner", Routing: "label", RunnerURL: f.upstream.url, HealthURL: f.upstream.url + "/health", App: liveRunnerApp, Metadata: "oracle-runner", Mode: "persistent", Capacity: 2, PriceInfo: priceInfo{Price: "0.001", Currency: "usd", Unit: "fixed"}}}})
	reset := func(t *testing.T) {
		f.chain.setOracle(t, f.payer, "setAnswer", big.NewInt(1000_00000000))
		f.chain.setOracle(t, f.payer, "setUpdatedAt", big.NewInt(time.Now().Unix()))
	}
	restartOrch := func(t *testing.T, age time.Duration) {
		f.orch.process.stop()
		current := opts
		current.extra = nil
		if current.backend == "node" {
			current.extra = map[string]any{"PriceMaxAge": age.String()}
		}
		f.orch = startOrchestrator(t, current)
		waitRunnerByMetadata(t, f.orch, "oracle-runner")
	}
	normalAge := envDuration(t, "E2E_ORACLE_MAX_AGE", "2h")
	runtimeBudget := func(t *testing.T) {
		t.Helper()
		require.Greater(t, timeout, poll, "INFRASTRUCTURE: runtime oracle coverage needs E2E_ORACLE_TIMEOUT above E2E_ORACLE_POLL_INTERVAL=%s; increase the suite timeout too", poll)
		require.Greater(t, normalAge, poll, "INFRASTRUCTURE: the valid observation must remain usable through a complete poll interval")
	}
	t.Run("changed observation runtime", func(t *testing.T) {
		runtimeBudget(t)
		reset(t)
		restartOrch(t, normalAge)
		before := waitRunnerByMetadata(t, f.orch, "oracle-runner").PriceInfo.Price
		f.chain.setOracle(t, f.payer, "setAnswer", big.NewInt(2000_00000000))
		eventually(t, func() bool {
			for _, entry := range discover(t, f.orch) {
				for _, runner := range entry.Runners {
					if runner.Metadata == "oracle-runner" && runner.PriceInfo != nil && runner.PriceInfo.Price != before {
						require.Equal(t, "500000000000", runner.PriceInfo.Price.String())
						return true
					}
				}
			}
			return false
		}, timeout, time.Second, "changed observation reprices discovery")
	})
	t.Run("changed observation after restart", func(t *testing.T) {
		reset(t)
		restartOrch(t, normalAge)
		require.Equal(t, "1000000000000", waitRunnerByMetadata(t, f.orch, "oracle-runner").PriceInfo.Price.String())
		f.chain.setOracle(t, f.payer, "setAnswer", big.NewInt(2000_00000000))
		restartOrch(t, normalAge)
		require.Equal(t, "500000000000", waitRunnerByMetadata(t, f.orch, "oracle-runner").PriceInfo.Price.String())
	})
	for _, observation := range []string{"invalid observation", "stale observation"} {
		t.Run(observation, func(t *testing.T) {
			reset(t)
			// Restart with a fresh valid observation before each expiry fixture.
			// Recovery from a previous invalid observation is a separate runtime
			// polling requirement, not a prerequisite for independent cases.
			restartOrch(t, maxAge)
			if observation == "invalid observation" {
				f.chain.setOracle(t, f.payer, "setAnswer", new(big.Int))
			} else {
				f.chain.setOracle(t, f.payer, "setUpdatedAt", big.NewInt(1))
			}
			waitOracleAbsent(t, f.orch, "oracle-runner")
			status, body := httpJSON(t, "POST", f.orch.httpBase+"/apps/oracle-runner/session", nil, map[string]string{"Livepeer-Payer-Address": f.payer.address.Hex()})
			require.GreaterOrEqual(t, status, 400, string(body))
			var challenge paymentChallenge
			_ = json.Unmarshal(body, &challenge)
			require.Empty(t, challenge.PaymentParams, "unavailable oracle must not issue usable payment parameters")
		})
	}
	t.Run("signer oracle policy", func(t *testing.T) {
		start := func(t *testing.T, age time.Duration) *testSigner {
			return startSigner(t, signerOptions{backend: signerBackend, chain: f.chain, account: f.payer, orch: f.orch, useOracle: true, maxPriceWei: "0.001USD", extra: map[string]any{"MaxFixedPrice": "0.001", "ETHUSDMaxAge": age.String()}})
		}
		prepare := func(t *testing.T, age time.Duration) (*testSigner, *paymentJob) {
			reset(t)
			restartOrch(t, normalAge)
			signer := start(t, age)
			t.Cleanup(signer.process.stop)
			j := &paymentJob{signer: signer, orch: f.orch, route: "oracle-runner", mode: "persistent", unit: "fixed", app: liveRunnerApp}
			j.obtainChallenge(t)
			j.sign(t, nil)
			if signer.backend == "node" {
				status, body := httpJSON(t, "GET", signer.metricsBase+"/readyz", nil, nil)
				require.Equal(t, 200, status, string(body))
			}
			return signer, j
		}
		ceilingProbe := func(t *testing.T, signer *testSigner, quotedPrice []byte) (int, []byte) {
			// Direct negative fixture: retain the original quote while obtaining
			// fresh parameters so expiry/nonce limits cannot mask conversion.
			probe := &paymentJob{signer: signer, orch: f.orch, route: "oracle-runner", mode: "persistent", unit: "fixed", app: liveRunnerApp}
			probe.obtainChallenge(t)
			req := probe.request()
			req["orchestrator"] = base64.StdEncoding.EncodeToString(replaceWire(t, decode64(t, probe.challenge.PaymentParams), 3, protowire.BytesType, quotedPrice))
			return httpJSON(t, "POST", signer.base+"/generate-live-payment", req, nil)
		}
		t.Run("changed ceiling runtime", func(t *testing.T) {
			runtimeBudget(t)
			signer, j := prepare(t, normalAge)
			quotedPrice := wireBytes(t, decode64(t, j.challenge.PaymentParams), 3)
			f.chain.setOracle(t, f.payer, "setAnswer", big.NewInt(2000_00000000))
			eventually(t, func() bool {
				status, body := ceilingProbe(t, signer, quotedPrice)
				if status == 481 {
					assertNoCredentials(t, body)
					return true
				}
				require.Equal(t, 200, status, string(body))
				return false
			}, timeout, 5*time.Second, "oracle conversion updates signer ceiling")
		})
		t.Run("changed ceiling after restart", func(t *testing.T) {
			signer, j := prepare(t, normalAge)
			quotedPrice := wireBytes(t, decode64(t, j.challenge.PaymentParams), 3)
			f.chain.setOracle(t, f.payer, "setAnswer", big.NewInt(2000_00000000))
			signer.process.stop()
			// The same backend and payer key remain selected for this job.
			signer = start(t, normalAge)
			status, body := ceilingProbe(t, signer, quotedPrice)
			require.Equal(t, 481, status, string(body))
			assertNoCredentials(t, body)
		})
		for _, invalid := range []string{"invalid", "stale"} {
			t.Run(invalid, func(t *testing.T) {
				signer, probe := prepare(t, maxAge)
				if invalid == "invalid" {
					f.chain.setOracle(t, f.payer, "setAnswer", new(big.Int))
				} else {
					f.chain.setOracle(t, f.payer, "setUpdatedAt", big.NewInt(1))
				}
				if signer.backend == "node" {
					eventually(t, func() bool {
						status, _ := httpJSON(t, "GET", signer.metricsBase+"/readyz", nil, nil)
						return status == 503
					}, timeout, time.Second, "cached oracle observation expires before rejection")
				} else {
					time.Sleep(maxAge)
				}
				// Extend only ticket expiry to distinguish oracle rejection from
				// an unrelated parameter-refresh error; this job is not admitted.
				info := decode64(t, probe.challenge.PaymentParams)
				params := wireBytes(t, info, 2)
				params = replaceWire(t, params, 6, protowire.BytesType, new(big.Int).SetUint64(f.chain.block(t)+1000).Bytes())
				probe.challenge.PaymentParams = base64.StdEncoding.EncodeToString(replaceWire(t, info, 2, protowire.BytesType, params))
				status, body := probe.generate(t, nil)
				require.Contains(t, []int{500, 502, 503}, status, string(body))
				assertNoCredentials(t, body)
			})
		}
	})
}

func waitOracleAbsent(t *testing.T, orch *testOrchestrator, metadata string) {
	t.Helper()
	eventually(t, func() bool {
		for _, entry := range discover(t, orch) {
			for _, runner := range entry.Runners {
				if runner.Metadata == metadata {
					return false
				}
			}
		}
		return true
	}, envDuration(t, "E2E_ORACLE_TIMEOUT", "90s"), 300*time.Millisecond, "invalid/stale oracle must withdraw priced discovery")
}
