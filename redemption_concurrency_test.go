package e2escope

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redemptionArtifact(t *testing.T, value any) {
	t.Helper()
	dir := envDefault("E2E_ARTIFACT_DIR", "artifacts")
	require.NoError(t, os.MkdirAll(dir, 0700))
	name := strings.ReplaceAll(t.Name(), "/", "_") + "-" + time.Now().UTC().Format("20060102T150405.000000000") + ".json"
	data, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0600))
	t.Logf("redemption evidence: %s", filepath.Join(dir, name))
}

func redemptionCapabilities(t *testing.T, c *chainFixture) map[string]string {
	t.Helper()
	ctx, cancel := chainContext(t)
	defer cancel()
	results := make(map[string]string)
	probe := func(name, method string, out any, args ...any) bool {
		err := c.rpc.CallContext(ctx, out, method, args...)
		results[name] = ""
		if err != nil {
			results[name] = err.Error()
		}
		return err == nil
	}
	var count hexutil.Uint64
	probe("pending_nonce", "eth_getTransactionCount", &count, common.Address{}, "pending")
	var pool json.RawMessage
	probe("txpool", "txpool_status", &pool)
	var raw json.RawMessage
	if probe("canonical_header", "eth_getBlockByNumber", &raw, "latest", false) {
		var header types.Header
		var reported struct{ Hash common.Hash }
		if err := json.Unmarshal(raw, &header); err != nil {
			results["canonical_header"] = err.Error()
		} else if json.Unmarshal(raw, &reported) != nil || reported.Hash != header.Hash() {
			results["canonical_header"] = "reported hash differs from Ethereum RLP header hash"
		}
		if reported.Hash != (common.Hash{}) {
			ref := map[string]any{"blockHash": reported.Hash, "requireCanonical": true}
			probe("canonical_account_nonce", "eth_getTransactionCount", &count, common.Address{}, ref)
			var value hexutil.Bytes
			data := append(crypto.Keccak256([]byte("usedTickets(bytes32)"))[:4], make([]byte, 32)...)
			probe("canonical_contract_read", "eth_call", &value, map[string]any{"to": c.broker, "data": hexutil.Bytes(data)}, ref)
		}
	}
	if probe("finalized", "eth_getBlockByNumber", &raw, "finalized", false) {
		var header struct {
			Hash   common.Hash
			Number *hexutil.Big
		}
		if json.Unmarshal(raw, &header) != nil || header.Hash == (common.Hash{}) || header.Number == nil {
			results["finalized"] = "no valid finalized header"
		} else {
			probe("finalized_nonce", "eth_getTransactionCount", &count, common.Address{}, map[string]any{"blockHash": header.Hash, "requireCanonical": true})
		}
	}
	var result any
	if probe("mining_control", "miner_stop", &result) {
		// Probe only this suite's disposable chain, before any workers exist.
		require.NoError(t, c.rpc.CallContext(ctx, &result, "miner_start", 1), "restore devnet mining after capability probe")
	}
	var snapshot string
	if probe("snapshot_reorg", "evm_snapshot", &snapshot) {
		var restored bool
		require.NoError(t, c.rpc.CallContext(ctx, &restored, "evm_revert", snapshot))
		require.True(t, restored)
	}
	// A nonexistent snapshot checks method availability without reverting state.
	probe("reorg_control", "evm_revert", &result, "0xdeadbeef")
	probe("drop_transaction", "txpool_dropTransaction", &result, common.Hash{})
	redemptionArtifact(t, results)
	return results
}

func requireRedemptionCapabilities(t *testing.T, capabilities map[string]string, names ...string) {
	t.Helper()
	for _, name := range names {
		if message, exists := capabilities[name]; !exists || message != "" {
			t.Skipf("INFRASTRUCTURE: blocked %s: %s", name, message)
		}
	}
}

type redemptionGroup struct {
	chain   *chainFixture
	rpc     *redemptionRPC
	members []*paidFixture
	tickets []*observedTicket
	escrow  map[common.Address]*big.Int
}

func newRedemptionGroup(t *testing.T, c *chainFixture, backends []string, duplicate bool) *redemptionGroup {
	t.Helper()
	c.configureFunding(t)
	wallet := c.paidRecipient(t, false)
	c.setupFeed(t, wallet)
	g := &redemptionGroup{chain: c, rpc: newRedemptionRPC(c, len(backends)), escrow: make(map[common.Address]*big.Int)}
	g.rpc.account = wallet.address
	t.Cleanup(func() { close(g.rpc.resume) })
	upstream := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(t, w, map[string]any{"paid": true, "prompt": "hello"})
	}))
	for process, backend := range backends {
		workerChain := *c
		faultRPC(t, &workerChain, g.rpc.hook(process))
		opts := orchestratorOptions{backend: backend, account: wallet, chain: &workerChain,
			extra: map[string]any{"RedeemerTxTimeout": "3s", "RedeemerMaxTxReplacements": 1},
			// The nonce barrier can outlive the state used by gas estimation.
			// Leave gas headroom so that state changes cannot mimic a nonce bug.
			goExtra: []string{"-maxFaceValue", envDefault("E2E_TICKET_FACE_VALUE", "400000000000"), "-gasLimit", "6000000", "-transactionTimeout", "3s", "-maxTransactionReplacements", "1"}}
		f := &paidFixture{owner: t, chain: c, recipient: wallet, redeemer: wallet, upstream: &httptestServerCount{url: upstream.URL}}
		f.orch = startOrchestrator(t, opts)
		g.members = append(g.members, f)
	}
	// Finish account/signing setup before synchronizing the whole batch.
	// Otherwise setup can cross a round boundary before ticket issuance.
	for i, f := range g.members {
		if !duplicate || i == 0 {
			g.preparePayer(t, f)
		}
	}
	c.waitUnlockedRound(t, wallet)
	for i, f := range g.members {
		if duplicate && i > 0 {
			g.copyTicket(t, i)
		} else {
			g.tickets = append(g.tickets, g.newWork(t, f))
		}
	}
	t.Cleanup(func() {
		status := "pass"
		if t.Failed() {
			status = "fail"
		}
		states := make([]any, len(g.members))
		for i, f := range g.members {
			db := openReadDB(t, paymentDB(f.orch))
			query := "SELECT coalesce(tx_hash,''),coalesce(phase,'queued') FROM winning_tickets LEFT JOIN redemption_attempts USING(sig)"
			if f.orch.backend == "go-livepeer" {
				query = "SELECT coalesce(txHash,''),case when redeemedAt IS NULL then 'queued' else 'redeemed' end FROM ticketQueue"
			}
			rows, err := db.Query(query)
			if err == nil {
				var values []map[string]string
				for rows.Next() {
					var hash, phase string
					if err = rows.Scan(&hash, &phase); err != nil {
						break
					}
					values = append(values, map[string]string{"hash": hash, "phase": phase})
				}
				states[i] = values
				err = errors.Join(err, rows.Err(), rows.Close())
			}
			if err != nil {
				states[i] = err.Error()
			}
			_ = db.Close()
		}
		sends := g.rpc.transactions()
		for i := range sends {
			ctx, cancel := chainContext(t)
			_ = c.rpc.CallContext(ctx, &sends[i].Receipt, "eth_getTransactionReceipt", sends[i].Hash)
			cancel()
		}
		redemptionArtifact(t, map[string]any{"status": status, "first_nonces": g.rpc.firstNonces(), "submissions": sends, "databases": states})
	})
	return g
}

// Seed a second independent store with the same genuine winning ticket, as can
// happen when a replica starts from a backup. Never manufacture a signature or
// claim that two unrelated payment requests generated the same ticket.
func (g *redemptionGroup) copyTicket(t *testing.T, index int) {
	t.Helper()
	f, first, ticket := g.members[index], g.members[0], g.tickets[0]
	db, err := sql.Open("sqlite", paymentDB(f.orch))
	require.NoError(t, err)
	defer db.Close()
	query := "INSERT INTO winning_tickets(recipient,payer_address,face_value,win_prob,ticket_nonce,recipient_rand_hash,creation_round,creation_round_block_hash,sig,recipient_rand,params_expiration_block) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
	if f.orch.backend == "go-livepeer" {
		query = "INSERT INTO ticketQueue(recipient,sender,faceValue,winProb,senderNonce,recipientRandHash,creationRound,creationRoundBlockHash,sig,recipientRand,paramsExpirationBlock) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
	}
	_, err = db.Exec(query, ticket.Recipient.Hex(), ticket.Sender.Hex(), ticket.FaceValue.Bytes(), ticket.WinProb.Bytes(), ticket.SenderNonce.Int64(), ticket.RecipientRandHash.Hex(), new(big.Int).SetBytes(ticket.AuxData[:32]).Int64(), common.BytesToHash(ticket.AuxData[32:]).Hex(), ticket.Signature, ticket.Random.Bytes(), fmt.Sprint(ticket.Expiry))
	require.NoError(t, err)
	f.payer = first.payer
	f.signer = startSigner(t, signerOptions{backend: "node", chain: g.chain, account: f.payer, orch: f.orch})
	// go-livepeer initializes a persisted sender's in-memory queue on demand.
	j := f.job(t, "fixed", "persistent")
	j.obtainChallenge(t)
	j.closeRunner()
	g.tickets = append(g.tickets, ticket)
}

func (g *redemptionGroup) preparePayer(t *testing.T, f *paidFixture) {
	t.Helper()
	f.payer = g.chain.newAccount(t, true)
	g.chain.fundPayer(t, f.payer, ethAmount(t, "E2E_PAYER_DEPOSIT_ETH", "0.1"), ethAmount(t, "E2E_PAYER_RESERVE_ETH", "0.1"))
	deposit, reserve, _ := senderEscrow(t, g.chain, f.payer.address)
	g.escrow[f.payer.address] = new(big.Int).Add(deposit, reserve)
	f.signer = startSigner(t, signerOptions{backend: "node", chain: g.chain, account: f.payer, orch: f.orch})
}

func (g *redemptionGroup) newWork(t *testing.T, f *paidFixture) *observedTicket {
	t.Helper()
	ticket := makeWinning(t, f, nil, func(j *paymentJob) {
		// Chain initialization and a backend's watcher cache are independent.
		// Do not pay using the previous round while waiting for that cache.
		eventually(t, func() bool {
			g.chain.initializeRound(t, f.payer)
			j.obtainChallenge(t)
			params := wireBytes(t, decode64(t, j.challenge.PaymentParams), 2)
			expiration := wireBytes(t, params, 7)
			advertised := wireVarint(t, expiration, 1)
			current := g.chain.call(t, "roundsManager", g.chain.rounds, "currentRound")[0].(*big.Int)
			return advertised == current.Uint64()
		}, envDuration(t, "E2E_BLOCK_TIMEOUT", "3m"), 200*time.Millisecond, "INFRASTRUCTURE: backend must advertise the initialized current round before payment")
	})
	db := openReadDB(t, paymentDB(f.orch))
	defer db.Close()
	query := "SELECT count(*) FROM winning_tickets WHERE payer_address=?"
	if f.orch.backend == "go-livepeer" {
		query = "SELECT count(*) FROM ticketQueue WHERE sender=?"
	}
	var count int
	require.NoError(t, db.QueryRow(query, f.payer.address.Hex()).Scan(&count))
	require.Equal(t, 1, count, "fixture must generate one winner for exact receipt and escrow attribution")
	return ticket
}

func (g *redemptionGroup) prepare(t *testing.T, tickets ...*observedTicket) {
	t.Helper()
	if len(tickets) == 0 {
		tickets = g.tickets
	}
	var expiry uint64
	for _, ticket := range tickets {
		expiry = max(expiry, ticket.Expiry)
	}
	g.chain.advanceTo(t, expiry+1)
	waitRedemptionRound(t, g.chain, g.members[0].payer)
	var balance, gasPrice hexutil.Big
	g.chain.request(t, &balance, "eth_getBalance", g.rpc.account, "latest")
	g.chain.request(t, &gasPrice, "eth_gasPrice")
	budget := new(big.Int).Mul((*big.Int)(&gasPrice), big.NewInt(int64(6000000*4*len(g.members))))
	require.GreaterOrEqual(t, (*big.Int)(&balance).Cmp(budget), 0, "INFRASTRUCTURE: shared wallet must cover redemption and replacement gas")
	for i, ticket := range tickets {
		t.Logf("ticket validity: process=%d creation_round=%s current_round=%s expiry=%d block=%d", i, new(big.Int).SetBytes(ticket.AuxData[:32]), g.chain.call(t, "roundsManager", g.chain.rounds, "currentRound")[0], ticket.Expiry, g.chain.block(t))
		require.True(t, g.chain.recipientActive(t, g.members[i].recipient))
		data, err := g.chain.contracts["ticketBroker"].Pack("redeemWinningTicket", ticket, ticket.Signature, ticket.Random)
		require.NoError(t, err)
		var output hexutil.Bytes
		g.chain.request(t, &output, "eth_call", map[string]any{"from": g.rpc.account, "to": g.chain.broker, "data": hexutil.Bytes(data)}, "latest")
		deposit, reserve, _ := senderEscrow(t, g.chain, ticket.Sender)
		require.GreaterOrEqual(t, new(big.Int).Add(deposit, reserve).Cmp(ticket.FaceValue), 0)
	}
}

// A fee replacement needs several seconds, but waiting for the first quarter
// of a later round can expire a perfectly valid ticket on the short devnet.
func waitRedemptionRound(t *testing.T, c *chainFixture, account *chainAccount) {
	t.Helper()
	length := c.call(t, "roundsManager", c.rounds, "roundLength")[0].(*big.Int)
	require.Greater(t, length.Int64(), int64(10), "INFRASTRUCTURE: rounds need room for the configured recovery timeouts")
	eventually(t, func() bool {
		c.initializeRound(t, account)
		start := c.call(t, "roundsManager", c.rounds, "currentRoundStartBlock")[0].(*big.Int)
		remaining := new(big.Int).Sub(new(big.Int).Add(start, length), new(big.Int).SetUint64(c.block(t)))
		return remaining.Cmp(big.NewInt(10)) >= 0 && c.call(t, "roundsManager", c.rounds, "currentRoundInitialized")[0].(bool)
	}, envDuration(t, "E2E_BLOCK_TIMEOUT", "3m"), 200*time.Millisecond, "INFRASTRUCTURE: need an initialized round with recovery headroom")
}

func (g *redemptionGroup) settle(t *testing.T, index int, ticket *observedTicket) *types.Receipt {
	t.Helper()
	f := g.members[index]
	data, err := g.chain.contracts["ticketBroker"].Pack("redeemWinningTicket", ticket, ticket.Signature, ticket.Random)
	require.NoError(t, err)
	var mined *types.Receipt
	eventually(t, func() bool {
		for _, send := range g.rpc.transactions() {
			if send.Process != index || send.Calldata != crypto.Keccak256Hash(data) {
				continue
			}
			var receipt *types.Receipt
			ctx, cancel := chainContext(t)
			err := g.chain.rpc.CallContext(ctx, &receipt, "eth_getTransactionReceipt", send.Hash)
			cancel()
			if err == nil && receipt != nil && receipt.Status == 1 {
				mined = receipt
				return true
			}
		}
		return false
	}, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 200*time.Millisecond, "no successful real redemption receipt: %s", readLog(t, f.orch.logPath))
	// Validate real settlement independently of the backend's persisted hash.
	// A bookkeeping defect must not hide the payout or prevent queue checks.
	verifySettlement(t, f, ticket, mined)
	deposit, reserve, _ := senderEscrow(t, g.chain, ticket.Sender)
	debit := new(big.Int).Sub(g.escrow[ticket.Sender], new(big.Int).Add(deposit, reserve))
	require.Equal(t, ticket.FaceValue.String(), debit.String(), "one full escrow debit for the observed winner")
	var used hexutil.Bytes
	g.chain.request(t, &used, "eth_call", map[string]any{"to": g.chain.broker, "data": hexutil.Bytes(append(crypto.Keccak256([]byte("usedTickets(bytes32)"))[:4], ticketHash(t, ticket).Bytes()...))}, "latest")
	require.Equal(t, big.NewInt(1), new(big.Int).SetBytes(used))
	redemptionArtifact(t, map[string]any{"settled_hash": mined.TxHash, "payer": ticket.Sender, "escrow_before": g.escrow[ticket.Sender].String(), "escrow_after": new(big.Int).Add(deposit, reserve).String(), "escrow_debit": debit.String()})
	eventually(t, func() bool {
		latest := observedWinning(t, f.orch, ticket.Sender)
		return latest != nil && latest.Tx != (common.Hash{})
	}, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 200*time.Millisecond, "on-chain success must be reflected in the persisted ticket")
	latest := observedWinning(t, f.orch, ticket.Sender)
	assert.Equal(t, mined.TxHash, latest.Tx, "persisted redemption hash must identify the actual successful transaction, including replacements")
	if f.orch.backend == "node" {
		db := openReadDB(t, paymentDB(f.orch))
		defer db.Close()
		eventually(t, func() bool {
			var at *string
			return db.QueryRow("SELECT redeemed_at FROM winning_tickets WHERE sig=?", ticket.Signature).Scan(&at) == nil && at != nil
		}, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 200*time.Millisecond, "Node must settle only after canonical finalization")
	}
	return mined
}

func ticketHash(t *testing.T, ticket *observedTicket) common.Hash {
	t.Helper()
	// Ticket hashing is packed, not Solidity ABI tuple encoding.
	return crypto.Keccak256Hash(ticket.Recipient.Bytes(), ticket.Sender.Bytes(), common.LeftPadBytes(ticket.FaceValue.Bytes(), 32), common.LeftPadBytes(ticket.WinProb.Bytes(), 32), common.LeftPadBytes(ticket.SenderNonce.Bytes(), 32), ticket.RecipientRandHash.Bytes(), ticket.AuxData)
}

func (g *redemptionGroup) progress(t *testing.T) {
	t.Helper()
	g.rpc.holdNonceReads()
	for _, f := range g.members {
		g.preparePayer(t, f)
	}
	g.chain.waitUnlockedRound(t, g.members[0].payer)
	tickets := make([]*observedTicket, len(g.members))
	for i, f := range g.members {
		tickets[i] = g.newWork(t, f)
	}
	// Queue another wave across every process before waiting for settlement.
	// This also avoids repeating the same forty-block acceptance wait per actor.
	g.prepare(t, tickets...)
	g.rpc.releaseNonceReads()
	for i, ticket := range tickets {
		g.settle(t, i, ticket)
	}
}

func TestOnchainRedemptionConcurrency(t *testing.T) {
	// One ticket per fixed-price job makes receipt/escrow attribution exact.
	t.Setenv("E2E_TICKET_FACE_VALUE", "4000000000000")
	t.Setenv("E2E_TICKET_EV", "2000000000000")
	t.Setenv("E2E_TICKET_WIN_PROB", "57896044618658097711785492504343953926634992332820282019728792003956564819967")
	// Record sources even when chain capabilities block every Node scenario.
	binary(t, "node")
	binary(t, "go-livepeer")
	c := preflightChain(t)
	capabilities := redemptionCapabilities(t, c)
	t.Run("unsupported_finality/node", func(t *testing.T) {
		if capabilities["finalized"] == "" {
			t.Skip("devnet supports finalized observations")
		}
		requireRedemptionCapabilities(t, capabilities, "pending_nonce", "txpool")
		g := newRedemptionGroup(t, c, []string{"node"}, false)
		g.prepare(t)
		deposit, reserve, _ := senderEscrow(t, c, g.members[0].payer.address)
		g.rpc.releaseNonceReads()
		eventually(t, func() bool { g.rpc.mu.Lock(); defer g.rpc.mu.Unlock(); return g.rpc.finalized >= 2 }, 15*time.Second, 100*time.Millisecond)
		require.NotEmpty(t, g.rpc.firstNonces(), "the real worker must reach nonce allocation")
		require.Empty(t, g.rpc.transactions(), "missing finality must not fabricate a submitted transaction")
		db := openReadDB(t, paymentDB(g.members[0].orch))
		defer db.Close()
		var pending int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets WHERE sig=? AND redeemed_at IS NULL AND sig NOT IN (SELECT sig FROM redemption_attempts WHERE phase IN ('confirmed','used','invalid','expired','reverted'))", g.tickets[0].Signature).Scan(&pending))
		require.Equal(t, 1, pending, "an unavailable finalized view must retain the winning ticket")
		afterDeposit, afterReserve, _ := senderEscrow(t, c, g.members[0].payer.address)
		require.Equal(t, deposit, afterDeposit)
		require.Equal(t, reserve, afterReserve)
	})
	run := func(t *testing.T, backends []string, duplicate bool, test func(*testing.T, *redemptionGroup)) {
		requireRedemptionCapabilities(t, capabilities, "pending_nonce", "txpool")
		for _, backend := range backends {
			if backend == "node" {
				requireRedemptionCapabilities(t, capabilities, "finalized", "finalized_nonce")
			}
		}
		group := newRedemptionGroup(t, c, backends, duplicate)
		group.prepare(t)
		test(t, group)
	}
	for _, backends := range [][]string{{"node", "node"}, {"node", "go-livepeer"}, {"go-livepeer", "node"}, {"go-livepeer", "go-livepeer"}, {"node", "node", "go-livepeer"}} {
		t.Run("collision/"+strings.Join(backends, "+"), func(t *testing.T) {
			run(t, backends, false, func(t *testing.T, g *redemptionGroup) {
				g.rpc.releaseNonceReads()
				for i, ticket := range g.tickets {
					g.settle(t, i, ticket)
				}
				nonces := g.rpc.firstNonces()
				require.Len(t, nonces, len(backends))
				for _, nonce := range nonces {
					require.Equal(t, nonces[0], nonce, "the real initial nonce reads must collide")
				}
				first := make(map[int]redemptionSend)
				for _, send := range g.rpc.transactions() {
					if _, seen := first[send.Process]; !seen {
						first[send.Process] = send
						require.Equal(t, nonces[0], send.Nonce)
					}
				}
				require.Len(t, first, len(backends))
				require.NotEqual(t, first[0].Hash, first[1].Hash)
				g.progress(t)
			})
		})
	}
	for _, backend := range []string{"node", "go-livepeer"} {
		for _, scenario := range []string{"external_transaction", "uncertain_restart", "replacement", "replacement_limit", "nonce_gap", "duplicate"} {
			t.Run(scenario+"/"+backend, func(t *testing.T) {
				if scenario == "replacement" || scenario == "replacement_limit" || scenario == "nonce_gap" {
					requireRedemptionCapabilities(t, capabilities, "mining_control")
				}
				if scenario == "nonce_gap" {
					requireRedemptionCapabilities(t, capabilities, "drop_transaction")
				}
				backends := []string{backend}
				if scenario == "duplicate" {
					backends = append(backends, backend)
				}
				run(t, backends, scenario == "duplicate", func(t *testing.T, g *redemptionGroup) {
					runRedemptionFault(t, g, scenario)
				})
			})
		}
	}
}

func runRedemptionFault(t *testing.T, g *redemptionGroup, scenario string) {
	t.Helper()
	f, ticket := g.members[0], g.tickets[0]
	var external common.Hash
	switch scenario {
	case "external_transaction":
		g.rpc.external = func(ctx context.Context, incoming *types.Transaction) error {
			tx, err := types.SignTx(types.NewTransaction(incoming.Nonce(), f.redeemer.address, new(big.Int), 21000, incoming.GasFeeCap(), nil), types.LatestSignerForChainID(g.chain.chainID), f.redeemer.key)
			if err != nil {
				return err
			}
			raw, err := tx.MarshalBinary()
			if err != nil {
				return err
			}
			var hash common.Hash
			if err := g.chain.rpc.CallContext(ctx, &hash, "eth_sendRawTransaction", hexutil.Bytes(raw)); err != nil {
				return err
			}
			receipt, err := awaitRedemptionReceipt(ctx, g.chain, hash)
			if err == nil && receipt.Status != 1 {
				return fmt.Errorf("external transaction reverted")
			}
			g.rpc.mu.Lock()
			external = hash
			g.rpc.mu.Unlock()
			return err
		}
	case "uncertain_restart":
		g.rpc.loseAck = true
	case "replacement", "replacement_limit", "nonce_gap":
		var result any
		g.chain.request(t, &result, "miner_stop")
		t.Cleanup(func() { g.chain.request(t, &result, "miner_start", 1) })
	}
	g.rpc.releaseNonceReads()
	if scenario == "uncertain_restart" {
		ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"))
		defer cancel()
		require.NoError(t, awaitRPC(ctx, g.rpc.lost), "submission must reach the real pool before its reply is suppressed")
		f.orch.process.stop()
		g.rpc.mu.Lock()
		g.rpc.paused = false
		g.rpc.mu.Unlock()
		f.orch = startOrchestrator(t, f.orch.opts)
	}
	if scenario == "replacement" || scenario == "replacement_limit" || scenario == "nonce_gap" {
		eventually(t, func() bool { return len(g.rpc.transactions()) > 0 }, 20*time.Second, 100*time.Millisecond)
		original := g.rpc.transactions()[0]
		require.True(t, original.Accepted, "the real pool must acknowledge the original before testing recovery")
		pending, err := g.chain.rpcTransaction(t, original.Hash)
		require.NoError(t, err)
		require.NotNil(t, pending, "the accepted transaction must actually be in the real pool")
		var before *types.Receipt
		g.chain.request(t, &before, "eth_getTransactionReceipt", original.Hash)
		require.Nil(t, before, "mining must remain paused during replacement checks")
		if scenario == "nonce_gap" {
			var result any
			g.chain.request(t, &result, "txpool_dropTransaction", original.Hash)
			f.orch.process.stop()
			f.orch = startOrchestrator(t, f.orch.opts)
		}
		eventually(t, func() bool { return len(g.rpc.transactions()) >= 2 }, 20*time.Second, 100*time.Millisecond, "pending transaction must be recovered or replaced")
		next := g.rpc.transactions()[1]
		require.True(t, next.Accepted, "the real pool must accept the recovered transaction")
		require.Equal(t, original.Nonce, next.Nonce)
		require.Equal(t, original.Calldata, next.Calldata)
		require.Equal(t, original.tx.Gas(), next.tx.Gas())
		require.Equal(t, original.tx.Value(), next.tx.Value())
		require.Equal(t, original.tx.To(), next.tx.To())
		require.Equal(t, original.tx.ChainId(), next.tx.ChainId())
		if scenario == "nonce_gap" {
			require.Equal(t, original.Hash, next.Hash)
		} else {
			for _, fees := range [][2]*big.Int{{original.tx.GasFeeCap(), next.tx.GasFeeCap()}, {original.tx.GasTipCap(), next.tx.GasTipCap()}} {
				minimum := new(big.Int).Div(new(big.Int).Add(new(big.Int).Mul(fees[0], big.NewInt(110)), big.NewInt(99)), big.NewInt(100))
				require.GreaterOrEqual(t, fees[1].Cmp(minimum), 0, "both caps must meet geth's 10%% replacement threshold")
				require.Positive(t, fees[1].Cmp(fees[0]))
			}
		}
		if scenario == "replacement_limit" {
			// Once the replacement is acknowledged, its inclusion timeout must
			// expire without a second replacement in this root's fee lifecycle.
			time.Sleep(3500 * time.Millisecond)
			sameNonce := 0
			for _, send := range g.rpc.transactions() {
				if send.Nonce == original.Nonce {
					sameNonce++
				}
			}
			require.Equal(t, 2, sameNonce, "one configured replacement per attempt")
		}
		var result any
		g.chain.request(t, &result, "miner_start", 1)
	}
	g.settle(t, 0, ticket)
	if scenario == "external_transaction" {
		g.rpc.mu.Lock()
		hash := external
		g.rpc.mu.Unlock()
		require.NotEqual(t, common.Hash{}, hash)
		var receipt *types.Receipt
		g.chain.request(t, &receipt, "eth_getTransactionReceipt", hash)
		require.NotNil(t, receipt)
		require.EqualValues(t, 1, receipt.Status)
		redemptionArtifact(t, map[string]any{"external_transaction": hash, "receipt_status": receipt.Status})
	}
	if scenario == "duplicate" {
		loser := g.members[1]
		db := openReadDB(t, paymentDB(loser.orch))
		defer db.Close()
		query := "SELECT count(*) FROM winning_tickets JOIN redemption_attempts USING(sig) WHERE sig=? AND phase IN ('confirmed','used')"
		if loser.orch.backend == "go-livepeer" {
			query = "SELECT count(*) FROM ticketQueue WHERE sig=? AND redeemedAt IS NOT NULL"
		}
		eventually(t, func() bool { var n int; return db.QueryRow(query, ticket.Signature).Scan(&n) == nil && n == 1 }, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 200*time.Millisecond, "losing replica must reconcile the genuinely used ticket")
		successes := make(map[common.Hash]bool)
		for _, send := range g.rpc.transactions() {
			var receipt *types.Receipt
			g.chain.request(t, &receipt, "eth_getTransactionReceipt", send.Hash)
			if receipt != nil && receipt.Status == 1 {
				successes[send.Hash] = true
			}
		}
		require.Len(t, successes, 1, "one canonical redemption across independent stores")
		if loser.orch.backend == "node" {
			var n int
			require.NoError(t, db.QueryRow("SELECT count(*) FROM redemption_transactions WHERE sig=?", ticket.Signature).Scan(&n))
			require.Positive(t, n, "losing transaction history must remain inspectable")
		}
	}
	g.progress(t)
}
