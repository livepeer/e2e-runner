package e2escope

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/require"
)

// Each component has its own database. The forwarding orchestrator has no
// wallet credentials; only the standalone process can sign redemption gas.
type remoteRedemption struct {
	f                       *paidFixture
	dir, endpoint, probes   string
	config                  map[string]any
	process                 *childProcess
	fault                   atomic.Int32
	deliveries, interrupted atomic.Int64
}

const (
	deliveryAvailable int32 = iota
	deliveryOutage
	deliveryLostReply
	deliveryWrongHash
)

func newRemoteRedemption(t *testing.T, limits map[string]any) *remoteRedemption {
	t.Helper()
	c := preflightChain(t)
	c.requireFinality(t)
	r := &remoteRedemption{dir: t.TempDir()}
	listen, metrics := freeAddr(t), freeAddr(t)
	r.endpoint, r.probes = publicURL(t, "http://"+listen), publicURL(t, "http://"+metrics)
	target, err := url.Parse(r.endpoint)
	require.NoError(t, err)
	proxy := fixtureProxy(target)
	proxy.ModifyResponse = func(response *http.Response) error {
		mode := r.fault.Load()
		if response.StatusCode == http.StatusAccepted && (mode == deliveryLostReply || mode == deliveryWrongHash) {
			_ = response.Body.Close()
			body := `{"ticket_hash":"` + common.Hash{}.Hex() + `"}`
			if mode == deliveryLostReply {
				response.StatusCode = http.StatusServiceUnavailable
				body = "injected loss of durable ticket acknowledgment"
			}
			response.Body = io.NopCloser(strings.NewReader(body))
			response.ContentLength = int64(len(body))
			response.Header.Set("Content-Length", fmt.Sprint(len(body)))
			r.interrupted.Add(1)
		}
		return nil
	}
	forward := newFixtureServer(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		r.deliveries.Add(1)
		if r.fault.Load() == deliveryOutage {
			r.interrupted.Add(1)
			http.Error(w, "test redeemer outage", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, request)
	}))
	forwardURL, err := url.Parse(forward.URL)
	require.NoError(t, err)
	r.f = setupPaid(t, c, "node", "node", splitWallet, "", "", nil, paidSetupOptions{
		orchestrator: orchestratorOptions{extra: map[string]any{"RedeemerURL": forwardURL, "RedeemerGrants": fixtureGrants(t)}},
	})
	r.config = map[string]any{
		"Network": "devenv", "Listen": listen, "MetricsListen": metrics,
		"RedeemerDB":   filepath.Join(r.dir, "redeemer.sqlite"),
		"KeystoreFile": r.f.redeemer.keystore, "KeystorePasswordFile": r.f.redeemer.passwordFile,
		"RPCURLFile": secretFile(t, r.dir, "rpc", c.url), "ChainID": c.chainID.Uint64(), "Controller": c.controller.Hex(),
		// A real fee ceiling keeps accepted tickets unsubmitted until the test
		// has checked the handoff boundary. Recovery removes it on restart.
		"RedeemerMaxFeePerGas": "1",
	}
	maps.Copy(r.config, limits)
	r.start(t)
	waitForHTTP(t, http.MethodGet, r.probes+"/readyz", http.StatusOK, r.process.logPath)
	data, err := os.ReadFile(filepath.Join(r.f.orch.dir, "orchestrator.toml"))
	require.NoError(t, err)
	require.NotContains(t, string(data), "KeystoreFile")
	require.NotContains(t, string(data), "KeystorePassword")
	c.waitUnlockedRound(t, r.f.payer)
	return r
}

func (r *remoteRedemption) start(t *testing.T) {
	t.Helper()
	path := writeTOML(t, filepath.Join(r.dir, "redeemer.toml"), r.config)
	r.process = startChild(t, "redeemer-node", binary(t, "node"), r.dir, []string{"redeemer", "--config", path})
	waitForHTTP(t, http.MethodGet, r.probes+"/healthz", http.StatusOK, r.process.logPath)
}

func (r *remoteRedemption) store() *testOrchestrator {
	return &testOrchestrator{backend: "node", dir: r.dir, logPath: r.process.logPath}
}

type remoteTicketState struct {
	Delivery, Settlement string
	Settled, Exposed     bool
	Failures             int
}

func waitRemoteTicket(t *testing.T, orch *testOrchestrator, ticket *observedTicket, condition func(remoteTicketState) bool) remoteTicketState {
	t.Helper()
	db := openReadDB(t, paymentDB(orch))
	defer db.Close()
	var state remoteTicketState
	eventually(t, func() bool {
		err := db.QueryRow("SELECT delivery,settlement,settled,exposed,failures FROM ticket_state WHERE hash=?", ticketHash(t, ticket).Bytes()).Scan(&state.Delivery, &state.Settlement, &state.Settled, &state.Exposed, &state.Failures)
		if err == sql.ErrNoRows {
			return false
		}
		require.NoError(t, err)
		return condition(state)
	}, envDuration(t, "E2E_REDEMPTION_TIMEOUT", "90s"), 200*time.Millisecond, "ticket state: %s", readLog(t, orch.logPath))
	return state
}

func (r *remoteRedemption) winner(t *testing.T, f *paidFixture) *observedTicket {
	t.Helper()
	// Use the existing real payment fixture's round synchronization and
	// one-winner assertion, independently for each funded payer.
	g := &redemptionGroup{chain: f.chain}
	return g.newWork(t, f)
}

func (r *remoteRedemption) eligible(t *testing.T, ticket *observedTicket) {
	t.Helper()
	r.f.chain.advanceTo(t, ticket.Expiry+1)
	waitRedemptionRound(t, r.f.chain, r.f.payer)
}

func (r *remoteRedemption) assertUnsubmitted(t *testing.T, ticket *observedTicket) {
	t.Helper()
	for _, orch := range []*testOrchestrator{r.f.orch, r.store()} {
		db := openReadDB(t, paymentDB(orch))
		var count int
		require.NoError(t, db.QueryRow("SELECT count(*) FROM redemption_transactions").Scan(&count))
		require.Zero(t, count, "ticket acknowledgment must not claim financial settlement")
		require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets WHERE sig=? AND redeemed_at IS NOT NULL", ticket.Signature).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, db.Close())
	}
}

func (r *remoteRedemption) settle(t *testing.T, f *paidFixture, ticket *observedTicket, escrow *big.Int) {
	t.Helper()
	observer := *f
	observer.orch = r.store()
	receipt := waitSettlement(t, &observer, ticket)
	for _, orch := range []*testOrchestrator{f.orch, r.store()} {
		waitRemoteTicket(t, orch, ticket, func(s remoteTicketState) bool { return s.Settled && s.Settlement == "used" })
	}
	deposit, reserve, _ := senderEscrow(t, f.chain, f.payer.address)
	debit := new(big.Int).Sub(escrow, new(big.Int).Add(deposit, reserve))
	require.Equal(t, ticket.FaceValue.String(), debit.String(), "duplicate delivery must debit escrow exactly once")
	db := openReadDB(t, paymentDB(f.orch))
	defer db.Close()
	var attempts int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM redemption_transactions").Scan(&attempts))
	require.Zero(t, attempts, "the keyless orchestrator must never sign a transaction")
	redemptionArtifact(t, map[string]any{"ticket_hash": ticketHash(t, ticket), "transaction": receipt.TxHash,
		"recipient": f.recipient.address, "gas_payer": f.redeemer.address, "escrow_debit": debit.String(), "deliveries": r.deliveries.Load()})
}

func payerEscrow(t *testing.T, f *paidFixture) *big.Int {
	t.Helper()
	deposit, reserve, _ := senderEscrow(t, f.chain, f.payer.address)
	return new(big.Int).Add(deposit, reserve)
}

func remoteTicketRequest(c *chainFixture, ticket *observedTicket) map[string]any {
	return map[string]any{"chain_id": c.chainID.String(), "controller": c.controller.Hex(), "ticket": map[string]any{
		"payer_address": ticket.Sender.Hex(), "recipient": ticket.Recipient.Hex(), "face_value": ticket.FaceValue.String(),
		"win_prob": ticket.WinProb.String(), "ticket_nonce": ticket.SenderNonce.Uint64(), "recipient_rand_hash": ticket.RecipientRandHash.Hex(),
		"creation_round": new(big.Int).SetBytes(ticket.AuxData[:32]).String(), "creation_round_block_hash": common.BytesToHash(ticket.AuxData[32:]).Hex(),
		"params_expiration_block": fmt.Sprint(ticket.Expiry), "sig": hexutil.Encode(ticket.Signature), "recipient_rand": ticket.Random.String(),
	}}
}

func (r *remoteRedemption) post(t *testing.T, payload any, expected int) []byte {
	t.Helper()
	status, body, err := requestJSON(http.MethodPost, r.endpoint+"/tickets", payload, nil)
	require.NoError(t, err)
	require.Equal(t, expected, status, "%s", body)
	return body
}

func TestOnchainRedeemer(t *testing.T) {
	// Fixed jobs need at most one ticket; observe an actual winner rather than
	// assuming a probabilistic payment won.
	t.Setenv("E2E_TICKET_FACE_VALUE", "1000000000000000")
	t.Setenv("E2E_TICKET_EV", "500000000000000")
	t.Setenv("E2E_TICKET_WIN_PROB", "57896044618658097711785492504343953926634992332820282019728792003956564819967")
	t.Setenv("E2E_SIGNER_MAX_TICKET_EV", "3000000000000000")
	t.Setenv("E2E_SIGNER_MAX_BATCH_EV", "20000000000000000")
	for _, scenario := range []struct {
		name string
		mode int32
	}{{"handoff_and_restart", deliveryAvailable}, {"redeemer_outage", deliveryOutage}, {"lost_acknowledgment", deliveryLostReply}, {"incorrect_acknowledgment", deliveryWrongHash}} {
		t.Run(scenario.name, func(t *testing.T) {
			r := newRemoteRedemption(t, nil)
			r.fault.Store(scenario.mode)
			escrow := payerEscrow(t, r.f)
			ticket := r.winner(t, r.f)
			waitRemoteTicket(t, r.f.orch, ticket, func(s remoteTicketState) bool { return s.Delivery == "held" })
			require.LessOrEqual(t, r.f.chain.block(t), ticket.Expiry, "INFRASTRUCTURE: need time to observe the acceptance window")
			require.Zero(t, r.deliveries.Load(), "randomness must not leave the orchestrator before parameter expiry")
			r.eligible(t, ticket)
			if scenario.mode != deliveryAvailable {
				state := waitRemoteTicket(t, r.f.orch, ticket, func(s remoteTicketState) bool { return s.Delivery == "retryable" && s.Failures > 0 })
				require.False(t, state.Settled)
				require.True(t, state.Exposed)
				require.Positive(t, r.interrupted.Load(), "the intended HTTP fault must actually occur")
				db := openReadDB(t, paymentDB(r.store()))
				var count int
				require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets WHERE sig=?", ticket.Signature).Scan(&count))
				require.NoError(t, db.Close())
				if scenario.mode == deliveryOutage {
					require.Zero(t, count, "unavailable redeemer never received the ticket")
				} else {
					require.Equal(t, 1, count, "a lost or incorrect reply follows real durable admission")
				}
				// Restore both independent processes with their retained stores.
				r.f.orch.process.stop()
				r.process.stop()
				r.start(t)
				r.fault.Store(deliveryAvailable)
				r.f.orch = startOrchestrator(t, r.f.orch.opts)
			}
			state := waitRemoteTicket(t, r.f.orch, ticket, func(s remoteTicketState) bool { return s.Delivery == "acknowledged" })
			require.False(t, state.Settled, "202 acknowledges storage only")
			require.True(t, state.Exposed)
			r.assertUnsubmitted(t, ticket)
			require.Equal(t, escrow, payerEscrow(t, r.f), "handoff alone cannot spend escrow")
			// Repeated delivery, including after restart, must keep one identity.
			r.post(t, remoteTicketRequest(r.f.chain, ticket), http.StatusAccepted)
			db := openReadDB(t, paymentDB(r.store()))
			var count int
			require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets").Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, db.Close())
			// The redeemer must finish retained work while its source is offline.
			r.f.orch.process.stop()
			r.process.stop()
			delete(r.config, "RedeemerMaxFeePerGas")
			r.start(t)
			observer := *r.f
			observer.orch = r.store()
			waitSettlement(t, &observer, ticket)
			waitRemoteTicket(t, r.store(), ticket, func(s remoteTicketState) bool { return s.Settled && s.Settlement == "used" })
			r.f.orch = startOrchestrator(t, r.f.orch.opts)
			r.settle(t, r.f, ticket, escrow)
			if scenario.mode == deliveryAvailable {
				ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_CLI_TIMEOUT", "2m"))
				defer cancel()
				output, err := fixtureCommand(t, ctx, "redeemer-inspect", binary(t, "node"), r.dir,
					"redeemer", "redemptions", "--config", filepath.Join(r.dir, "redeemer.toml")).CombinedOutput()
				require.NoError(t, err, "%s", output)
				var statuses []map[string]string
				require.NoError(t, json.Unmarshal(output, &statuses))
				require.NotEmpty(t, statuses)
				for _, status := range statuses {
					for key := range status {
						require.Contains(t, []string{"hash", "phase", "error"}, key, "inspection must omit ticket secrets and signed bytes")
					}
					require.Contains(t, []string{"confirmed", "used"}, status["phase"])
				}
			}
		})
	}

	for _, limit := range []string{"MaxPendingTickets", "MaxTicketsPerRecipient"} {
		t.Run("admission/"+limit, func(t *testing.T) {
			r := newRemoteRedemption(t, map[string]any{limit: 1})
			r.fault.Store(deliveryOutage)
			ticket := r.winner(t, r.f)
			second := *r.f
			g := &redemptionGroup{chain: r.f.chain, escrow: make(map[common.Address]*big.Int)}
			g.preparePayer(t, &second)
			next := r.winner(t, &second)
			// Direct API coverage starts after expiry, preserving the same
			// randomness-release boundary as the orchestrator's real worker.
			r.eligible(t, ticket)
			for _, invalid := range []string{"chain", "controller", "signature", "randomness", "unknown_field", "oversized"} {
				t.Run(invalid, func(t *testing.T) {
					payload := remoteTicketRequest(r.f.chain, ticket)
					fields := payload["ticket"].(map[string]any)
					var request any = payload
					switch invalid {
					case "chain":
						payload["chain_id"] = new(big.Int).Add(r.f.chain.chainID, big.NewInt(1)).String()
					case "controller":
						payload["controller"] = common.Address{}.Hex()
					case "signature":
						fields["sig"] = hexutil.Encode(make([]byte, 65))
					case "randomness":
						fields["recipient_rand"] = new(big.Int).Add(ticket.Random, big.NewInt(1)).String()
					case "unknown_field":
						payload["unexpected"] = true
					case "oversized":
						request = strings.NewReader(strings.Repeat(" ", 16<<10) + "{}")
					}
					r.post(t, request, http.StatusBadRequest)
				})
			}
			db := openReadDB(t, paymentDB(r.store()))
			defer db.Close()
			var count int
			require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets").Scan(&count))
			require.Zero(t, count, "rejected input must not be persisted")
			request := remoteTicketRequest(r.f.chain, ticket)
			var reply struct {
				Hash common.Hash `json:"ticket_hash"`
			}
			require.NoError(t, json.Unmarshal(r.post(t, request, http.StatusAccepted), &reply))
			require.Equal(t, ticketHash(t, ticket), reply.Hash)
			r.post(t, request, http.StatusAccepted)
			request["ticket"].(map[string]any)["params_expiration_block"] = fmt.Sprint(ticket.Expiry + 1)
			r.post(t, request, http.StatusConflict)
			require.NoError(t, db.QueryRow("SELECT count(*) FROM winning_tickets").Scan(&count))
			require.Equal(t, 1, count, "duplicates at capacity and conflicting scheduling metadata cannot add work")
			if limit == "MaxPendingTickets" {
				waitForHTTP(t, http.MethodGet, r.probes+"/readyz", http.StatusServiceUnavailable, r.process.logPath)
			}
			r.eligible(t, next)
			r.post(t, remoteTicketRequest(r.f.chain, next), http.StatusServiceUnavailable)
			r.post(t, remoteTicketRequest(r.f.chain, ticket), http.StatusAccepted)
			escrow := payerEscrow(t, r.f)
			r.process.stop()
			delete(r.config, "RedeemerMaxFeePerGas")
			r.start(t)
			r.settle(t, r.f, ticket, escrow)
			waitForHTTP(t, http.MethodGet, r.probes+"/readyz", http.StatusOK, r.process.logPath)
			// Finalized settlement releases capacity for the retained second
			// ticket; neither a full queue nor a duplicate may lose it.
			r.post(t, remoteTicketRequest(r.f.chain, next), http.StatusAccepted)
			r.settle(t, &second, next, g.escrow[second.payer.address])
		})
	}

	t.Run("wallet_and_database_policy", func(t *testing.T) {
		r := newRemoteRedemption(t, nil)
		r.fault.Store(deliveryOutage)
		ticket := r.winner(t, r.f)
		r.f.orch.process.stop()
		badConfig := func(component string, config map[string]any, expected string) {
			t.Helper()
			path := writeTOML(t, filepath.Join(t.TempDir(), component+".toml"), config)
			ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_READY_TIMEOUT", "30s"))
			defer cancel()
			output, err := fixtureCommand(t, ctx, component+"-node", binary(t, "node"), r.dir, component, "--config", path).CombinedOutput()
			require.Error(t, err)
			require.NoError(t, ctx.Err(), "invalid configuration must fail promptly")
			require.Contains(t, string(output), expected)
		}
		config := maps.Clone(r.config)
		config["KeystoreFile"], config["KeystorePasswordFile"] = r.f.recipient.keystore, r.f.recipient.passwordFile
		badConfig("redeemer", config, "redeemer wallet is registered or active as an orchestrator")
		config = maps.Clone(r.config)
		config["RedeemerDB"] = paymentDB(r.f.orch)
		badConfig("redeemer", config, "application role mismatch")
		var orchConfig map[string]any
		_, err := toml.DecodeFile(filepath.Join(r.f.orch.dir, "orchestrator.toml"), &orchConfig)
		require.NoError(t, err)
		changedURL, err := url.Parse(r.endpoint + "/different-destination")
		require.NoError(t, err)
		orchConfig["RedeemerURL"] = changedURL
		badConfig("orchestrator", orchConfig, "drain outstanding tickets")
		r.assertUnsubmitted(t, ticket)
	})
}
