package e2escope

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClearinghouseChainRecovery(t *testing.T) {
	forEachPair(t, func(t *testing.T, orch, signer string, wallets walletMode) {
		for _, scenario := range []string{"escrow and withdrawal", "matched settlement", "delayed attribution", "unmatched settlement", "ambiguous attribution fixture", "confirmations and checkpoints", "RPC outage", "reorg rollback replay", "reorg beyond boundary"} {
			t.Run(scenario, func(t *testing.T) {
				c := preflightChain(t)
				c.requireCanonicalHeaders(t)
				c.requireControl(t)
				if orch == "node" {
					c.requireFinality(t)
				}
				faults := faultRPC(t, c)
				ch := startClearinghouse(t, c, true)
				kafkaConfig := ch.signerKafkaConfig(t, signer)
				if scenario == "escrow and withdrawal" {
					t.Setenv("E2E_PAYER_DEPOSIT_ETH", "0.0000008")
				}
				opts := orchestratorOptions{}
				if orch == "go-livepeer" {
					opts.goExtra = []string{"-maxFaceValue", envDefault("E2E_TICKET_FACE_VALUE", "400000000000")}
				}
				f := setupPaid(t, c, orch, signer, wallets, ch.webhook+"/v1/signer/authorize", "Livepeer-Clearinghouse-Token: "+ch.signerSecret, kafkaConfig, opts)
				ch.config["ReorgLookback"] = int64(8)
				ch.enableListener(t, f.payer.address.Hex())
				waitChainCheckpoint(t, ch, c.block(t)-2)
				k := ch.key(t, "eth", "1")
				if scenario == "RPC outage" {
					before := chainCheckpoint(t, ch)
					faults.down.Store(true)
					// Administrative chain calls still use the real endpoint; only the
					// processes under test lose their RPC connection.
					c.fundPayer(t, f.payer, big.NewInt(17), big.NewInt(29))
					time.Sleep(700 * time.Millisecond)
					require.Equal(t, before, chainCheckpoint(t, ch))
					faults.down.Store(false)
					c.mineTo(t, c.block(t)+3)
					waitChainCheckpoint(t, ch, c.block(t)-2)
					assertEscrowReconciled(t, ch, f)
					return
				}
				if scenario == "confirmations and checkpoints" {
					before := eventCount(t, ch)
					c.fundPayer(t, f.payer, big.NewInt(17), big.NewInt(29))
					height := c.block(t)
					if c.block(t) < height+2 {
						require.Equal(t, before, eventCount(t, ch), "unconfirmed logs excluded")
					}
					c.mineTo(t, height+3)
					waitChainCheckpoint(t, ch, height+1)
					require.Greater(t, eventCount(t, ch), before)
					checkpoint := chainCheckpoint(t, ch)
					events := eventCount(t, ch)
					ch.restart(t)
					require.GreaterOrEqual(t, chainCheckpoint(t, ch), checkpoint)
					require.Equal(t, events, eventCount(t, ch), "saved checkpoint prevents replay charges")
					assertEscrowReconciled(t, ch, f)
					return
				}
				if scenario == "reorg rollback replay" || scenario == "reorg beyond boundary" {
					var snapshot string
					c.request(t, &snapshot, "evm_snapshot")
					before := eventCount(t, ch)
					c.fundPayer(t, f.payer, big.NewInt(17), big.NewInt(29))
					c.mineTo(t, c.block(t)+3)
					waitChainCheckpoint(t, ch, c.block(t)-2)
					require.Greater(t, eventCount(t, ch), before)
					if scenario == "reorg beyond boundary" {
						c.mineTo(t, c.block(t)+12)
						waitChainCheckpoint(t, ch, c.block(t)-2)
					}
					ch.process.stop()
					var restored bool
					c.request(t, &restored, "evm_revert", snapshot)
					require.True(t, restored)
					c.mineTo(t, c.block(t)+4)
					ch.restart(t)
					if scenario == "reorg beyond boundary" {
						eventually(t, func() bool {
							return strings.Contains(readLog(t, ch.process.logPath), "reorg exceeds retained common-ancestor history")
						}, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond)
						return
					}
					waitChainCheckpoint(t, ch, c.block(t)-2)
					assertEscrowReconciled(t, ch, f)
					count := eventCount(t, ch)
					c.fundPayer(t, f.payer, big.NewInt(31), big.NewInt(47))
					c.mineTo(t, c.block(t)+3)
					waitChainCheckpoint(t, ch, c.block(t)-2)
					require.Greater(t, eventCount(t, ch), count)
					assertEscrowReconciled(t, ch, f)
					return
				}
				if scenario == "delayed attribution" {
					ch.config["EnableAccounting"] = false
					ch.restart(t)
				}
				headers := k.headers()
				if scenario == "unmatched settlement" {
					opts := f.signer.opts
					opts.kafka = nil
					f.signer = startSigner(t, opts)
				}
				ticket := makeWinning(t, f, headers)
				if scenario == "escrow and withdrawal" {
					for range 3 {
						makeWinning(t, f, headers)
					}
				}
				beforeSpent := ch.balance(t, k, "allocation_spent")
				c.mineTo(t, ticket.Expiry+3)
				receipt := waitSettlement(t, f, ticket)
				c.mineTo(t, receipt.BlockNumber.Uint64()+3)
				waitChainCheckpoint(t, ch, receipt.BlockNumber.Uint64()+1)
				require.Positive(t, countSettlements(t, ch, "settled"))
				assertEscrowReconciled(t, ch, f)
				if scenario == "escrow and withdrawal" {
					require.Positive(t, scalarInt(t, ch, "SELECT count(*) FROM ticket_broker_events WHERE event_type='reserve_claimed'"))
					c.transact(t, f.payer, "ticketBroker", c.broker, "unlock", new(big.Int))
					_, _, round := senderEscrow(t, c, f.payer.address)
					for c.call(t, "roundsManager", c.rounds, "currentRound")[0].(*big.Int).Cmp(round) < 0 {
						c.mineTo(t, c.block(t)+10)
					}
					c.initializeRound(t, f.payer)
					c.transact(t, f.payer, "ticketBroker", c.broker, "withdraw", new(big.Int))
					c.mineTo(t, c.block(t)+3)
					waitChainCheckpoint(t, ch, c.block(t)-2)
					deposit, reserve, _ := senderEscrow(t, c, f.payer.address)
					require.Zero(t, deposit.Sign())
					require.Zero(t, reserve.Sign())
					assertEscrowReconciled(t, ch, f)
					return
				}
				expected := "matched"
				if scenario == "unmatched settlement" || scenario == "delayed attribution" {
					expected = "unmatched"
				}
				require.Positive(t, scalarInt(t, ch, "SELECT count(*) FROM settlements WHERE match_status=?", expected))
				if scenario == "delayed attribution" {
					ch.config["EnableAccounting"] = true
					ch.restart(t)
					eventually(t, func() bool {
						return scalarInt(t, ch, "SELECT count(*) FROM settlements WHERE match_status='matched'") > 0
					}, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond)
				}
				if scenario == "ambiguous attribution fixture" {
					// Explicit injected fixture: two authorized session IDs claim the same PM
					// session. The successful matched path above used only real signer events.
					raw := ch.latestEvent(t)
					var event map[string]any
					require.NoError(t, json.Unmarshal(raw, &event))
					data := event["data"].(map[string]any)
					stateID := uuid.NewString()
					state := map[string]any{"StateID": stateID, "App": data["app"], "Type": data["pipeline"], "OrchestratorAddress": f.recipient.address.Hex()}
					status, body := httpJSON(t, "POST", ch.webhook+"/v1/signer/authorize", map[string]any{"state": state, "headers": map[string][]string{"Authorization": {"Bearer " + k.Key}}}, map[string]string{"Livepeer-Clearinghouse-Token": ch.signerSecret})
					require.Equal(t, 200, status, string(body))
					var decision map[string]any
					require.NoError(t, json.Unmarshal(body, &decision))
					require.Equal(t, float64(200), decision["status"])
					event["id"] = uuid.NewString()
					data["session_id"] = stateID
					data["auth_id"] = decision["auth_id"]
					data["request_id"] = uuid.NewString()
					payload, _ := json.Marshal(event)
					ch.publish(t, payload)
					eventually(t, func() bool {
						return scalarInt(t, ch, "SELECT count(*) FROM settlements WHERE match_status='ambiguous'") > 0
					}, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond)
				} else if scenario != "delayed attribution" {
					require.Equal(t, beforeSpent.String(), ch.balance(t, k, "allocation_spent").String(), "settlement never charges the grant again")
				}
			})
		}
	})
}

func scalarInt(t *testing.T, ch *clearinghouseFixture, query string, args ...any) int64 {
	t.Helper()
	db := openReadDB(t, ch.db)
	defer db.Close()
	var n int64
	require.NoError(t, db.QueryRow(query, args...).Scan(&n))
	return n
}
func eventCount(t *testing.T, ch *clearinghouseFixture) int64 {
	return scalarInt(t, ch, "SELECT count(*) FROM ticket_broker_events WHERE status='canonical'")
}
func countSettlements(t *testing.T, ch *clearinghouseFixture, status string) int64 {
	return scalarInt(t, ch, "SELECT count(*) FROM settlements WHERE status=?", status)
}
func chainCheckpoint(t *testing.T, ch *clearinghouseFixture) int64 {
	return scalarInt(t, ch, "SELECT coalesce(max(next_position),0) FROM ingestion_checkpoints WHERE source='chain'")
}
func waitChainCheckpoint(t *testing.T, ch *clearinghouseFixture, block uint64) {
	t.Helper()
	eventually(t, func() bool { return chainCheckpoint(t, ch) > int64(block) }, envDuration(t, "E2E_ACCOUNTING_TIMEOUT", "20s"), 100*time.Millisecond, "listener checkpoint beyond %d: %s", block, readLog(t, ch.process.logPath))
}
func assertEscrowReconciled(t *testing.T, ch *clearinghouseFixture, f *paidFixture) {
	t.Helper()
	deposit, reserve, _ := senderEscrow(t, f.chain, f.payer.address)
	owner := f.chain.chainID.String() + ":" + strings.ToLower(f.chain.broker.Hex()) + ":" + strings.ToLower(f.payer.address.Hex())
	for _, value := range []struct {
		kind string
		want *big.Int
	}{{"escrow_deposit", deposit}, {"escrow_reserve", reserve}} {
		got := ch.scalar(t, "SELECT coalesce((SELECT balance_units FROM account_balances WHERE account_type=? AND account_id=? AND currency='eth'),'0')", value.kind, owner)
		require.Equal(t, value.want.String(), got, value.kind)
	}
	ch.request(t, "GET", "/v1/escrow/report", nil, 200, nil)
	ch.request(t, "GET", "/v1/settlements", nil, 200, nil)
}
