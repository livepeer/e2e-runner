package e2escope

import (
	"context"
	"crypto/ecdsa"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/contracts.json
var contractABIs []byte

type chainAccount struct {
	key                    *ecdsa.PrivateKey
	address                common.Address
	keystore, passwordFile string
	runScoped              bool
}

type recipientChain struct {
	chainID    string
	genesis    common.Hash
	controller common.Address
}

// Hold the lease until fixture cleanup has stopped every worker and restored
// snapshots. Only account material survives; RPC clients and databases do not.
var paidRecipients = struct {
	sync.Mutex
	accounts map[recipientChain]*chainAccount
	dirs     []string
}{accounts: make(map[recipientChain]*chainAccount)}

type chainFixture struct {
	url, setupURL                                    string
	rpc                                              *rpc.Client
	chainID                                          *big.Int
	genesis                                          common.Hash
	controller, broker, bonding, rounds, token, feed common.Address
	funder, lptFunder                                common.Address
	fundingKey                                       *ecdsa.PrivateKey
	contracts                                        map[string]abi.ABI
	cliAccounts                                      map[common.Address]*chainAccount
	goControls                                       map[common.Address]*goChainControl
}

func chainContext(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), envDuration(t, "E2E_RPC_TIMEOUT", "10s"))
}
func (c *chainFixture) request(t *testing.T, out any, method string, args ...any) {
	t.Helper()
	ctx, cancel := chainContext(t)
	defer cancel()
	require.NoError(t, c.rpc.CallContext(ctx, out, method, args...), "INFRASTRUCTURE: %s via %s", method, c.url)
}
func preflightChain(t *testing.T) *chainFixture {
	t.Helper()
	c := &chainFixture{url: os.Getenv("E2E_RPC_URL"), contracts: map[string]abi.ABI{}}
	c.setupURL = c.url
	require.NotEmpty(t, c.url, "INFRASTRUCTURE: E2E_RPC_URL is required")
	expected := os.Getenv("E2E_EXPECTED_CHAIN_ID")
	require.NotEmpty(t, expected, "INFRASTRUCTURE: E2E_EXPECTED_CHAIN_ID is required")
	require.True(t, common.IsHexAddress(os.Getenv("E2E_ETH_CONTROLLER")), "INFRASTRUCTURE: E2E_ETH_CONTROLLER must be a deployed Controller address")
	ctx, cancel := chainContext(t)
	defer cancel()
	var err error
	c.rpc, err = rpc.DialContext(ctx, c.url)
	require.NoError(t, err, "INFRASTRUCTURE: RPC endpoint %s", c.url)
	t.Cleanup(c.rpc.Close)
	var id hexutil.Big
	c.request(t, &id, "eth_chainId")
	c.chainID = new(big.Int).Set((*big.Int)(&id))
	var genesis struct{ Hash common.Hash }
	c.request(t, &genesis, "eth_getBlockByNumber", "0x0", false)
	require.NotEqual(t, common.Hash{}, genesis.Hash, "INFRASTRUCTURE: missing genesis block")
	c.genesis = genesis.Hash
	require.Equal(t, expected, c.chainID.String(), "INFRASTRUCTURE: unexpected chain")
	c.controller = common.HexToAddress(os.Getenv("E2E_ETH_CONTROLLER"))
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(contractABIs, &raw))
	for name, data := range raw {
		a, err := abi.JSON(strings.NewReader(string(data)))
		require.NoError(t, err)
		c.contracts[name] = a
	}
	var code hexutil.Bytes
	c.request(t, &code, "eth_getCode", c.controller, "latest")
	require.NotEmpty(t, code, "INFRASTRUCTURE: Controller has no code")
	for name, target := range map[string]*common.Address{"TicketBroker": &c.broker, "BondingManager": &c.bonding, "RoundsManager": &c.rounds, "LivepeerToken": &c.token} {
		value := c.call(t, "controller", c.controller, "getContract", crypto.Keccak256Hash([]byte(name)))
		*target = value[0].(common.Address)
		require.NotEqual(t, common.Address{}, *target, "INFRASTRUCTURE: Controller missing %s", name)
	}
	var block struct{ Hash common.Hash }
	c.request(t, &block, "eth_getBlockByNumber", "latest", false)
	data, err := c.contracts["controller"].Pack("getContract", crypto.Keccak256Hash([]byte("TicketBroker")))
	require.NoError(t, err)
	c.request(t, &code, "eth_call", map[string]any{"to": c.controller, "data": hexutil.Bytes(data)}, map[string]any{"blockHash": block.Hash, "requireCanonical": true})
	require.NotEmpty(t, code, "INFRASTRUCTURE: canonical contract reads")
	t.Logf("chain=%s controller=%s ticketBroker=%s", c.chainID, c.controller, c.broker)
	return c
}
func (c *chainFixture) call(t *testing.T, kind string, address common.Address, method string, args ...any) []any {
	t.Helper()
	a := c.contracts[kind]
	data, err := a.Pack(method, args...)
	require.NoError(t, err)
	var out hexutil.Bytes
	c.request(t, &out, "eth_call", map[string]any{"to": address, "data": hexutil.Bytes(data)}, "latest")
	values, err := a.Unpack(method, out)
	require.NoError(t, err, "%s", method)
	return values
}
func (c *chainFixture) configureFunding(t *testing.T) {
	t.Helper()
	var accounts []common.Address
	c.request(t, &accounts, "eth_accounts")
	if path := os.Getenv("E2E_FUNDER_KEY_FILE"); path != "" {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		c.fundingKey, err = crypto.HexToECDSA(strings.TrimSpace(strings.TrimPrefix(string(data), "0x")))
		require.NoError(t, err)
		c.funder = crypto.PubkeyToAddress(c.fundingKey.PublicKey)
	}
	if address := os.Getenv("E2E_FUNDER_ADDRESS"); address != "" {
		require.True(t, common.IsHexAddress(address))
		c.funder = common.HexToAddress(address)
	}
	if c.fundingKey != nil {
		require.Equal(t, crypto.PubkeyToAddress(c.fundingKey.PublicKey), c.funder, "INFRASTRUCTURE: funding key does not match E2E_FUNDER_ADDRESS")
	}
	if c.funder == (common.Address{}) {
		require.NotEmpty(t, accounts, "INFRASTRUCTURE: configure an unlocked funding account or E2E_FUNDER_KEY_FILE")
		c.funder = accounts[0]
	}
	var balance hexutil.Big
	c.request(t, &balance, "eth_getBalance", c.funder, "latest")
	require.Positive(t, (*big.Int)(&balance).Sign(), "INFRASTRUCTURE: funding account has no ETH")
	if address := os.Getenv("E2E_LPT_FUNDER_ADDRESS"); address != "" {
		require.True(t, common.IsHexAddress(address), "E2E_LPT_FUNDER_ADDRESS")
		c.lptFunder = common.HexToAddress(address)
	} else {
		largest := new(big.Int)
		for _, account := range append([]common.Address{c.funder}, accounts...) {
			balance := c.call(t, "livepeerToken", c.token, "balanceOf", account)[0].(*big.Int)
			if balance.Cmp(largest) > 0 {
				c.lptFunder = account
				largest = balance
			}
		}
	}
	if c.lptFunder == (common.Address{}) {
		c.lptFunder = c.funder
	}
}

func (c *chainFixture) paidRecipient(t *testing.T, isolated bool) *chainAccount {
	t.Helper()
	paidRecipients.Lock()
	// Registered before any fixture workers or snapshot cleanup: LIFO cleanup
	// releases the next fixture only after all of those have completed.
	t.Cleanup(paidRecipients.Unlock)
	if isolated {
		a := c.newAccount(t, true)
		c.activate(t, a)
		return a
	}
	identity := recipientChain{c.chainID.String(), c.genesis, c.controller}
	a, attempted := paidRecipients.accounts[identity]
	if !attempted {
		// A failed first setup must fail dependent fixtures, not create more
		// registered recipients. The account itself is published only on success.
		paidRecipients.accounts[identity] = nil
		// Compose's TMPDIR is within its shared run-root mount. Local mode uses
		// the host's private temporary directory. Neither belongs to testing.T.
		base := ""
		if composeMode() {
			root := os.Getenv("E2E_RUN_ROOT")
			require.NotEmpty(t, root, "INFRASTRUCTURE: Compose recipient needs the shared E2E_RUN_ROOT")
			base = filepath.Join(root, "tmp")
		}
		dir, err := os.MkdirTemp(base, "e2e-scope-recipient-")
		require.NoError(t, err)
		paidRecipients.dirs = append(paidRecipients.dirs, dir)
		a = c.createAccountIn(t, "node", dir)
		a.runScoped = true
		c.send(t, c.funder, c.fundingKey, common.Address{}, a.address, nil, ethAmount(t, "E2E_ACCOUNT_ETH", "1"))
		c.activate(t, a)
		// Failed funding/registration must never publish a partially set up R.
		paidRecipients.accounts[identity] = a
		t.Logf("run recipient activated=%s", a.address)
	} else {
		require.NotNil(t, a, "INFRASTRUCTURE: run recipient setup failed earlier; see the first paid fixture")
		if c.cliAccounts == nil {
			c.cliAccounts = make(map[common.Address]*chainAccount)
		}
		c.cliAccounts[a.address] = a
		c.waitAccountIdle(t, a.address)
		c.initializeRound(t, a)
		delegator := c.call(t, "bondingManager", c.bonding, "getDelegator", a.address)
		require.Positive(t, delegator[0].(*big.Int).Sign(), "run recipient lost its bond; staking cases must request an isolated recipient")
		require.Equal(t, a.address, delegator[2].(common.Address), "run recipient must remain self-bonded")
		if !c.inTranscoderPool(t, a.address) {
			// A displaced, still registered recipient needs only the difference
			// between the pool entry threshold and its existing total stake.
			// Repair membership even if deactivation is scheduled for next round.
			stake := c.call(t, "bondingManager", c.bonding, "transcoderTotalStake", a.address)[0].(*big.Int)
			additional := new(big.Int).Sub(c.selfBondAmount(t), stake)
			if additional.Sign() <= 0 {
				// The CLI skips unchanged registration settings and requires a
				// positive bond. One base unit updates membership in an open slot.
				additional.SetInt64(1)
			}
			t.Logf("run recipient stake adjustment=%s additional=%s LPT base units", a.address, additional)
			c.fundStake(t, a, additional)
			c.roundTransaction(t, a, "bond", additional, a.address)
		}
		if !c.recipientActive(t, a) {
			c.waitRecipientActive(t, a)
		}
		t.Logf("run recipient reused=%s", a.address)
	}
	return a
}

func cleanupPaidRecipients() error {
	paidRecipients.Lock()
	defer paidRecipients.Unlock()
	var first error
	for _, dir := range paidRecipients.dirs {
		if err := os.RemoveAll(dir); err != nil && first == nil {
			first = err
		}
	}
	paidRecipients.accounts = make(map[recipientChain]*chainAccount)
	paidRecipients.dirs = nil
	return first
}

func (c *chainFixture) waitAccountIdle(t *testing.T, address common.Address) {
	t.Helper()
	eventually(t, func() bool {
		var pending, mined hexutil.Uint64
		c.request(t, &pending, "eth_getTransactionCount", address, "pending")
		c.request(t, &mined, "eth_getTransactionCount", address, "latest")
		return pending == mined
	}, envDuration(t, "E2E_TRANSACTION_TIMEOUT", "45s"), 200*time.Millisecond, "INFRASTRUCTURE: outstanding transactions from %s before setup submission", address)
}
func ethAmount(t *testing.T, key, fallback string) *big.Int {
	t.Helper()
	amount := decimalWei(t, envDefault(key, fallback))
	require.NotEqual(t, -1, amount.Sign(), key)
	return amount
}
func (c *chainFixture) newAccount(t *testing.T, funded bool) *chainAccount {
	t.Helper()
	account := c.createAccount(t, "node")
	if funded {
		c.send(t, c.funder, c.fundingKey, common.Address{}, account.address, nil, ethAmount(t, "E2E_ACCOUNT_ETH", "1"))
	}
	return account
}
func (c *chainFixture) accountFromKey(t *testing.T, key *ecdsa.PrivateKey) *chainAccount {
	t.Helper()
	address := crypto.PubkeyToAddress(key.PublicKey)
	if c.cliAccounts == nil {
		c.cliAccounts = map[common.Address]*chainAccount{}
	}
	if account := c.cliAccounts[address]; account != nil {
		return account
	}
	dir := t.TempDir()
	password := uuid.NewString()
	account := &chainAccount{key: key, address: crypto.PubkeyToAddress(key.PublicKey), passwordFile: secretFile(t, dir, "password", password)}
	encoded, err := keystore.EncryptKey(&keystore.Key{Id: uuid.New(), Address: account.address, PrivateKey: key}, password, keystore.LightScryptN, keystore.LightScryptP)
	require.NoError(t, err)
	account.keystore = filepath.Join(dir, "account.json")
	require.NoError(t, os.WriteFile(account.keystore, encoded, 0600))
	c.cliAccounts[address] = account
	return account
}
func (c *chainFixture) send(t *testing.T, from common.Address, key *ecdsa.PrivateKey, deploy, to common.Address, data []byte, value *big.Int) *types.Receipt {
	t.Helper()
	receipt := c.submit(t, from, key, deploy, to, data, value)
	require.Equal(t, uint64(1), receipt.Status, "transaction %s reverted", receipt.TxHash)
	return receipt
}
func (c *chainFixture) submit(t *testing.T, from common.Address, key *ecdsa.PrivateKey, deploy, to common.Address, data []byte, value *big.Int) *types.Receipt {
	t.Helper()
	if a := c.cliAccounts[from]; a != nil && a.runScoped {
		c.waitAccountIdle(t, from)
	}
	var hash common.Hash
	if key == nil {
		tx := map[string]any{"from": from, "data": hexutil.Bytes(data), "value": (*hexutil.Big)(value), "gas": hexutil.Uint64(6000000)}
		if deploy == (common.Address{}) {
			tx["to"] = to
		}
		c.request(t, &hash, "eth_sendTransaction", tx)
	} else {
		var nonce hexutil.Uint64
		var gasPrice hexutil.Big
		c.request(t, &nonce, "eth_getTransactionCount", from, "pending")
		c.request(t, &gasPrice, "eth_gasPrice")
		var tx *types.Transaction
		if deploy != (common.Address{}) {
			tx = types.NewContractCreation(uint64(nonce), value, 6000000, (*big.Int)(&gasPrice), data)
		} else {
			tx = types.NewTransaction(uint64(nonce), to, value, 6000000, (*big.Int)(&gasPrice), data)
		}
		signed, err := types.SignTx(tx, types.LatestSignerForChainID(c.chainID), key)
		require.NoError(t, err)
		raw, err := signed.MarshalBinary()
		require.NoError(t, err)
		c.request(t, &hash, "eth_sendRawTransaction", hexutil.Bytes(raw))
	}
	var receipt *types.Receipt
	eventually(t, func() bool {
		ctx, cancel := chainContext(t)
		defer cancel()
		err := c.rpc.CallContext(ctx, &receipt, "eth_getTransactionReceipt", hash)
		return err == nil && receipt != nil
	}, envDuration(t, "E2E_TRANSACTION_TIMEOUT", "45s"), 200*time.Millisecond, "INFRASTRUCTURE: transaction %s not mined", hash)
	return receipt
}
func (c *chainFixture) transact(t *testing.T, a *chainAccount, kind string, to common.Address, method string, value *big.Int, args ...any) *types.Receipt {
	t.Helper()
	if kind == "ticketBroker" {
		command := []string{"ticketbroker"}
		switch method {
		case "fundDepositAndReserve":
			command = append(command, "fund", "--amount", args[0].(*big.Int).String(), "--reserve", args[1].(*big.Int).String(), "--base-units")
		case "unlock", "withdraw":
			command = append(command, method)
		case "cancelUnlock":
			command = append(command, "cancel-unlock")
		default:
			t.Fatalf("TicketBroker action %s has no CLI mapping", method)
		}
		return c.nodeTransaction(t, a, command...)
	}
	data, err := c.contracts[kind].Pack(method, args...)
	require.NoError(t, err)
	return c.send(t, a.address, a.key, common.Address{}, to, data, value)
}
func (c *chainFixture) fundPayer(t *testing.T, a *chainAccount, deposit, reserve *big.Int) {
	t.Helper()
	c.transact(t, a, "ticketBroker", c.broker, "fundDepositAndReserve", new(big.Int).Add(deposit, reserve), deposit, reserve)
}
func (c *chainFixture) activate(t *testing.T, a *chainAccount) {
	t.Helper()
	amount := c.selfBondAmount(t)
	t.Logf("fresh orchestrator=%s self-bond=%s LPT base units", a.address, amount)
	c.fundStake(t, a, amount)
	c.initializeRound(t, a)
	c.roundTransaction(t, a, "bond", amount, a.address)
	c.waitUnlockedRound(t, a)
	c.roundTransaction(t, a, "transcoder", big.NewInt(100000), big.NewInt(500000))
	c.waitRecipientActive(t, a)
}

func (c *chainFixture) fundStake(t *testing.T, a *chainAccount, amount *big.Int) {
	t.Helper()
	balance := c.call(t, "livepeerToken", c.token, "balanceOf", a.address)[0].(*big.Int)
	missing := new(big.Int).Sub(amount, balance)
	if missing.Sign() <= 0 {
		return
	}
	c.ensureLPT(t, c.lptFunder, missing)
	var transferAccount *chainAccount
	if c.lptFunder == c.funder {
		if c.fundingKey != nil {
			transferAccount = c.accountFromKey(t, c.fundingKey)
		}
	}
	c.transferLPT(t, c.lptFunder, transferAccount, a.address, missing)
}

func (c *chainFixture) recipientActive(t *testing.T, a *chainAccount) bool {
	t.Helper()
	if !c.call(t, "bondingManager", c.bonding, "isActiveTranscoder", a.address)[0].(bool) {
		return false
	}
	activated := c.transcoderRound(t, a.address, "activationRound")
	initialized := c.call(t, "roundsManager", c.rounds, "lastInitializedRound")[0].(*big.Int)
	return initialized.Cmp(activated) >= 0
}

func (c *chainFixture) waitRecipientActive(t *testing.T, a *chainAccount) {
	t.Helper()
	deadline := time.Now().Add(envDuration(t, "E2E_ACTIVATION_TIMEOUT", "3m"))
	for time.Now().Before(deadline) {
		c.initializeRound(t, a)
		if c.recipientActive(t, a) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("INFRASTRUCTURE: freshly registered orchestrator did not become active before E2E_ACTIVATION_TIMEOUT; check round length, initialization and transcoder pool stake")
}

func (c *chainFixture) inTranscoderPool(t *testing.T, address common.Address) bool {
	t.Helper()
	size := c.call(t, "bondingManager", c.bonding, "getTranscoderPoolSize")[0].(*big.Int)
	require.True(t, size.IsUint64() && size.Uint64() <= 10000, "INFRASTRUCTURE: pool traversal limit")
	member := c.call(t, "bondingManager", c.bonding, "getFirstTranscoderInPool")[0].(common.Address)
	for n := uint64(0); n < size.Uint64(); n++ {
		require.NotEqual(t, common.Address{}, member, "INFRASTRUCTURE: inconsistent transcoder pool")
		if member == address {
			return true
		}
		member = c.call(t, "bondingManager", c.bonding, "getNextTranscoderInPool", member)[0].(common.Address)
	}
	return false
}

func (c *chainFixture) selfBondAmount(t *testing.T) *big.Int {
	t.Helper()
	amount := ethAmount(t, "E2E_BOND_LPT", "1000")
	require.Positive(t, amount.Sign(), "E2E_BOND_LPT")
	size := c.call(t, "bondingManager", c.bonding, "getTranscoderPoolSize")[0].(*big.Int)
	maximum := c.call(t, "bondingManager", c.bonding, "getTranscoderPoolMaxSize")[0].(*big.Int)
	if size.Cmp(maximum) >= 0 && size.Sign() > 0 {
		// A fresh transcoder needs to displace the pool's smallest stake, not
		// its leader. Open slots need only the configured minimum self-bond.
		require.True(t, size.IsUint64(), "INFRASTRUCTURE: invalid transcoder pool size")
		require.LessOrEqual(t, size.Uint64(), uint64(10000), "INFRASTRUCTURE: pool traversal limit")
		tail := c.call(t, "bondingManager", c.bonding, "getFirstTranscoderInPool")[0].(common.Address)
		for n := uint64(1); n < size.Uint64(); n++ {
			tail = c.call(t, "bondingManager", c.bonding, "getNextTranscoderInPool", tail)[0].(common.Address)
			require.NotEqual(t, common.Address{}, tail, "INFRASTRUCTURE: inconsistent transcoder pool")
		}
		stake := c.call(t, "bondingManager", c.bonding, "transcoderTotalStake", tail)[0].(*big.Int)
		target := new(big.Int).Add(stake, ethAmount(t, "E2E_BOND_MARGIN_LPT", "1000"))
		require.Greater(t, target.Cmp(stake), 0, "E2E_BOND_MARGIN_LPT must be positive for a full pool")
		if target.Cmp(amount) > 0 {
			amount = target
		}
	}
	return amount
}
func (c *chainFixture) initializeRound(t *testing.T, a *chainAccount) {
	t.Helper()
	if !c.call(t, "roundsManager", c.rounds, "currentRoundInitialized")[0].(bool) {
		output, err := c.nodeCLI(t, a, true, "round", "initialize")
		if err != nil {
			require.True(t, c.call(t, "roundsManager", c.rounds, "currentRoundInitialized")[0].(bool), "round initialization failed: %v\n%s", err, output)
		}
	}
}
func chainFlags(c *chainFixture, a *chainAccount) []string {
	return []string{"-network", "devenv", "-ethUrl", c.url, "-ethController", c.controller.Hex(), "-ethKeystorePath", filepath.Dir(a.keystore), "-ethPassword", a.passwordFile, "-ethAcctAddr", a.address.Hex(), "-initializeRound=false", "-reward=false"}
}

// This test-only AggregatorV3 fixture implements decimals, description and
// latestRoundData plus setters. It has no production role or protocol state.
func oracleBytecode(answer *big.Int) []byte {
	runtime := []byte{0x60, 0, 0x35, 0x60, 0xe0, 0x1c}
	labels := map[string]int{}
	refs := map[string]int{}
	signatures := []string{"decimals()", "description()", "latestRoundData()", "setAnswer(uint256)", "setUpdatedAt(uint256)"}
	for _, sig := range signatures {
		runtime = append(runtime, 0x80, 0x63)
		runtime = append(runtime, crypto.Keccak256([]byte(sig))[:4]...)
		runtime = append(runtime, 0x14, 0x61, 0, 0, 0x57)
		refs[sig] = len(runtime) - 3
	}
	runtime = append(runtime, 0x60, 0, 0x60, 0, 0xfd)
	push := func(n int) { runtime = append(runtime, 0x60, byte(n)) }
	store := func(n int) { push(n); runtime = append(runtime, 0x52) }
	ret := func(n int) { push(n); push(0); runtime = append(runtime, 0xf3) }
	for _, sig := range signatures {
		labels[sig] = len(runtime)
		runtime = append(runtime, 0x5b, 0x50)
		switch sig {
		case "decimals()":
			push(8)
			store(0)
			ret(32)
		case "description()":
			push(32)
			store(0)
			push(9)
			store(32)
			runtime = append(runtime, 0x7f)
			word := make([]byte, 32)
			copy(word, "ETH / USD")
			runtime = append(runtime, word...)
			store(64)
			ret(96)
		case "latestRoundData()":
			push(1)
			store(0)
			push(0)
			runtime = append(runtime, 0x54)
			store(32)
			push(1)
			runtime = append(runtime, 0x54)
			store(64)
			push(1)
			runtime = append(runtime, 0x54)
			store(96)
			push(1)
			store(128)
			ret(160)
		default:
			push(4)
			runtime = append(runtime, 0x35)
			if sig == "setAnswer(uint256)" {
				push(0)
			} else {
				push(1)
			}
			runtime = append(runtime, 0x55)
			if sig == "setAnswer(uint256)" {
				// A changed oracle observation advances its timestamp as a real
				// aggregator update does; go-livepeer ignores unchanged timestamps.
				runtime = append(runtime, 0x42, 0x60, 1, 0x55)
			}
			ret(0)
		}
	}
	for sig, offset := range refs {
		runtime[offset] = byte(labels[sig] >> 8)
		runtime[offset+1] = byte(labels[sig])
	}
	init := []byte{0x7f}
	init = append(init, common.LeftPadBytes(answer.Bytes(), 32)...)
	init = append(init, 0x60, 0, 0x55, 0x42, 0x60, 1, 0x55)
	offset := len(init) + 15
	n := len(runtime)
	init = append(init, 0x61, byte(n>>8), byte(n), 0x61, byte(offset>>8), byte(offset), 0x60, 0, 0x39, 0x61, byte(n>>8), byte(n), 0x60, 0, 0xf3)
	return append(init, runtime...)
}
func (c *chainFixture) setupFeed(t *testing.T, a *chainAccount) {
	t.Helper()
	if address := os.Getenv("E2E_PRICE_FEED_ADDRESS"); address != "" {
		require.True(t, common.IsHexAddress(address))
		c.feed = common.HexToAddress(address)
		return
	}
	usd, ok := new(big.Int).SetString(envDefault("E2E_ETH_USD", "1000"), 10)
	require.True(t, ok)
	usd.Mul(usd, big.NewInt(100000000))
	receipt := c.send(t, a.address, a.key, common.HexToAddress("0x1"), common.Address{}, oracleBytecode(usd), new(big.Int))
	c.feed = receipt.ContractAddress
	require.NotEqual(t, common.Address{}, c.feed)
	t.Logf("deployed test ETH/USD feed %s", c.feed)
}
func (c *chainFixture) requireFinality(t *testing.T) {
	t.Helper()
	var block json.RawMessage
	ctx, cancel := chainContext(t)
	defer cancel()
	err := c.rpc.CallContext(ctx, &block, "eth_getBlockByNumber", "finalized", false)
	require.NoError(t, err, "INFRASTRUCTURE: RPC must support finalized blocks for Node redemption confirmation; latest is not a substitute")
	require.NotEqual(t, "null", string(block), "INFRASTRUCTURE: no finalized block")
}
func (c *chainFixture) checkControl(t *testing.T) {
	t.Helper()
	var modules map[string]string
	c.request(t, &modules, "rpc_modules")
	require.Contains(t, modules, "evm", "INFRASTRUCTURE: snapshot/revert, expiry and reorg tests need evm controls")
}
func (c *chainFixture) requireControl(t *testing.T, recipient ...*chainAccount) string {
	t.Helper()
	c.checkControl(t)
	var snapshot string
	c.request(t, &snapshot, "evm_snapshot")
	t.Cleanup(func() {
		var restored bool
		ctx, cancel := chainContext(t)
		defer cancel()
		if err := c.rpc.CallContext(ctx, &restored, "evm_revert", snapshot); err != nil || !restored {
			t.Errorf("INFRASTRUCTURE: snapshot restore failed: %v", err)
			return
		}
		if len(recipient) > 0 {
			c.initializeRound(t, recipient[0])
			require.True(t, c.recipientActive(t, recipient[0]), "recipient remains active after snapshot restore")
		}
	})
	return snapshot
}
func (c *chainFixture) setOracle(t *testing.T, a *chainAccount, method string, value *big.Int) {
	data := append(crypto.Keccak256([]byte(method + "(uint256)"))[:4], common.LeftPadBytes(value.Bytes(), 32)...)
	c.send(t, a.address, a.key, common.Address{}, c.feed, data, new(big.Int))
}
func (c *chainFixture) block(t *testing.T) uint64 {
	var n hexutil.Uint64
	c.request(t, &n, "eth_blockNumber")
	return uint64(n)
}
func unhex(t *testing.T, s string) []byte {
	t.Helper()
	data, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	require.NoError(t, err)
	return data
}

func (c *chainFixture) requireCanonicalHeaders(t *testing.T) {
	t.Helper()
	var raw json.RawMessage
	c.request(t, &raw, "eth_getBlockByNumber", "latest", false)
	var header types.Header
	require.NoError(t, json.Unmarshal(raw, &header))
	var reported struct{ Hash common.Hash }
	require.NoError(t, json.Unmarshal(raw, &reported))
	detail := "reported block hash differs from Ethereum RLP header hash"
	if reported.Hash != header.Hash() {
		originalMiner := header.Coinbase
		header.Coinbase = common.Address{}
		if reported.Hash == header.Hash() {
			detail += "; RPC substitutes the Clique signer (" + originalMiner.Hex() + ") for the header's zero coinbase"
		}
		header.Coinbase = originalMiner
	}
	require.Equal(t, reported.Hash, header.Hash(), "INFRASTRUCTURE: %s; clearinghouse's Ethereum header decoder cannot verify canonical ancestry", detail)
}

func (c *chainFixture) waitUnlockedRound(t *testing.T, a *chainAccount) {
	t.Helper()
	length := c.call(t, "roundsManager", c.rounds, "roundLength")[0].(*big.Int)
	window := new(big.Int).Div(length, big.NewInt(4))
	eventually(t, func() bool {
		c.initializeRound(t, a)
		start := c.call(t, "roundsManager", c.rounds, "currentRoundStartBlock")[0].(*big.Int)
		elapsed := new(big.Int).Sub(new(big.Int).SetUint64(c.block(t)), start)
		// Multi-step CLIs may approve, bond, register and set a URI. Begin
		// early enough to avoid crossing the short devnet round boundary.
		return elapsed.Sign() >= 0 && elapsed.Cmp(window) < 0 && !c.call(t, "roundsManager", c.rounds, "currentRoundLocked")[0].(bool)
	}, envDuration(t, "E2E_ACTIVATION_TIMEOUT", "3m"), 500*time.Millisecond, "INFRASTRUCTURE: wait for an initialized registration round with time for the CLI workflow")
}

// The short devnet rounds can change between the read and inclusion. Retry only
// these reverted setup transactions; successful transactions are never replayed.
func (c *chainFixture) roundTransaction(t *testing.T, a *chainAccount, method string, args ...any) {
	t.Helper()
	var command []string
	switch method {
	case "bond":
		command = []string{"stake", "bond", args[1].(common.Address).Hex(), "--amount", args[0].(*big.Int).String(), "--base-units"}
	case "transcoder":
		command = []string{"orchestrator", "register", "--reward-cut", "10", "--fee-cut", "50", "--service-uri", "http://127.0.0.1:1"}
	default:
		t.Fatalf("round setup action %s has no CLI mapping", method)
	}
	eventually(t, func() bool {
		c.waitUnlockedRound(t, a)
		if method == "transcoder" && c.call(t, "bondingManager", c.bonding, "isActiveTranscoder", a.address)[0].(bool) {
			last := c.transcoderRound(t, a.address, "lastRewardRound")
			round := c.call(t, "roundsManager", c.rounds, "lastInitializedRound")[0].(*big.Int)
			if last.Cmp(round) < 0 {
				c.nodeTransaction(t, a, "reward", "call")
			}
		}
		output, err := c.nodeCLI(t, a, true, command...)
		if err == nil {
			return true
		}
		// Only retry a known round-boundary rejection. Other CLI errors remain
		// failures, and a confirmed workflow is never replayed.
		locked := c.call(t, "roundsManager", c.rounds, "currentRoundLocked")[0].(bool)
		initialized := c.call(t, "roundsManager", c.rounds, "currentRoundInitialized")[0].(bool)
		// Node redacts RPC revert text. An uninitialized round after the
		// preflight, together with a failed simulation, also proves this race.
		uninitialized := !initialized && strings.Contains(string(output), "transaction simulation failed")
		if !locked && !uninitialized && !strings.Contains(string(output), "round") {
			t.Fatalf("Node CLI %s: %v\n%s", method, err, output)
		}
		t.Logf("round-boundary CLI retry %s: %v", method, err)
		return false
	}, envDuration(t, "E2E_ACTIVATION_TIMEOUT", "3m"), 500*time.Millisecond, "INFRASTRUCTURE: %s registration failed in unlocked rounds", method)
}

func (c *chainFixture) transcoderRound(t *testing.T, address common.Address, field string) *big.Int {
	t.Helper()
	values := c.call(t, "bondingManager", c.bonding, "getTranscoder", address)
	for i, output := range c.contracts["bondingManager"].Methods["getTranscoder"].Outputs {
		if output.Name == field {
			value, ok := values[i].(*big.Int)
			require.True(t, ok, "getTranscoder.%s must be uint256", field)
			return value
		}
	}
	t.Fatalf("getTranscoder output %q missing from fixture ABI", field)
	return nil
}

func decimalWei(t *testing.T, s string) *big.Int {
	r, ok := new(big.Rat).SetString(s)
	require.True(t, ok)
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)))
	require.True(t, r.IsInt())
	return new(big.Int).Set(r.Num())
}
func senderEscrow(t *testing.T, c *chainFixture, a common.Address) (*big.Int, *big.Int, *big.Int) {
	v := c.call(t, "ticketBroker", c.broker, "getSenderInfo", a)
	sender := reflect.Indirect(reflect.ValueOf(v[0]))
	reserve := reflect.Indirect(reflect.ValueOf(v[1]))
	return sender.FieldByName("Deposit").Interface().(*big.Int), reserve.FieldByName("FundsRemaining").Interface().(*big.Int), sender.FieldByName("WithdrawRound").Interface().(*big.Int)
}
