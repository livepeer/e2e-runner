package e2escope

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Fresh protocol accounts come from the production Node CLI. go-livepeer's
// startup creation path is exercised separately by the CLI comparison suite.
func (c *chainFixture) createAccount(t *testing.T, backend string) *chainAccount {
	t.Helper()
	dir := t.TempDir()
	password := uuid.NewString()
	a := &chainAccount{passwordFile: secretFile(t, dir, "password", password)}
	var reported common.Address
	switch backend {
	case "node":
		a.keystore = filepath.Join(dir, "account.json")
		output, err := nodeAccountCreate(t, dir, a)
		require.NoError(t, err, "Node account create: %s", output)
		var record struct{ Address common.Address }
		require.NoError(t, json.Unmarshal(output, &record), "Node account create JSON: %s", output)
		reported = record.Address
	case "go-livepeer":
		// Startup creation takes a password value, while reopening an existing
		// account accepts a password file. Keep the creation value in a private
		// config file so it never appears in process arguments or retained logs.
		config := secretFile(t, dir, "create.conf", "ethPassword "+password+"\n")
		keyDir := filepath.Join(dir, "keystore")
		require.NoError(t, os.Mkdir(keyDir, 0700))
		cliAddr := freeAddr(t)
		args := []string{"-config", config, "-gateway", "-network", "devenv", "-ethUrl", c.url, "-ethController", c.controller.Hex(), "-ethKeystorePath", keyDir, "-initializeRound=false", "-reward=false", "-datadir", dir, "-httpAddr", freeAddr(t), "-cliAddr", cliAddr, "-rtmpAddr", freeAddr(t), "-maxPricePerUnit", "1", "-orchAddr", "http://127.0.0.1:1"}
		p := startChild(t, "go-account-create", binary(t, backend), dir, args)
		waitForHTTP(t, "GET", "http://"+cliAddr+"/status", 200, p.logPath)
		p.stop()
		files, err := filepath.Glob(filepath.Join(keyDir, "UTC--*"))
		require.NoError(t, err)
		require.Len(t, files, 1, "go-livepeer startup must create one fresh account")
		a.keystore = files[0]
	default:
		t.Fatalf("unknown account backend %q", backend)
	}
	encoded, err := os.ReadFile(a.keystore)
	require.NoError(t, err)
	key, err := keystore.DecryptKey(encoded, password)
	require.NoError(t, err, "%s created keystore must decrypt with its password", backend)
	a.key, a.address = key.PrivateKey, crypto.PubkeyToAddress(key.PrivateKey.PublicKey)
	require.NotEqual(t, common.Address{}, a.address)
	require.Equal(t, a.address, key.Address, "keystore address must match its private key")
	if backend == "node" {
		require.Equal(t, a.address, reported, "CLI address must match its created keystore")
	}
	info, err := os.Stat(a.keystore)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	if c.cliAccounts == nil {
		c.cliAccounts = map[common.Address]*chainAccount{}
	}
	require.NotContains(t, c.cliAccounts, a.address, "account creation must generate a fresh address")
	c.cliAccounts[a.address] = a
	return a
}

func nodeAccountCreate(t *testing.T, dir string, a *chainAccount) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_CLI_TIMEOUT", "2m"))
	defer cancel()
	cmd := fixtureCommand(t, ctx, "node-chain", binary(t, "node"), dir, "chain", "account", "create", "--config", secretFile(t, dir, "create.toml", ""), "--keystore-file", a.keystore, "--keystore-password-file", a.passwordFile, "--output", "json")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	retainCLIOutput(t, "node-account-create", append(bytes.Clone(output), stderr.Bytes()...))
	if err != nil {
		return append(output, stderr.Bytes()...), err
	}
	return output, nil
}

// Protocol setup uses the production CLI. Raw RPC is retained for ETH funding
// from unlocked RPC accounts, test contract deployment/mutation, chain controls,
// and deliberately invalid transactions that the CLI correctly refuses to send.
func (c *chainFixture) nodeCLI(t *testing.T, a *chainAccount, submit bool, command ...string) ([]byte, error) {
	t.Helper()
	bin := binary(t, "node")
	dir := filepath.Dir(a.keystore)
	args := []string{"chain", "--rpc-url-file", secretFile(t, dir, "chain-rpc", c.url), "--chain-id", c.chainID.String(), "--controller-address", c.controller.Hex(), "--account", a.address.Hex(), "--output", "json"}
	args = append(args, command...)
	if submit {
		args = append(args, "--submit", "--keystore-file", a.keystore, "--keystore-password-file", a.passwordFile, "--transaction-timeout", envDefault("E2E_TRANSACTION_TIMEOUT", "45s"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_CLI_TIMEOUT", "2m"))
	defer cancel()
	cmd := fixtureCommand(t, ctx, "node-chain", bin, dir, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	retainCLIOutput(t, "node-chain", append(bytes.Clone(output), stderr.Bytes()...))
	if err != nil {
		return append(output, stderr.Bytes()...), err
	}
	return output, nil
}

func (c *chainFixture) nodeTransaction(t *testing.T, a *chainAccount, command ...string) *types.Receipt {
	t.Helper()
	output, err := c.nodeCLI(t, a, true, command...)
	require.NoError(t, err, "Node CLI %v: %s", command, output)
	decoder := json.NewDecoder(bytes.NewReader(output))
	var hash common.Hash
	for {
		var record struct {
			Submitted bool        `json:"submitted"`
			Hash      common.Hash `json:"transaction_hash"`
		}
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "Node CLI JSON: %s", output)
		if record.Submitted {
			hash = record.Hash
		}
	}
	require.NotEqual(t, common.Hash{}, hash, "CLI must report a submitted transaction: %s", output)
	var receipt *types.Receipt
	c.request(t, &receipt, "eth_getTransactionReceipt", hash)
	require.NotNil(t, receipt)
	require.Equal(t, uint64(1), receipt.Status)
	return receipt
}

func (c *chainFixture) transferLPT(t *testing.T, from common.Address, keyAccount *chainAccount, to common.Address, amount *big.Int) {
	t.Helper()
	if keyAccount == nil {
		keyAccount = c.cliAccounts[from]
	}
	if keyAccount != nil {
		c.nodeTransaction(t, keyAccount, "token", "transfer", to.Hex(), "--amount", amount.String(), "--base-units")
		return
	}
	// Node requires a local keystore for submission; an operator-selected
	// account unlocked only at the RPC cannot be used by that CLI.
	t.Logf("LPT transfer from RPC-held account %s uses eth_sendTransaction; configure E2E_FUNDER_KEY_FILE for Node CLI signing", from)
	data, err := c.contracts["livepeerToken"].Pack("transfer", to, amount)
	require.NoError(t, err)
	c.send(t, from, nil, common.Address{}, c.token, data, new(big.Int))
}

type goChainControl struct {
	base    string
	process *childProcess
}

func (c *chainFixture) goControl(t *testing.T, a *chainAccount, addresses ...string) *goChainControl {
	t.Helper()
	if c.goControls == nil {
		c.goControls = map[common.Address]*goChainControl{}
	}
	if control := c.goControls[a.address]; control != nil {
		return control
	}
	if len(addresses) == 0 {
		addresses = []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	}
	require.Len(t, addresses, 3)
	cliAddr := addresses[0]
	dir := t.TempDir()
	args := []string{"-gateway", "-enableCliTxRoutes", "-httpAddr", addresses[1], "-cliAddr", cliAddr, "-rtmpAddr", addresses[2], "-datadir", dir, "-maxPricePerUnit", "1", "-orchAddr", "http://127.0.0.1:1"}
	args = append(args, chainFlags(c, a)...)
	p := startChild(t, "go-chain-control", binary(t, "go-livepeer"), dir, args)
	control := &goChainControl{base: publicURL(t, "http://"+cliAddr), process: p}
	waitForHTTP(t, "GET", control.base+"/status", 200, p.logPath)
	c.goControls[a.address] = control
	return control
}

var cliMenu = regexp.MustCompile(`(?m)^(\d+)\. ([^\r\n]+)\r?$`)

// Drive the actual interactive go-livepeer CLI. Select by menu label because
// numbering depends on node role and chain; assert chain effects separately.
func (c *chainFixture) goCLI(t *testing.T, a *chainAccount, choice string, inputs ...string) {
	t.Helper()
	control := c.goControl(t, a)
	target, err := url.Parse(control.base)
	require.NoError(t, err)
	var requests, failures lockedBuffer
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(r *http.Response) error {
		if r.Request.Method == http.MethodPost {
			_, _ = requests.Write([]byte(r.Request.Method + " " + r.Request.URL.Path + ": " + r.Status + "\n"))
			if r.StatusCode < 200 || r.StatusCode >= 300 {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					return err
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				_, _ = failures.Write(append([]byte(r.Request.URL.Path+": "), body...))
			}
		}
		return nil
	}
	server := httptest.NewServer(proxy)
	defer server.Close()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), envDuration(t, "E2E_CLI_TIMEOUT", "2m"))
	defer cancel()
	cmd := fixtureCommand(t, ctx, "go-livepeer-cli", binary(t, "go-livepeer-cli"), "", "-host", host, "-http", port)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	var output lockedBuffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	waited := false
	defer func() {
		cancel()
		_ = stdin.Close()
		if !waited {
			_ = cmd.Wait()
		}
		retainCLIOutput(t, "go-livepeer-cli", []byte(output.String()+"\n"+requests.String()+failures.String()))
	}()
	var selectID, exitID string
	eventually(t, func() bool {
		for _, match := range cliMenu.FindAllStringSubmatch(output.String(), -1) {
			if match[2] == choice {
				selectID = match[1]
			}
			if match[2] == "Exit" {
				exitID = match[1]
			}
		}
		return selectID != "" && exitID != ""
	}, envDuration(t, "E2E_READY_TIMEOUT", "30s"), 50*time.Millisecond, "CLI menu %q: %s", choice, output.String())
	_, err = io.WriteString(stdin, strings.Join(append(append([]string{selectID}, inputs...), exitID), "\n")+"\n")
	require.NoError(t, err)
	err = cmd.Wait()
	waited = true
	require.NoError(t, err, "go-livepeer CLI %q: %s", choice, output.String())
	require.NotEmpty(t, requests.String(), "go-livepeer CLI must submit the selected action")
	require.Empty(t, failures.String(), "go-livepeer CLI %q HTTP failure: %s", choice, failures.String())
}

func retainCLIOutput(t *testing.T, label string, output []byte) {
	t.Helper()
	dir := envDefault("E2E_ARTIFACT_DIR", "artifacts")
	require.NoError(t, os.MkdirAll(dir, 0755))
	f, err := os.CreateTemp(dir, label+"-*.log")
	require.NoError(t, err)
	_, err = f.Write(output)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func (c *chainFixture) ensureLPT(t *testing.T, target common.Address, needed *big.Int) {
	t.Helper()
	balance := c.call(t, "livepeerToken", c.token, "balanceOf", target)[0].(*big.Int)
	if balance.Cmp(needed) >= 0 {
		return
	}
	require.Equal(t, "true", envDefault("E2E_AUTO_FAUCET", "true"), "INFRASTRUCTURE: insufficient LPT and automatic faucet funding disabled")
	faucet := c.call(t, "controller", c.controller, "getContract", crypto.Keccak256Hash([]byte("LivepeerTokenFaucet")))[0].(common.Address)
	require.NotEqual(t, common.Address{}, faucet, "INFRASTRUCTURE: Controller has no faucet")
	faucetABI, err := abi.JSON(strings.NewReader(`[{"type":"function","name":"requestAmount","inputs":[],"outputs":[{"type":"uint256"}]},{"type":"function","name":"nextValidRequest","inputs":[{"type":"address"}],"outputs":[{"type":"uint256"}]}]`))
	require.NoError(t, err)
	c.contracts["faucet"] = faucetABI
	dose := c.call(t, "faucet", faucet, "requestAmount")[0].(*big.Int)
	require.Positive(t, dose.Sign(), "INFRASTRUCTURE: zero faucet amount")
	missing := new(big.Int).Sub(needed, balance)
	requests := new(big.Int).Div(new(big.Int).Add(missing, new(big.Int).Sub(dose, big.NewInt(1))), dose)
	limit, err := strconv.ParseUint(envDefault("E2E_FAUCET_MAX_REQUESTS", "500"), 10, 64)
	require.NoError(t, err)
	require.True(t, requests.IsUint64() && requests.Uint64() <= limit, "INFRASTRUCTURE: need %s faucet requests; limit=%d", requests, limit)
	faucetBalance := c.call(t, "livepeerToken", c.token, "balanceOf", faucet)[0].(*big.Int)
	require.GreaterOrEqual(t, faucetBalance.Cmp(new(big.Int).Mul(requests, dose)), 0, "INFRASTRUCTURE: faucet cannot cover required LPT")
	deadline := time.Now().Add(envDuration(t, "E2E_FAUCET_TIMEOUT", "45m"))
	// Claims run serially and stop their control node before the next starts,
	// so a large top-up needs only three reserved ports for the entire batch.
	addresses := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	t.Logf("requesting %s faucet claims via go-livepeer to cover %s LPT base units for %s", requests, missing, target)
	for n := uint64(0); n < requests.Uint64(); n++ {
		require.True(t, time.Now().Before(deadline), "INFRASTRUCTURE: faucet funding deadline exceeded after %d claims", n)
		// The deployed faucet grants 10 LPT per account per hour. Each claim
		// belongs to a fresh disposable test wallet, then funds the fixture.
		bridge := c.newAccount(t, true)
		require.Zero(t, c.call(t, "faucet", faucet, "nextValidRequest", bridge.address)[0].(*big.Int).Sign())
		before := c.call(t, "livepeerToken", c.token, "balanceOf", bridge.address)[0].(*big.Int)
		c.goControl(t, bridge, addresses...)
		c.goCLI(t, bridge, "Get test LPT")
		after := c.call(t, "livepeerToken", c.token, "balanceOf", bridge.address)[0].(*big.Int)
		require.Equal(t, dose.String(), new(big.Int).Sub(after, before).String(), "go-livepeer faucet claim must increase LPT by requestAmount")
		c.transferLPT(t, bridge.address, bridge, target, after)
		c.goControls[bridge.address].process.stop()
		delete(c.goControls, bridge.address)
		if (n+1)%10 == 0 || n+1 == requests.Uint64() {
			t.Logf("faucet funding: %d/%s claims transferred", n+1, requests)
		}
	}
	require.GreaterOrEqual(t, c.call(t, "livepeerToken", c.token, "balanceOf", target)[0].(*big.Int).Cmp(needed), 0, "faucet replenishment must cover stake")
}

func TestOnchainCLI(t *testing.T) {
	fixturePoolFor(t)
	c := preflightChain(t)
	c.configureFunding(t)
	for _, backend := range []string{"node", "go-livepeer"} {
		t.Run(backend, func(t *testing.T) {
			a := c.createAccount(t, backend)
			if backend == "node" {
				before, err := os.ReadFile(a.keystore)
				require.NoError(t, err)
				output, err := nodeAccountCreate(t, filepath.Dir(a.keystore), a)
				require.Error(t, err, "account create must refuse to overwrite an existing keystore: %s", output)
				require.Contains(t, string(output), "already exists", "creation must fail because the keystore exists")
				after, err := os.ReadFile(a.keystore)
				require.NoError(t, err)
				require.Equal(t, before, after, "refused creation must preserve the existing account")
			}
			c.send(t, c.funder, c.fundingKey, common.Address{}, a.address, nil, ethAmount(t, "E2E_ACCOUNT_ETH", "1"))
			if backend == "node" {
				c.initializeRound(t, a)
			} else if !c.call(t, "roundsManager", c.rounds, "currentRoundInitialized")[0].(bool) {
				c.goCLI(t, a, `Invoke "initialize round"`)
				require.True(t, c.call(t, "roundsManager", c.rounds, "currentRoundInitialized")[0].(bool))
			}
			before := c.call(t, "livepeerToken", c.token, "balanceOf", a.address)[0].(*big.Int)
			c.goCLI(t, a, "Get test LPT")
			after := c.call(t, "livepeerToken", c.token, "balanceOf", a.address)[0].(*big.Int)
			require.Greater(t, after.Cmp(before), 0)
			to := c.newAccount(t, false)
			deposit, reserve := big.NewInt(1000000000000000), big.NewInt(2000000000000000)
			if backend == "node" {
				c.transferLPT(t, a.address, a, to.address, big.NewInt(1))
				c.fundPayer(t, a, deposit, reserve)
				c.transact(t, a, "ticketBroker", c.broker, "unlock", new(big.Int))
				c.transact(t, a, "ticketBroker", c.broker, "cancelUnlock", new(big.Int))
			} else {
				c.goCLI(t, a, `Invoke "transfer" (LPT)`, to.address.Hex(), "1", "y")
				c.goCLI(t, a, `Invoke "deposit broadcasting funds" (ETH)`, "0.001", "0.002")
				c.goCLI(t, a, `Invoke "unlock broadcasting funds"`, "y")
				c.goCLI(t, a, `Invoke "cancel unlock of broadcasting funds"`, "y")
			}
			require.Equal(t, "1", c.call(t, "livepeerToken", c.token, "balanceOf", to.address)[0].(*big.Int).String())
			d, r, withdrawal := senderEscrow(t, c, a.address)
			require.Equal(t, deposit.String(), d.String())
			require.Equal(t, reserve.String(), r.String())
			require.Zero(t, withdrawal.Sign())
			serviceURI := "http://127.0.0.1:1"
			c.waitUnlockedRound(t, a)
			if backend == "node" {
				c.nodeTransaction(t, a, "orchestrator", "register", "--amount", "1", "--base-units", "--reward-cut", "10", "--fee-cut", "50", "--service-uri", serviceURI)
			} else {
				// Seed the URI via Node so go-livepeer's interactive wizard does
				// not need an external public-IP lookup for its default.
				c.nodeTransaction(t, a, "orchestrator", "set-config", "--service-uri", serviceURI)
				c.goControls[a.address].process.stop()
				c.setupFeed(t, a)
				orch := startOrchestrator(t, orchestratorOptions{backend: "go-livepeer", account: a, chain: c, goExtra: []string{"-enableCliTxRoutes"}})
				c.goControls[a.address] = &goChainControl{base: orch.cliBase, process: orch.process}
				c.waitUnlockedRound(t, a)
				c.goCLI(t, a, `Invoke multi-step "become an orchestrator"`, "10", "50", "1", "Wei", "1", serviceURI, "1")
			}
			stake := c.call(t, "bondingManager", c.bonding, "getDelegator", a.address)
			require.Equal(t, "1", stake[0].(*big.Int).String(), "both CLIs bond exactly one LPT base unit")
			commission := c.call(t, "bondingManager", c.bonding, "getTranscoder", a.address)
			require.Equal(t, "100000", commission[1].(*big.Int).String())
			require.Equal(t, "500000", commission[2].(*big.Int).String())
			registry := c.call(t, "controller", c.controller, "getContract", crypto.Keccak256Hash([]byte("ServiceRegistry")))[0].(common.Address)
			registryABI, err := abi.JSON(strings.NewReader(`[{"type":"function","name":"getServiceURI","inputs":[{"type":"address"}],"outputs":[{"type":"string"}]}]`))
			require.NoError(t, err)
			c.contracts["serviceRegistry"] = registryABI
			require.Equal(t, serviceURI, c.call(t, "serviceRegistry", registry, "getServiceURI", a.address)[0])
			t.Logf("%s CLI verified account creation, faucet, transfer, funding, unlock/cancel, bond, commissions and service URI", backend)
		})
	}
}
