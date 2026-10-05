package e2escope

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var provenanceMu sync.Mutex
var provenanceID = time.Now().UTC().Format("20060102T150405.000000000")
var provenance = struct {
	Time     string                       `json:"time"`
	Sources  map[string]map[string]string `json:"sources"`
	Binaries map[string]map[string]string `json:"binaries"`
}{Time: time.Now().UTC().Format(time.RFC3339), Sources: map[string]map[string]string{}, Binaries: map[string]map[string]string{}}

func recordBinary(t *testing.T, name, path string) {
	t.Helper()
	provenanceMu.Lock()
	defer provenanceMu.Unlock()
	if _, exists := provenance.Binaries[path]; exists {
		return
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err, "binary %s", path)
	sum := sha256.Sum256(data)
	info, err := exec.Command(envDefault("E2E_GO_BINARY", "go"), "version", "-m", path).CombinedOutput()
	if err != nil {
		t.Logf("binary provenance: %v", err)
	}
	provenance.Binaries[path] = map[string]string{"backend": name, "sha256": hex.EncodeToString(sum[:]), "build_info": string(info)}
	for name, dir := range map[string]string{
		"harness": envDefault("E2E_HARNESS_DIR", "."), "go-livepeer": os.Getenv(envGoLivepeerDir),
		"node": os.Getenv("E2E_NODE_DIR"), "clearinghouse": os.Getenv("E2E_CLEARINGHOUSE_DIR"),
		"python-runner": os.Getenv(envPythonRunnerDir), "golang-runner": os.Getenv("E2E_GOLANG_RUNNER_DIR"),
	} {
		if dir == "" || provenance.Sources[name] != nil {
			continue
		}
		dir = absPath(t, dir)
		git := func(args ...string) ([]byte, error) {
			return exec.Command("git", append([]string{"-C", dir}, args...)...).Output()
		}
		entry := map[string]string{"directory": dir}
		if composeMode() {
			data, err := os.ReadFile(filepath.Join(dir, ".e2e-source.json"))
			require.NoError(t, err)
			var source map[string]string
			require.NoError(t, json.Unmarshal(data, &source))
			entry["revision"], entry["local_changes"], entry["original_directory"] = source["revision"], source["status"], source["source"]
			entry["snapshot_sha256"] = source["snapshot_sha256"]
			provenance.Sources[name] = entry
			continue
		}
		for key, args := range map[string][]string{"revision": {"rev-parse", "HEAD"}, "local_changes": {"status", "--short"}} {
			output, err := git(args...)
			if err == nil {
				entry[key] = string(output)
			} else {
				entry[key] = err.Error()
			}
		}
		if diff, err := git("diff", "--binary", "HEAD"); err == nil {
			digest := sha256.Sum256(diff)
			entry["local_diff_sha256"] = hex.EncodeToString(digest[:])
			// Include non-ignored untracked source content in the working-state
			// fingerprint without retaining patch or credential contents.
			fingerprint := sha256.New()
			_, _ = fingerprint.Write(diff)
			if paths, err := git("ls-files", "--others", "--exclude-standard", "-z"); err == nil {
				for _, path := range bytes.Split(paths, []byte{0}) {
					if len(path) == 0 {
						continue
					}
					if data, err := os.ReadFile(filepath.Join(dir, string(path))); err == nil {
						sum := sha256.Sum256(data)
						_, _ = fingerprint.Write(path)
						_, _ = fingerprint.Write([]byte{0})
						_, _ = fingerprint.Write(sum[:])
					}
				}
			}
			entry["working_state_sha256"] = hex.EncodeToString(fingerprint.Sum(nil))
		}
		provenance.Sources[name] = entry
	}
	out, _ := json.MarshalIndent(provenance, "", "  ")
	dir := envDefault("E2E_ARTIFACT_DIR", "artifacts")
	require.NoError(t, os.MkdirAll(dir, 0755))
	for _, name := range []string{"provenance.json", "provenance-" + provenanceID + ".json"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), out, 0600), "write provenance")
	}
}
