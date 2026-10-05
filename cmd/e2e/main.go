// e2e loads the same visible configuration before Go resolves the runner module.
// Direct go test uses the pinned runner module; Make additionally lets
// E2E_GOLANG_RUNNER_DIR select a source checkout without editing go.mod.
package main

import (
	"cmp"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if exit, ok := err.(*exec.ExitError); ok {
			os.Exit(exit.ExitCode())
		}
		os.Exit(1)
	}
}

func run() error {
	if err := godotenv.Load(cmp.Or(os.Getenv("E2E_ENV_FILE"), "test.env")); err != nil {
		return err
	}
	args := os.Args[1:]
	if len(args) == 0 {
		args = []string{"test", "-count=1", "./..."}
	}
	if args[0] == "compose" || args[0] == "compose-gcp" {
		if len(args) == 1 {
			args = append(args, strings.Fields(os.Getenv("E2E_COMPOSE_TEST_ARGS"))...)
		}
		script := "docker/run.sh"
		if args[0] == "compose-gcp" {
			script = "docker/validate-gcp.sh"
		}
		command := exec.Command("bash", append([]string{script}, args[1:]...)...)
		command.Stdout, command.Stderr, command.Stdin = os.Stdout, os.Stderr, os.Stdin
		return command.Run()
	}
	if args[0] == "test" {
		hasTimeout := slices.ContainsFunc(args[1:], func(arg string) bool {
			return arg == "-timeout" || strings.HasPrefix(arg, "-timeout=")
		})
		if !hasTimeout {
			timeout := cmp.Or(os.Getenv("E2E_TEST_TIMEOUT"), "90m")
			args = append([]string{"test", "-timeout", timeout}, args[1:]...)
		}
	}
	if dir := os.Getenv("E2E_GOLANG_RUNNER_DIR"); dir != "" {
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return err
		}
		temp, err := os.MkdirTemp("", "e2e-scope-module-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(temp)
		modfile := filepath.Join(temp, "go.mod")
		for _, name := range []string{"go.mod", "go.sum"} {
			data, err := os.ReadFile(name)
			if err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(temp, name), data, 0600); err != nil {
				return err
			}
		}
		edit := exec.Command("go", "mod", "edit", "-modfile", modfile, "-replace", "github.com/livepeer/golang-runner="+absolute)
		if out, err := edit.CombinedOutput(); err != nil {
			return fmt.Errorf("configure Go runner: %w: %s", err, out)
		}
		args = append([]string{args[0], "-modfile", modfile}, args[1:]...)
	}
	command := exec.Command("go", args...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.Stdin = os.Stdin
	return command.Run()
}
