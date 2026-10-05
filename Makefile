# TestMain loads test.env with process-environment precedence.
.PHONY: test test-offchain test-onchain test-accounting test-compose test-compose-gcp
export E2E_COMPOSE_TEST_ARGS
test:
	go run ./cmd/e2e test -count=1 ./...
test-offchain:
	go run ./cmd/e2e test -count=1 -run 'Test(DynamicRunner|StaticRunner|LiveRunnerServer|RunnerSession|PythonRunnerMulti)' ./...
test-onchain:
	go run ./cmd/e2e test -count=1 -run '^TestOnchain' ./...
test-accounting:
	go run ./cmd/e2e test -count=1 -run '^TestClearinghouse' ./...
test-compose:
	go run ./cmd/e2e compose
test-compose-gcp:
	go run ./cmd/e2e compose-gcp
