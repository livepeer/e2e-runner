include test.env
export

.PHONY: test
test:
	go test ./...
