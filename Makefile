.PHONY: test test-short vet fmt ci

test:
	go test -race ./...

test-short:
	go test ./...

vet:
	go vet ./...

fmt:
	go fmt ./...

ci: test
