SHELL := /bin/sh

.PHONY: fmt test vet test-integration verify

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

test:
	go test -race ./...

vet:
	go vet ./...

test-integration:
	go test -tags=integration -count=1 -timeout=5m ./integration/...

verify: test vet
