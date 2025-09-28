PROJECT_NAME = go-lib
GO_VERSION = go1.25.1
export CGO_ENABLED = 0
GO_FILES = $$( find . -type f -name '*.go' )

include Makefile.forge

default:
	$(MAKE) tidy
	$(MAKE) vet
.PHONY: default

tidy: | .d.goimports .d.go
	$(GO) mod tidy
.PHONY: tidy

vet: .d.go
	$(GO) vet -unreachable=false ./...
.PHONY: vet
