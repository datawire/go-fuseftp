#
# Intro

help:
	@echo 'Usage:'
	@echo '  make help'
	@echo '  make cover'
	@echo '  make test'
	@echo '  make lint'
.PHONY: help

.SECONDARY:
.PHONY: FORCE

SHELL := bash

GOARCH ?= $(shell go env GOARCH)
GOOS ?= $(shell go env GOOS)

ifeq ($(GOOS),windows)
  EXE=.exe
else
  EXE=
endif

test:
	go test -v -c ./pkg/fs
	go test -v -count=1 -vet=off ./pkg/fs
.PHONY: test

cover:
	go test -v -covermode=count -coverprofile=coverage.out ./pkg/fs
.PHONY: cover

TOOLS=tools/bin

rpc/fuseftp.pb.go rpc/fuseftp.grpc_pb.go: rpc/fuseftp.proto $(TOOLS)/protoc$(EXE) $(TOOLS)/protoc-gen-go$(EXE) $(TOOLS)/protoc-gen-go-grpc$(EXE)
	$(TOOLS)/protoc \
	  --plugin=protoc-gen-go=$(TOOLS)/protoc-gen-go$(EXE) \
	  --plugin=protoc-gen-go-grpc=$(TOOLS)/protoc-gen-go-grpc$(EXE) \
	  --go_out=./rpc \
	  --go_opt=module=github.com/datawire/go-fuseftp/rpc \
	  --go-grpc_out=./rpc \
	  --go-grpc_opt=module=github.com/datawire/go-fuseftp/rpc \
	  --proto_path=. \
	  $<

BUILD_OUTPUT = build-output
BIN_OUTPUT = $(BUILD_OUTPUT)/bin

.PHONY: fuseftp
fuseftp: $(BIN_OUTPUT)/fuseftp-$(GOOS)-$(GOARCH)$(EXE)

$(BIN_OUTPUT)/fuseftp-$(GOOS)-$(GOARCH)$(EXE): rpc/fuseftp.pb.go rpc/fuseftp.grpc_pb.go $(wildcard pkg/fs/*.go) $(wildcard pkg/main/*.go)
	mkdir -p $(BIN_OUTPUT)
	go build -o $@ ./pkg/main/...

%.out.html: %.out
	go tool cover -html=$< -o=$@

lint: $(TOOLS)/golangci-lint$(EXE)
	$(TOOLS)/golangci-lint run ./...
.PHONY: lint

GOHOSTARCH ?= $(shell go env GOHOSTARCH)
GOHOSTOS ?= $(shell go env GOHOSTOS)

# Install protoc and friends under tools/bin.
PROTOC_VERSION=21.5
ifeq ($(GOHOSTARCH),arm64)
  PROTOC_ARCH=aarch_64
else ifeq ($(GOHOSTARCH),amd64)
  PROTOC_ARCH=x86_64
else
  PROTOC_ARCH=$(GOHOSTARCH)
endif
ifeq ($(GOHOSTOS),windows)
  PROTOC_OS_ARCH=win64
else
  PROTOC_OS_ARCH=$(patsubst darwin,osx,$(GOHOSTOS))-$(PROTOC_ARCH)
endif

PROTOC_ZIP=protoc-$(PROTOC_VERSION)-$(PROTOC_OS_ARCH).zip
tools/$(PROTOC_ZIP):
	mkdir -p $(@D)
	curl -sfL https://github.com/protocolbuffers/protobuf/releases/download/v$(PROTOC_VERSION)/$(PROTOC_ZIP) -o $@

%/bin/protoc$(EXE) %/include %/readme.txt: %/$(PROTOC_ZIP)
	cd $* && unzip -q -o -DD $(<F)

$(TOOLS)/%: tools/src/%/go.mod tools/src/%/pin.go
	cd $(<D) && GOOS= GOARCH= go build -o $(abspath $@) $$(sed -En 's,^import "(.*)".*,\1,p' pin.go)
