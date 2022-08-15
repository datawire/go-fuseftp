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
SHELL = bash

test:
	go test -v -c ./pkg/fs
	go test -v -count=1 -vet=off ./pkg/fs
.PHONY: test

cover:
	go test -v -covermode=count -coverprofile=coverage.out ./pkg/fs
.PHONY: cover

build:
	go build ./pkg/fs
.PHONY: build

%.out.html: %.out
	go tool cover -html=$< -o=$@

lint: tools/golangci-lint
	tools/golangci-lint run ./...
.PHONY: lint

tools/%: tools/src/%.d/go.mod tools/src/%.d/pin.go
	cd $(<D) && go build -o ../../$(@F) $$(sed -En 's,^import _ "(.*)"$$,\1,p' pin.go)
