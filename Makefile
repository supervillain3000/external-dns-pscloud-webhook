SHELL := /bin/bash

PROJECT_NAME ?= external-dns-pscloud-webhook
BINARY ?= $(PROJECT_NAME)
BUILD_DIR ?= build/bin
GO ?= go
GOFLAGS ?=
IMAGE ?= ghcr.io/$(USER)/$(PROJECT_NAME)
TAG ?= dev

.PHONY: all build test lint run clean docker-build

all: build

build:
	mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -o $(BUILD_DIR)/$(BINARY) ./cmd/webhook

test:
	$(GO) test ./...

lint:
	golangci-lint run

run:
	$(GO) run ./cmd/webhook

clean:
	rm -rf build

docker-build:
	docker build -t $(IMAGE):$(TAG) .
