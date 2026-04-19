APP_NAME ?= traffic-go
GO ?= go
NPM ?= npm
WEB_DIR ?= web
BUILD_DIR ?= bin
CONFIG ?= deploy/config.example.yaml
BINARY ?= $(BUILD_DIR)/$(APP_NAME)
RELEASE_ROOT ?= release
RELEASE_DIR ?= $(RELEASE_ROOT)/linux-amd64
ARCHIVE ?= $(RELEASE_ROOT)/$(APP_NAME)-linux-amd64.tar.gz

.PHONY: help test test-backend test-frontend build build-backend build-frontend sync-frontend release-linux run dev dev-web clean clean-build clean-release clean-frontend fmt vet tidy

help:
	@printf '%s\n' \
		'Available targets:' \
		'  make test-backend   Run Go tests' \
		'  make test-frontend  Run frontend tests in web/' \
		'  make build-frontend Build frontend assets' \
		'  make build          Build the embedded app for the host platform' \
		'  make release-linux  Run tests and package a linux/amd64 release' \
		'  make run            Run the backend with the example config' \
		'  make dev            Alias for make run' \
		'  make dev-web        Run the frontend dev server' \
		'  make clean          Remove build artifacts'

test: test-backend test-frontend

test-backend:
	$(GO) test ./...

test-frontend:
	$(NPM) --prefix $(WEB_DIR) run test

build: sync-frontend build-backend

build-backend:
	$(GO) build -o $(BINARY) ./cmd/$(APP_NAME)

build-frontend:
	$(NPM) --prefix $(WEB_DIR) run build

sync-frontend: build-frontend
	mkdir -p internal/embed/dist
	rm -rf internal/embed/dist/*
	cp -R $(WEB_DIR)/dist/. internal/embed/dist/

release-linux: test clean-release sync-frontend
	mkdir -p $(RELEASE_DIR)
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build -o $(RELEASE_DIR)/$(APP_NAME) ./cmd/$(APP_NAME)
	cp deploy/config.example.yaml $(RELEASE_DIR)/config.yaml
	cp deploy/config.example.yaml $(RELEASE_DIR)/config.example.yaml
	cp deploy/install-centos7.sh $(RELEASE_DIR)/install-centos7.sh
	cp deploy/traffic-go.service $(RELEASE_DIR)/traffic-go.service
	chmod +x $(RELEASE_DIR)/install-centos7.sh
	tar -C $(RELEASE_DIR) -czf $(ARCHIVE) .

run:
	$(GO) run ./cmd/$(APP_NAME) -config $(CONFIG)

dev: run

dev-web:
	$(NPM) --prefix $(WEB_DIR) run dev -- --host 127.0.0.1

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

clean-build:
	rm -rf $(BUILD_DIR)

clean-release:
	rm -rf $(RELEASE_ROOT)

clean-frontend:
	rm -rf $(WEB_DIR)/dist

clean: clean-build clean-release clean-frontend
