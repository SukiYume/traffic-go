APP_NAME ?= traffic-go
GO ?= go
NPM ?= npm
WEB_DIR ?= web
BUILD_DIR ?= bin
CONFIG ?= deploy/config.example.yaml
BINARY ?= $(BUILD_DIR)/$(APP_NAME)
RELEASE_ROOT ?= release
VERSION ?= dev
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= unknown
GO_BUILD_FLAGS ?= -trimpath
GO_LDFLAGS ?= -s -w -X traffic-go/internal/buildinfo.Version=$(VERSION) -X traffic-go/internal/buildinfo.Commit=$(COMMIT) -X traffic-go/internal/buildinfo.BuildDate=$(BUILD_DATE)

.PHONY: help test verify test-backend test-frontend build build-backend build-frontend sync-frontend release-linux run dev dev-web clean clean-build clean-release clean-frontend fmt vet tidy audit-go audit-web

help:
	@printf '%s\n' \
		'Available targets:' \
		'  make test-backend   Run Go tests' \
		'  make test-frontend  Run frontend tests in web/' \
		'  make build-frontend Build frontend assets' \
		'  make build          Build the embedded app for the host platform' \
		'  make release-linux  Run tests and package a linux/amd64 release' \
		'  make verify         Run backend/frontend tests and Go vet' \
		'  make audit-go       Run govulncheck (must be installed)' \
		'  make audit-web      Audit production frontend dependencies' \
		'  make run            Run the backend with the example config' \
		'  make dev            Alias for make run' \
		'  make dev-web        Run the frontend dev server' \
		'  make clean          Remove build artifacts'

test: test-backend test-frontend

verify: test vet

test-backend:
	$(GO) test ./... -count=1

test-frontend:
	$(NPM) --prefix $(WEB_DIR) run test

build: sync-frontend build-backend

build-backend:
	mkdir -p $(BUILD_DIR)
	$(GO) build $(GO_BUILD_FLAGS) -ldflags "$(GO_LDFLAGS)" -o $(BINARY) ./cmd/$(APP_NAME)

build-frontend:
	$(NPM) --prefix $(WEB_DIR) run build

sync-frontend: build-frontend
	test -s $(WEB_DIR)/dist/index.html
	mkdir -p internal/embed/dist
	test ! -L internal/embed/dist
	find internal/embed/dist -mindepth 1 -maxdepth 1 ! -name placeholder.txt -exec rm -rf -- {} +
	cp -R $(WEB_DIR)/dist/. internal/embed/dist/
	test -f internal/embed/dist/placeholder.txt

release-linux:
	APP_NAME=$(APP_NAME) GO=$(GO) NPM=$(NPM) WEB_DIR=$(WEB_DIR) RELEASE_ROOT=$(RELEASE_ROOT) $(if $(filter dev,$(VERSION)),,VERSION=$(VERSION)) bash deploy/package-release.sh

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

audit-go:
	govulncheck ./...

audit-web:
	$(NPM) --prefix $(WEB_DIR) audit --omit=dev

clean-build:
	bash deploy/clean-generated.sh "$(BUILD_DIR)"

clean-release:
	bash deploy/clean-generated.sh "$(RELEASE_ROOT)"

clean-frontend:
	bash deploy/clean-generated.sh "$(WEB_DIR)/dist"

clean: clean-build clean-release clean-frontend
