APP_NAME ?= traffic-go
GO ?= go
NPM ?= npm
WEB_DIR ?= web
BUILD_DIR ?= bin
CONFIG ?= deploy/config.example.yaml
BINARY ?= $(BUILD_DIR)/$(APP_NAME)

.PHONY: help test test-backend test-frontend build build-backend build-frontend sync-frontend run dev dev-web clean fmt vet tidy

help:
	@printf '%s\n' \
		'Available targets:' \
		'  make test-backend   Run Go tests' \
		'  make test-frontend  Run frontend tests in web/' \
		'  make build-frontend Build frontend assets' \
		'  make build          Build the Go binary' \
		'  make run            Run the backend with the example config' \
		'  make dev            Alias for make run' \
		'  make dev-web        Run the frontend dev server' \
		'  make clean          Remove build artifacts'

test: test-backend test-frontend

test-backend:
	$(GO) test ./...

test-frontend:
	$(NPM) --prefix $(WEB_DIR) run test

build: build-frontend build-backend

build-backend: sync-frontend
	$(GO) build -o $(BINARY) ./cmd/$(APP_NAME)

build-frontend:
	$(NPM) --prefix $(WEB_DIR) run build

sync-frontend: build-frontend
	mkdir -p internal/embed/dist
	rm -rf internal/embed/dist/*
	cp -R $(WEB_DIR)/dist/. internal/embed/dist/

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

clean:
	rm -rf $(BUILD_DIR)
