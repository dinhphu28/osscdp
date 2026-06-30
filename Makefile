.PHONY: up down migrate-up migrate-down run-api run-worker test tidy build

# Load .env into the environment for app/migrate targets.
ifneq (,$(wildcard .env))
include .env
export
endif

COMPOSE := docker compose -f deploy/docker-compose.yml --env-file .env

up: ## Start Postgres + Redpanda
	$(COMPOSE) up -d

down: ## Stop the local stack
	$(COMPOSE) down

migrate-up: ## Apply migrations
	go run ./cmd/migrate up

migrate-down: ## Roll back the latest migration
	go run ./cmd/migrate down

run-api: ## Run cdp-api (auto-migrates on boot)
	go run ./cmd/cdp-api

run-worker: ## Run cdp-worker stub
	go run ./cmd/cdp-worker

build: ## Build all binaries
	go build ./...

test: ## Run all tests
	go test ./...

tidy: ## Tidy modules
	go mod tidy
