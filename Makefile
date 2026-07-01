.PHONY: up down migrate-up migrate-down run-api run-worker test tidy build \
	stack-up stack-down loadtest backup restore promtool

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

stack-up: ## Start the full stack (apps + Postgres + Redpanda + Prometheus + Grafana + Alertmanager)
	$(COMPOSE) --profile full up -d --build

stack-down: ## Stop the full stack
	$(COMPOSE) --profile full down

loadtest: ## Run the k6 ingress load test (API_KEY=... [API_URL=http://localhost:18080])
	docker run --rm -i --network host \
		-e API_URL=$(or $(API_URL),http://localhost:18080) -e API_KEY=$(API_KEY) \
		grafana/k6 run - < loadtest/track.js

backup: ## pg_dump the database to ./backups
	./scripts/backup.sh

restore: ## Restore a dump into a scratch DB: make restore FILE=backups/cdp-....dump
	./scripts/restore.sh $(FILE)

promtool: ## Validate Prometheus alert rules
	docker run --rm --entrypoint promtool -v $(PWD)/deploy/prometheus:/etc/prometheus \
		prom/prometheus:v2.54.1 check rules /etc/prometheus/alerts.yml
