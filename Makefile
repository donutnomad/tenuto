APP_PORT ?= 7400
UI_PORT ?= 7401
DOCS_PORT ?= 7402
TENUTO_SERVER_BINARY ?= bin/tenuto-server
TENUTO_DEMO_BINARY ?= bin/tenuto-demo
TENUTO_STARTUP_EXAMPLE_BINARY ?= bin/tenuto-startup-example
RUN_DIR ?= .run
TENUTO_SERVER_URL ?= http://127.0.0.1:$(APP_PORT)
VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GO_ENV ?= GOWORK=off
# 额外的 Console dev 环境变量，例如文件监听失效时传 CONSOLE_DEV_ENV=WATCHPACK_POLLING=true。
CONSOLE_DEV_ENV ?=
# 仅供本地 demo 使用的 task token 签名密钥。
DEV_TASK_TOKEN_SECRET ?= insecure-local-dev-task-token-secret

.PHONY: generate vet test check dev dev-console dev-docs build-docs check-docs preview-docs dev-backend dev-server dev-demo start-dev-backend migrate run-server build-server build-demo build-example stop wasm

# 统一代码生成：唯一的 //go:generate 入口在 server/cmd/tenuto-server/main.go，从根目录递归扫描。
generate:
	$(GO_ENV) go generate ./...

vet:
	$(GO_ENV) go vet ./...

# Go tests plus Console unit tests.
test:
	$(GO_ENV) go test ./...
	pnpm --dir console test

# 提交前检查：生成产物与工作区一致（无 diff），再 vet 和测试。
check: generate
	git diff --exit-code
	$(MAKE) vet
	$(MAKE) test

# Start Tenuto Console.
# 依赖 wasm：replay.wasm 是构建产物、不在版本库里（见 .gitignore），
# 新 clone 不先构建的话 replay 页面会 404。
dev-console: wasm
	@rm -rf console/.next
	cd console && $(CONSOLE_DEV_ENV) NEXT_PUBLIC_API_PORT=$(APP_PORT) pnpm dev -H 0.0.0.0 -p $(UI_PORT)

# Start the Chinese Tenuto documentation site.
dev-docs:
	pnpm --dir docs dev --port $(DOCS_PORT)

build-docs:
	pnpm --dir docs build

check-docs:
	pnpm --dir docs check
	$(GO_ENV) go test ./docs/examples/...
	$(MAKE) build-docs

preview-docs:
	pnpm --dir docs preview --port $(DOCS_PORT)

# Start Tenuto Server and the demo worker/seeder as background processes.
dev-backend: stop start-dev-backend

# Run Tenuto Server with ephemeral adapters.
dev-server:
	$(GO_ENV) WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:$(APP_PORT) go run ./server/cmd/tenuto-server/

# Run the demo worker/seeder against an existing Tenuto Server.
dev-demo:
	$(GO_ENV) WORKFLOW_SERVER_URL=$(TENUTO_SERVER_URL) go run ./server/demo/app/

# Apply pending schema migrations to WORKFLOW_DB_DSN / TASKQ_DB_DSN (run before run-server).
migrate:
	$(GO_ENV) go run ./server/cmd/tenuto-server/ migrate

# Run Tenuto Server using production environment configuration.
run-server:
	$(GO_ENV) go run ./server/cmd/tenuto-server/

# Build the standalone Tenuto Server binary.
build-server:
	mkdir -p $(dir $(TENUTO_SERVER_BINARY))
	$(GO_ENV) go build -trimpath \
		-ldflags "-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)" \
		-o $(TENUTO_SERVER_BINARY) ./server/cmd/tenuto-server/

build-demo:
	mkdir -p $(dir $(TENUTO_DEMO_BINARY))
	$(GO_ENV) go build -trimpath -o $(TENUTO_DEMO_BINARY) ./server/demo/app/

# 作者主路径：生成 Workflow 定义产物 → go build。定义产物被 embed 进二进制，
# 启动时自动登记；普通 go build 不会生成产物，所以生成放在构建入口里。
build-example:
	$(GO_ENV) go run ./compiler/cmd/tenuto-workflow-gen generate ./docs/examples/startup-workflow
	mkdir -p $(dir $(TENUTO_STARTUP_EXAMPLE_BINARY))
	$(GO_ENV) go build -trimpath -o $(TENUTO_STARTUP_EXAMPLE_BINARY) ./docs/examples/startup-workflow/worker/

start-dev-backend: build-server build-demo
	@mkdir -p $(RUN_DIR)
	@test -z "$$(lsof -ti:$(APP_PORT) 2>/dev/null)" || { echo "port $(APP_PORT) is already in use; run 'make stop' or choose APP_PORT"; exit 1; }
	@WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:$(APP_PORT) WORKFLOW_TASK_TOKEN_SECRET=$(DEV_TASK_TOKEN_SECRET) nohup $(TENUTO_SERVER_BINARY) >$(RUN_DIR)/tenuto-server.log 2>&1 & echo $$! >$(RUN_DIR)/tenuto-server.pid
	@for attempt in $$(seq 1 100); do curl -fsS $(TENUTO_SERVER_URL)/health/ready >/dev/null 2>&1 && exit 0; sleep 0.1; done; kill $$(cat $(RUN_DIR)/tenuto-server.pid) 2>/dev/null || true; cat $(RUN_DIR)/tenuto-server.log; exit 1
	@WORKFLOW_SERVER_URL=$(TENUTO_SERVER_URL) nohup $(TENUTO_DEMO_BINARY) >$(RUN_DIR)/tenuto-demo.log 2>&1 & echo $$! >$(RUN_DIR)/tenuto-demo.pid
	@echo "Tenuto Server: $(TENUTO_SERVER_URL) (log: $(RUN_DIR)/tenuto-server.log)"
	@echo "demo worker: background (log: $(RUN_DIR)/tenuto-demo.log)"

# Start Tenuto Server, demo worker/seeder, and Console.
# 依赖 wasm：同 dev-console，replay.wasm 不在版本库里。
dev: wasm stop start-dev-backend
	@rm -rf console/.next
	@echo "Starting Tenuto Console on :$(UI_PORT)..."
	cd console && $(CONSOLE_DEV_ENV) NEXT_PUBLIC_API_PORT=$(APP_PORT) pnpm dev -H 0.0.0.0 -p $(UI_PORT)

# Build the Console WASM replay module.
wasm:
	cd console/wasm && $(GO_ENV) GOOS=js GOARCH=wasm go build -o ../public/replay.wasm .
	cp -f "$$($(GO_ENV) go env GOROOT)/lib/wasm/wasm_exec.js" console/public/wasm_exec.js

# Stop the background server and demo worker started by dev-backend (tracked by PID files).
stop:
	@for name in tenuto-demo tenuto-server; do \
		pid_file=$(RUN_DIR)/$$name.pid; \
		if test -f $$pid_file; then \
			pid=$$(cat $$pid_file); \
			kill $$pid 2>/dev/null || true; \
			for attempt in $$(seq 1 20); do kill -0 $$pid 2>/dev/null || break; sleep 0.1; done; \
			kill -9 $$pid 2>/dev/null || true; \
			rm -f $$pid_file; \
		fi; \
	done
	@-rm -f console/.next/dev/dev-server.lock 2>/dev/null || true
