#!/usr/bin/env bash
# 一键运行 upgrade-demo：tenuto-server（不执行 Workflow Task）、app-a、app-b 各自独立进程，
# driver 驱动全部场景并在需要时启动、停止各个发布版本的 app-a / app-b 进程。
#
#   ./docs/examples/upgrade-demo/run-demo.sh
#   UD_BACKEND=mysql UD_MYSQL_CNF=~/.my.cnf ./docs/examples/upgrade-demo/run-demo.sh
#   UD_BACKEND=mysql UD_MYSQL_HOST=127.0.0.1 UD_MYSQL_PASSWORD=... ./docs/examples/upgrade-demo/run-demo.sh
#
# 环境变量：
#   UD_BACKEND            memory（默认）：tenuto-server 的内存后端，不需要数据库
#                         mysql：tenuto-server，MySQL + TaskQ，内嵌 matching
#   UD_MYSQL_CNF          mysql 客户端配置（[client] 段的 host / port / user / password）
#   UD_MYSQL_HOST / UD_MYSQL_PORT / UD_MYSQL_USER / UD_MYSQL_PASSWORD
#                         直接给出连接信息；缺省时从 $HOME/.my.cnf 读取
#   UD_DB_PREFIX          库名前缀，默认 upgrade_demo（每次运行重建 <前缀>_workflow 与 <前缀>_taskq）
#   UD_LISTEN_HOST        监听与访问地址，默认 localhost
#   UD_API_PORT           tenuto-server 端口，默认 7400
#   UD_STEP=1             driver 在每个检查点暂停，按回车继续
#   UD_CONSOLE=1          同时启动 console（端口 UD_CONSOLE_PORT，默认 7421）
#   UD_SKIP_BUILD=1       复用上次构建的二进制
#
# 流程：建库（mysql）→ 生成与构建 → Server → driver（启动 app-a / app-b 并驱动场景）→ 清理。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
DEMO="docs/examples/upgrade-demo"
WORK="$ROOT/.run/upgrade-demo"
BIN="$WORK/bin"
LOGS="$WORK/logs"
NAMESPACE="upgrade-demo"

BACKEND="${UD_BACKEND:-memory}"
LISTEN_HOST="${UD_LISTEN_HOST:-localhost}"
API_PORT="${UD_API_PORT:-7400}"
CONSOLE_PORT="${UD_CONSOLE_PORT:-7421}"
API="http://${LISTEN_HOST}:${API_PORT}"
DB_PREFIX="${UD_DB_PREFIX:-upgrade_demo}"

log() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }

case "$BACKEND" in
	memory | mysql) ;;
	*) echo "UD_BACKEND 取 memory 或 mysql" >&2; exit 2 ;;
esac

rm -rf "$LOGS"
mkdir -p "$BIN" "$LOGS"
cnf_file=""
server_pid="" console_pid=""

cleanup() {
	local status=$?
	trap - EXIT INT TERM
	[[ -n "$cnf_file" ]] && rm -f "$cnf_file"
	# driver 正常退出时已经停掉它启动的应用进程；这里兜底清理残留。
	pkill -TERM -f "$BIN/app-" 2>/dev/null || true
	for pid in "$console_pid" "$server_pid"; do
		if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; fi
	done
	if [[ $status -ne 0 ]]; then
		echo "---- server (tail) ----" >&2
		tail -n 30 "$LOGS/server.log" >&2 2>/dev/null || true
	fi
	exit $status
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---------- 1. 存储 ----------
if [[ "$BACKEND" == "mysql" ]]; then
	defaults_file="${UD_MYSQL_CNF:-$HOME/.my.cnf}"
	# 读取 mysql 客户端配置 [client] 段里的单个键。
	FromCnf() {
		[[ -f "$defaults_file" ]] || return 0
		awk -v key="$1" '
			/^[[:space:]]*\[/ { section=$0 }
			section ~ /\[(client|mysql)\][[:space:]]*$/ {
				pos=index($0, "="); if (!pos) next
				name=substr($0, 1, pos-1); gsub(/^[[:space:]]+|[[:space:]]+$/, "", name)
				if (name != key) next
				value=substr($0, pos+1); gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
				print value; exit
			}' "$defaults_file"
	}
	db_host="${UD_MYSQL_HOST:-$(FromCnf host)}"
	db_port="${UD_MYSQL_PORT:-$(FromCnf port)}"
	db_user="${UD_MYSQL_USER:-$(FromCnf user)}"
	db_secret="${UD_MYSQL_PASSWORD:-$(FromCnf password)}"
	db_port="${db_port:-3306}"
	db_user="${db_user:-root}"
	if [[ -z "$db_host" ]]; then
		echo "需要 UD_MYSQL_HOST 或 mysql 配置里的 host" >&2
		exit 2
	fi
	if [[ -z "$db_secret" ]]; then
		echo "需要 UD_MYSQL_PASSWORD 或 mysql 配置里的 password" >&2
		exit 2
	fi
	# 凭据只经权限 0600 的临时客户端配置与进程环境传递，退出时删除临时配置。
	cnf_file="$WORK/mysql.cnf"
	(umask 077 && printf '[client]\nprotocol=TCP\nhost=%s\nport=%s\nuser=%s\n' "$db_host" "$db_port" "$db_user" >"$cnf_file")
	printf 'password=%s\n' "$db_secret" >>"$cnf_file"

	log "重建 MySQL 库 ${DB_PREFIX}_workflow / ${DB_PREFIX}_taskq（${db_host}:${db_port}）"
	mysql --defaults-extra-file="$cnf_file" -e "DROP DATABASE IF EXISTS \`${DB_PREFIX}_workflow\`; DROP DATABASE IF EXISTS \`${DB_PREFIX}_taskq\`;
		CREATE DATABASE \`${DB_PREFIX}_workflow\` DEFAULT CHARSET utf8mb4;
		CREATE DATABASE \`${DB_PREFIX}_taskq\` DEFAULT CHARSET utf8mb4;"

	export WORKFLOW_BACKEND=mysql-taskq
	export WORKFLOW_DB_DSN="${db_user}:${db_secret}@tcp(${db_host}:${db_port})/${DB_PREFIX}_workflow?charset=utf8mb4&parseTime=true"
	export TASKQ_DB_DSN="${db_user}:${db_secret}@tcp(${db_host}:${db_port})/${DB_PREFIX}_taskq?charset=utf8mb4&parseTime=true"
	export WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64="$(printf %s '0123456789abcdef0123456789abcdef' | base64)"
	export TASKQ_MATCHING_ADDR="${LISTEN_HOST}:0"
	export WORKFLOW_TASK_TOKEN_SECRET="0123456789abcdef0123456789abcdef"
else
	export WORKFLOW_BACKEND=memory
	export WORKFLOW_TASK_TOKEN_SECRET="0123456789abcdef0123456789abcdef"
fi

# ---------- 2. 生成与构建 ----------
cd "$ROOT"
if [[ "${UD_SKIP_BUILD:-0}" != "1" ]]; then
	log "生成 Workflow 产物"
	GOWORK=off go run ./compiler/cmd/tenuto-workflow-gen generate \
		"./$DEMO/appa/v1" "./$DEMO/appa/v2" "./$DEMO/appa/v3" "./$DEMO/appb/v1" "./$DEMO/appb/v2" >"$LOGS/generate.log" 2>&1
	log "构建二进制"
	GOWORK=off go build -o "$BIN/tenuto-server" ./server/cmd/tenuto-server
	GOWORK=off go build -o "$BIN/app-a" "./$DEMO/cmd/app-a"
	GOWORK=off go build -o "$BIN/app-b" "./$DEMO/cmd/app-b"
	GOWORK=off go build -o "$BIN/driver" "./$DEMO/cmd/driver"
fi
for executable in tenuto-server app-a app-b driver; do
	[[ -x "$BIN/$executable" ]] || { printf "缺少二进制：%s\n" "$executable" >&2; exit 1; }
done

if [[ "$BACKEND" == "mysql" ]]; then
	# Server 启动时不建表：先用 tenuto-server migrate 在两个库上执行 schema 迁移。
	log "执行 schema 迁移"
	"$BIN/tenuto-server" migrate >"$LOGS/migrate.log" 2>&1 || { echo "schema 迁移失败，见 $LOGS/migrate.log" >&2; exit 1; }
fi

# ---------- 3. Server ----------
log "启动 tenuto-server（${BACKEND} 后端，不执行 Workflow Task）：$API"
# WORKFLOW_WORKER_ENABLED=false：Server 不执行 Workflow Task，全部交给 app-a / app-b 的 Worker。
WORKFLOW_WORKER_ENABLED=false WORKFLOW_HTTP_ADDR="${LISTEN_HOST}:${API_PORT}" WORKFLOW_NODE_ID="upgrade-demo-1" \
	"$BIN/tenuto-server" >"$LOGS/server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 60); do
	kill -0 "$server_pid" 2>/dev/null || break
	curl --max-time 1 -fsS "$API/health/ready" >/dev/null 2>&1 && break
	sleep 0.5
done
curl --max-time 1 -fsS "$API/health/ready" >/dev/null || { echo "tenuto-server 未就绪，见 $LOGS/server.log" >&2; exit 1; }
log "tenuto-server ready"

# ---------- 4. console（可选） ----------
if [[ "${UD_CONSOLE:-0}" == "1" ]]; then
	[[ -f "$ROOT/console/node_modules/next/dist/bin/next" ]] || { echo "缺少 Console 依赖，请在 console 目录执行 pnpm install" >&2; exit 1; }
	log "启动 console：http://${LISTEN_HOST}:${CONSOLE_PORT}/${NAMESPACE}/runs"
	if [[ ! -f "$ROOT/console/public/replay.wasm" ]]; then
		(cd "$ROOT/console/wasm" && GOOS=js GOARCH=wasm GOWORK=off go build -o ../public/replay.wasm .) >>"$LOGS/console.log" 2>&1
	fi
	(
		cd "$ROOT/console"
		NEXT_PUBLIC_API_PORT="$API_PORT" WATCHPACK_POLLING=true exec node node_modules/next/dist/bin/next dev -H "$LISTEN_HOST" -p "$CONSOLE_PORT"
	) >>"$LOGS/console.log" 2>&1 &
	console_pid=$!
fi

# ---------- 5. driver ----------
driver_args=(-url "$API" -namespace "$NAMESPACE" -bin "$BIN" -logs "$LOGS")
if [[ "${UD_STEP:-0}" == "1" ]]; then
	driver_args+=(-step)
fi
log "driver 开始；应用进程日志：$LOGS/app-*.log"
rc=0
"$BIN/driver" "${driver_args[@]}" 2>&1 | tee "$LOGS/driver.log" || rc=$?

echo
echo "=================================================================="
echo " Tenuto API : $API"
echo " 场景输出   : $LOGS/driver.log"
echo " 进程日志   : $LOGS/"
echo "=================================================================="
if [[ "$rc" -ne 0 ]]; then
	echo "RESULT FAIL: driver exit ${rc}"
	exit 1
fi
