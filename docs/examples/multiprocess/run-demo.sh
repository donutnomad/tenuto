#!/usr/bin/env bash
# 一键运行 multiprocess 示例：tenuto-server（内嵌 workflow worker）、app-1、app-2 两个 Activity
# 进程、console 各自独立进程。
#
#   MP_MYSQL_CNF=~/.my.cnf ./docs/examples/multiprocess/run-demo.sh
#   MP_MYSQL_HOST=127.0.0.1 MP_MYSQL_PASSWORD=... ./docs/examples/multiprocess/run-demo.sh
#
# 环境变量：
#   MP_MYSQL_CNF          mysql 客户端配置（含 user= 与 password= 行）
#   MP_MYSQL_HOST / MP_MYSQL_PORT / MP_MYSQL_USER / MP_MYSQL_PASSWORD
#                         直接给出连接信息；缺省时从 $HOME/.my.cnf 读取
#   MP_DB_PREFIX          库名前缀，默认 mp_demo（每次运行重建 <前缀>_workflow 与 <前缀>_taskq）
#   MP_LISTEN_HOST        监听与访问地址，默认 127.0.0.1
#   MP_API_PORT           tenuto-server 端口，默认 7400
#   MP_CONSOLE_PORT       console 端口，默认 7421（NEXT_PUBLIC_API_PORT 指向 MP_API_PORT）
#   MP_CONSOLE=0          不启动 console
#   MP_HOLD_SECONDS       结果打印后保持 console 的秒数（0 = 等到 Ctrl-C）
#   MP_SKIP_BUILD=1       复用上次构建的二进制
#
# 流程：建库 → 生成/构建 → tenuto-server（进程 1）→ app-2（进程 3）→ console（进程 4）
# → driver（进程 5）驱动场景，app-1（进程 2）按 v1 → v2 → v3 逐次上线 → 打印结果 → 清理。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
WORK="$ROOT/.run/multiprocess"
BIN="$WORK/bin"
LOGS="$WORK/logs"
READY="$WORK/ready"
NAMESPACE="multiprocess-demo"

LISTEN_HOST="${MP_LISTEN_HOST:-localhost}"
API_PORT="${MP_API_PORT:-7400}"
CONSOLE_PORT="${MP_CONSOLE_PORT:-7421}"
CONSOLE="${MP_CONSOLE:-1}"
API="http://${LISTEN_HOST}:${API_PORT}"
DB_PREFIX="${MP_DB_PREFIX:-mp_demo}"

log() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
defaults_file="${MP_MYSQL_CNF:-$HOME/.my.cnf}"
cnf_file=""

# 读取 mysql 客户端配置（$HOME/.my.cnf 或 MP_MYSQL_CNF）[client] 段里的单个键。
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

# MP_* 显式给出时优先；否则取 defaults_file 里的 [client]。
db_host="${MP_MYSQL_HOST:-$(FromCnf host)}"
db_port="${MP_MYSQL_PORT:-$(FromCnf port)}"
db_user="${MP_MYSQL_USER:-$(FromCnf user)}"
db_secret="${MP_MYSQL_PASSWORD:-$(FromCnf password)}"
db_port="${db_port:-3306}"
db_user="${db_user:-root}"
if [[ -z "$db_host" ]]; then
	echo "需要 MP_MYSQL_HOST 或 mysql 配置里的 host" >&2
	exit 2
fi
if [[ -z "$db_secret" ]]; then
	echo "需要 MP_MYSQL_PASSWORD 或 mysql 配置里的 password" >&2
	exit 2
fi

rm -rf "$LOGS" "$READY"
mkdir -p "$BIN" "$LOGS" "$READY"
cnf_file="$WORK/mysql.cnf"
(umask 077 && printf '[client]\nprotocol=TCP\nhost=%s\nport=%s\nuser=%s\n' "$db_host" "$db_port" "$db_user" >"$cnf_file")
printf 'password=%s\n' "$db_secret" >>"$cnf_file"

# 状态文件尚未创建时返回空值，避免 set -e 中断监视循环。
ReadText() {
	[[ -f "$1" ]] && while IFS= read -r line || [[ -n "$line" ]]; do printf "%s\n" "$line"; done <"$1"
	return 0
}

Mysql() {
	mysql --defaults-extra-file="$cnf_file" "$@"
}
driver_pid="" watcher_pid=""
server_pid="" app2_pid="" console_pid=""

cleanup() {
	local status=$?
	trap - EXIT INT TERM
	rm -f "$cnf_file"
	for pid in "$driver_pid" "$(ReadText "$READY/app1.pid")" "$watcher_pid" "$app2_pid" "$console_pid" "$server_pid"; do
		if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; fi
	done
	if [[ $status -ne 0 ]]; then
		echo "---- tenuto-server (tail) ----" >&2; tail -n 30 "$LOGS/tenuto-server.log" >&2 2>/dev/null || true
		echo "---- app-1 (tail) ----" >&2; tail -n 20 "$LOGS/app1.log" >&2 2>/dev/null || true
	fi
	exit $status
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# ---------- 1. 数据库 ----------
log "重建 MySQL 库 ${DB_PREFIX}_workflow / ${DB_PREFIX}_taskq（${db_host}:${db_port}）"
Mysql -e "DROP DATABASE IF EXISTS \`${DB_PREFIX}_workflow\`; DROP DATABASE IF EXISTS \`${DB_PREFIX}_taskq\`;
	CREATE DATABASE \`${DB_PREFIX}_workflow\` DEFAULT CHARSET utf8mb4;
	CREATE DATABASE \`${DB_PREFIX}_taskq\` DEFAULT CHARSET utf8mb4;"

# 凭据只通过进程环境交给 server / worker，不写入仓库文件。
export WORKFLOW_DB_DSN="${db_user}:${db_secret}@tcp(${db_host}:${db_port})/${DB_PREFIX}_workflow?charset=utf8mb4&parseTime=true"
export TASKQ_DB_DSN="${db_user}:${db_secret}@tcp(${db_host}:${db_port})/${DB_PREFIX}_taskq?charset=utf8mb4&parseTime=true"
export WORKFLOW_TASK_TOKEN_SECRET="0123456789abcdef0123456789abcdef"
export WORKFLOW_HISTORY_PAGE_TOKEN_KEY_BASE64="$(printf %s '0123456789abcdef0123456789abcdef' | base64)"

# ---------- 2. 生成与构建 ----------
cd "$ROOT"
if [[ "${MP_SKIP_BUILD:-0}" != "1" ]]; then
	log "生成 Workflow 产物"
	GOWORK=off go generate ./...
	log "构建二进制"
	GOWORK=off go build -o "$BIN/tenuto-server" ./server/cmd/tenuto-server
	GOWORK=off go build -o "$BIN/app1" ./docs/examples/multiprocess/app1
	GOWORK=off go build -o "$BIN/app2" ./docs/examples/multiprocess/app2/cmd
	GOWORK=off go build -o "$BIN/driver" ./docs/examples/multiprocess/setup
fi
for executable in tenuto-server app1 app2 driver; do
	[[ -x "$BIN/$executable" ]] || { printf "缺少二进制：%s\n" "$executable" >&2; exit 1; }
done

# Server 启动时不建表：先用 tenuto-server migrate 在两个库上执行 schema 迁移。
log "执行 schema 迁移"
WORKFLOW_BACKEND=mysql-taskq "$BIN/tenuto-server" migrate >"$LOGS/migrate.log" 2>&1 || { echo "schema 迁移失败，见 $LOGS/migrate.log" >&2; exit 1; }

# ---------- 3. tenuto-server（进程 1） ----------
log "启动 tenuto-server（进程 1）：$API"
WORKFLOW_BACKEND=mysql-taskq \
	WORKFLOW_HTTP_ADDR="${LISTEN_HOST}:${API_PORT}" WORKFLOW_NODE_ID="tenuto-demo-1" \
	TASKQ_MATCHING_ADDR="${LISTEN_HOST}:0" \
	"$BIN/tenuto-server" >"$LOGS/tenuto-server.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 30); do
	kill -0 "$server_pid" 2>/dev/null || break
	curl --max-time 1 -fsS "$API/health/ready" >/dev/null 2>&1 && break
	sleep 0.5
done
curl --max-time 1 -fsS "$API/health/ready" >/dev/null || { echo "tenuto-server 未就绪，见 $LOGS/tenuto-server.log" >&2; exit 1; }
log "tenuto-server ready"

# ---------- 4. app-2（进程 3） ----------
log "启动 app-2（进程 3）：inventory + ledger 队列"
MP_READY_DIR="$READY" TENUTO_URL="$API" TENUTO_NAMESPACE="$NAMESPACE" "$BIN/app2" >"$LOGS/app2.log" 2>&1 &
app2_pid=$!
for _ in $(seq 1 30); do
	kill -0 "$app2_pid" 2>/dev/null || break
	grep -q "ready namespace" "$LOGS/app2.log" 2>/dev/null && break
	sleep 0.5
done
grep -q "ready namespace" "$LOGS/app2.log" || { echo "app-2 未就绪，见 $LOGS/app2.log" >&2; exit 1; }
log "app-2 ready"

# ---------- 5. console（进程 4） ----------
CONSOLE_URL=""
if [[ "$CONSOLE" == "1" && -f "$ROOT/console/node_modules/next/dist/bin/next" ]]; then
	log "构建 console wasm 并启动 console（进程 4）：:$CONSOLE_PORT"
	if [[ ! -f "$ROOT/console/public/replay.wasm" ]]; then
		(cd "$ROOT/console/wasm" && GOOS=js GOARCH=wasm GOWORK=off go build -o ../public/replay.wasm .) >>"$LOGS/console.log" 2>&1
	fi
	(
		cd "$ROOT/console"
		NEXT_PUBLIC_API_PORT="$API_PORT" WATCHPACK_POLLING=true exec node node_modules/next/dist/bin/next dev -H "$LISTEN_HOST" -p "$CONSOLE_PORT"
	) >>"$LOGS/console.log" 2>&1 &
	console_pid=$!
	CONSOLE_URL="http://${LISTEN_HOST}:${CONSOLE_PORT}/${NAMESPACE}/runs"
	for _ in {1..30}; do
		kill -0 "$console_pid" 2>/dev/null || break
		curl --max-time 1 -fsS "http://${LISTEN_HOST}:${CONSOLE_PORT}" >/dev/null 2>&1 && break
		sleep 0.5
	done
	curl --max-time 2 -fsS "http://${LISTEN_HOST}:${CONSOLE_PORT}" >/dev/null || {
		printf 'console 未就绪，见 %s/console.log\n' "$LOGS" >&2
		exit 1
	}
elif [[ "$CONSOLE" == "1" ]]; then
	printf "缺少 Console 依赖，请在 console 目录执行 pnpm install\n" >&2
	exit 1
fi

# ---------- 6. app-1（进程 2，按版本重启）与 driver（进程 5） ----------
# app-1 的 PID 写进 $READY/app1.pid：watch_loop 在子 shell 里运行，主 shell 的 cleanup 通过文件找它。
driver_pid=""
start_app1() {
	printf '%s' "$1" >"$READY/app1-version"
	: >"$LOGS/app1.log"
	log "启动 app-1（进程 2）：version=$1"
	MP_READY_DIR="$READY" TENUTO_URL="$API" TENUTO_NAMESPACE="$NAMESPACE" APP1_VERSION="$1" APP1_BUILD_ID="app1-demo" "$BIN/app1" >>"$LOGS/app1.log" 2>&1 &
	echo $! >"$READY/app1.pid"
}

# 监视 app-1：就绪写 app1-up（driver 据此推进）；app-1 退出或缺失时按 app1-next 拉起下一版本。
watch_loop() {
	local target="" current="" pid="" started=0
	while kill -0 "$driver_pid" 2>/dev/null; do
		target="$(ReadText "$READY/app1-next")"
		if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
			printf 'app-1 %s 提前退出\n' "$current" >&2
			kill -TERM "$driver_pid" 2>/dev/null || true
			return 1
		fi
		if [[ -n "$target" && "$target" != "$current" ]]; then
			rm -f "$READY/app1-up"
			if [[ -n "$pid" ]]; then
				kill -TERM "$pid" 2>/dev/null || true
				for _ in {1..50}; do
					kill -0 "$pid" 2>/dev/null || break
					sleep 0.1
				done
				kill -KILL "$pid" 2>/dev/null || true
				wait "$pid" 2>/dev/null || true
			fi
			start_app1 "$target"
			pid="$(ReadText "$READY/app1.pid")"
			current="$target"
			started=$SECONDS
		fi
		if [[ -n "$pid" && ! -e "$READY/app1-up" ]]; then
			if grep -q "ready namespace" "$LOGS/app1.log"; then
				touch "$READY/app1-up"
			elif (( SECONDS - started >= 15 )); then
				printf 'app-1 %s 在 15 秒内未就绪\n' "$current" >&2
				kill -TERM "$driver_pid" 2>/dev/null || true
				return 1
			fi
		fi
		sleep 0.1
	done
}

rm -f "$READY/app1.pid" "$READY/app1-up" "$READY/app1-next"
# driver 与监视循环各在一个子 shell 里并发运行，父 shell 等 driver 结束再收尾。
run_driver() {
	MP_READY_DIR="$READY" exec "$BIN/driver" --url "$API" --namespace "$NAMESPACE" --ready-dir "$READY" >"$LOGS/driver.log" 2>&1
}
( run_driver ) &
driver_pid=$!
( watch_loop ) &
watcher_pid=$!
printf "Tenuto Console: %s\nTenuto API: %s\n" "${CONSOLE_URL:-未启动}" "$API"
log "driver（进程 5）开始；实时结果：$LOGS/driver.log"
rc=0
wait "$driver_pid" || rc=1
ReadText "$LOGS/driver.log"


echo
echo "=================================================================="
echo " Tenuto Console: ${CONSOLE_URL:-未启动（MP_CONSOLE=0）}"
echo " Tenuto API    : $API"
echo " 场景日志      : $LOGS/driver.log"
echo "=================================================================="
if [[ "$rc" -ne 0 ]]; then
	echo "RESULT FAIL（driver 返回非 0）"
	exit 1
fi
if [[ -n "$CONSOLE_URL" ]]; then
	log "console 保持运行以查看 Run 历史；按 Ctrl-C 结束全部进程"
	if [[ "${MP_HOLD_SECONDS:-0}" -gt 0 ]]; then
		sleep "$MP_HOLD_SECONDS"
	else
		while :; do sleep 1; done
	fi
fi
