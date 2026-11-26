#!/usr/bin/env bash
# shop 演示：tenuto-server + order-service + payment-service + warehouse-service 四个独立进程，
# 依次演示正常下单、缺货、拒付补偿、支付服务宕机后恢复。
#
#   ./examples/shop/run-demo.sh            # 演示结束后清理进程
#   SHOP_KEEP=1 ./examples/shop/run-demo.sh  # 演示结束后保持运行，Ctrl-C 退出
#
# 环境变量：SHOP_TENUTO_PORT（默认 7400）、SHOP_ORDER_PORT（默认 8080）。
# tenuto-server 使用内存后端，不需要数据库；进程退出后数据清空。
set -euo pipefail

SHOP="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SHOP/../.." && pwd)"
RUN="$SHOP/.run"
BIN="$RUN/bin"
LOGS="$RUN/logs"
TENUTO_PORT="${SHOP_TENUTO_PORT:-7400}"
ORDER_PORT="${SHOP_ORDER_PORT:-8080}"
export TENUTO_URL="http://127.0.0.1:${TENUTO_PORT}"
API="http://127.0.0.1:${ORDER_PORT}"
export GOWORK=off

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
note() { printf '\033[2m    %s\033[0m\n' "$*"; }

# 进程号存在 pid_<名称> 变量里（macOS 自带的 bash 3.2 没有关联数组）。
NAMES="order payment warehouse tenuto_server"
cleanup() {
	local status=$? name pid
	trap - EXIT INT TERM
	for name in $NAMES; do
		pid="$(eval "echo \${pid_$name:-}")"
		if [[ -n "$pid" ]]; then kill -TERM "$pid" 2>/dev/null || true; fi
	done
	wait 2>/dev/null || true
	if [[ $status -ne 0 && $status -ne 130 ]]; then
		for f in "$LOGS"/*.log; do echo "---- $(basename "$f") ----" >&2; tail -n 20 "$f" >&2; done
	fi
	exit $status
}
trap cleanup EXIT
trap 'exit 130' INT TERM

for port in "$TENUTO_PORT" "$ORDER_PORT"; do
	if lsof -ti:"$port" >/dev/null 2>&1; then
		echo "端口 $port 已被占用；用 SHOP_TENUTO_PORT / SHOP_ORDER_PORT 换一个" >&2
		exit 1
	fi
done

rm -rf "$LOGS"
mkdir -p "$BIN" "$LOGS"

step "构建 tenuto-server 与三个服务"
(cd "$ROOT" && go build -o "$BIN/tenuto-server" ./server/cmd/tenuto-server)
(cd "$SHOP" && go build -o "$BIN/" ./cmd/...)

# start <名称> <命令…>：后台启动，输出写到 .run/logs/<名称>.log。
start() {
	local name=$1
	shift
	"$@" >"$LOGS/$name.log" 2>&1 &
	printf -v "pid_$name" '%s' "$!"
}

# wait_log <名称> <文本>：等日志里出现文本。
wait_log() {
	for _ in $(seq 1 300); do
		grep -q "$2" "$LOGS/$1.log" 2>/dev/null && return 0
		sleep 0.1
	done
	echo "$1 没有输出 \"$2\"" >&2
	return 1
}

stop() {
	local pid
	pid="$(eval "echo \${pid_$1:-}")"
	kill -TERM "$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
	printf -v "pid_$1" '%s' ""
}

step "启动 tenuto-server（进程 1，内存后端，${TENUTO_URL}）"
WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR="127.0.0.1:${TENUTO_PORT}" \
	WORKFLOW_TASK_TOKEN_SECRET=insecure-local-demo-task-token-secret \
	start tenuto_server "$BIN/tenuto-server"
for _ in $(seq 1 100); do curl -fsS "$TENUTO_URL/health/ready" >/dev/null 2>&1 && break; sleep 0.1; done
curl -fsS "$TENUTO_URL/health/ready" >/dev/null

step "启动 warehouse-service（进程 2）、payment-service（进程 3）、order-service（进程 4）"
start warehouse "$BIN/warehouse-service"
PAYMENT_FLAKY=1 start payment "$BIN/payment-service"
ORDER_HTTP_ADDR="127.0.0.1:${ORDER_PORT}" start order "$BIN/order-service"
wait_log warehouse "ready"
wait_log payment "ready"
wait_log order "ready"
note "payment-service 以 PAYMENT_FLAKY=1 启动：每笔订单第一次扣款成功后模拟网关超时"

# place <json>：下单，打印并返回订单号。
place() {
	local resp
	resp=$(curl -fsS -X POST "$API/orders" -H 'Content-Type: application/json' -d "$1")
	note "POST /orders $1"
	note "→ $resp"
	ORDER_ID=$(printf '%s' "$resp" | sed -E 's/.*"order_id":"([^"]+)".*/\1/')
}

show() {
	local resp
	resp=$(curl -fsS "$API/orders/$1")
	if command -v jq >/dev/null; then printf '%s' "$resp" | jq .; else printf '%s\n' "$resp"; fi
}

# wait_for <订单号> <文本> [秒]：轮询 GET /orders/<id> 直到响应包含文本。
wait_for() {
	local deadline=$((SECONDS + ${3:-30}))
	while ((SECONDS < deadline)); do
		curl -fsS "$API/orders/$1" 2>/dev/null | grep -q "$2" && return 0
		sleep 0.3
	done
	echo "订单 $1 在 ${3:-30}s 内没有到达 $2" >&2
	show "$1" >&2
	return 1
}

logs_for() {
	grep -h "order=$1" "$LOGS/warehouse.log" "$LOGS/payment.log" "$LOGS/order.log" 2>/dev/null | sed 's/^/    /' || true
}

step "场景 1：正常下单。扣款第一次超时，Tenuto 重试，幂等表保证只扣一次"
place '{"order_id":"1001","sku":"book","qty":2,"amount":9800,"address":"上海市徐汇区 1 号"}'
wait_for 1001 '"stage":"awaiting_delivery"'
show 1001
note "已发货，Workflow 在持久 Timer 与 delivered Signal 之间等待。发送确认收货："
curl -fsS -X POST "$API/orders/1001/delivered" >/dev/null
wait_for 1001 '"status":"completed"'
show 1001
logs_for 1001

step "场景 2：缺货。warehouse 返回不可重试的 OutOfStock，订单直接 rejected"
place '{"order_id":"1002","sku":"lamp","qty":1,"amount":19900,"address":"北京市海淀区 2 号"}'
wait_for 1002 '"status":"rejected"'
show 1002
logs_for 1002

step "场景 3：拒付。payment 返回 PaymentDeclined，Workflow 释放已预留的库存（补偿）"
place '{"order_id":"1003","sku":"mug","qty":3,"amount":150000,"address":"杭州市西湖区 3 号"}'
wait_for 1003 '"status":"payment_declined"'
show 1003
logs_for 1003

step "场景 4：payment-service 宕机。扣款任务在队列里等待，服务恢复后从原处继续"
stop payment
note "已停止 payment-service"
place '{"order_id":"1004","sku":"book","qty":1,"amount":4900,"address":"深圳市南山区 4 号","auto_confirm_seconds":5}'
wait_for 1004 '"stage":"charging"'
show 1004
note "库存已预留；扣款已调度到 payments 队列，队列上没有在线 Worker，任务在队列里等待。重新启动 payment-service："
PAYMENT_FLAKY=0 start payment "$BIN/payment-service"
wait_log payment "ready"
wait_for 1004 '"stage":"awaiting_delivery"' 30
note "已发货；这一单不发 Signal，5 秒后持久 Timer 到期自动确认收货"
wait_for 1004 '"status":"completed"' 20
show 1004
logs_for 1004

step "演示完成"
note "进程日志：$LOGS/"
note "在 Console 查看 Run 与 Event History：仓库根目录执行 make dev-console APP_PORT=${TENUTO_PORT}，"
note "打开 http://localhost:7401 ，Namespace 选 shop"
if [[ "${SHOP_KEEP:-0}" == "1" ]]; then
	note "进程保持运行，Ctrl-C 退出。可以继续调用："
	note "  curl -X POST $API/orders -d '{\"sku\":\"book\",\"qty\":1,\"amount\":4900,\"address\":\"...\"}'"
	note "  curl $API/orders/<id>"
	note "  curl -X POST $API/orders/<id>/delivered"
	wait
fi
