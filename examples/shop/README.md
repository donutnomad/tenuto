# shop：多进程下单示例

一个完整的 Go 业务模块：三个独立的服务进程通过 Tenuto Server 协作完成下单。

| 进程 | 职责 | Task Queue |
| --- | --- | --- |
| `tenuto-server` | Workflow 引擎；执行全部 Workflow Task | — |
| `order-service` | HTTP 下单 API；登记 `place-order` Workflow；发送顾客通知 | `orders` |
| `payment-service` | 扣款（幂等；可模拟网关超时） | `payments` |
| `warehouse-service` | 预留库存、释放库存、发货（幂等） | `warehouse` |

```
            POST /orders                     ┌────────────────────┐
  curl ───────────────▶ order-service ──────▶│   tenuto-server    │
                        (orders 队列)  start │ Workflow 引擎       │
                                             │ Event History      │
                payment-service ◀────────────│ Task Queue         │
                (payments 队列)   poll/完成   │                    │
                warehouse-service ◀──────────│                    │
                (warehouse 队列)             └────────────────────┘
```

三个服务之间没有直接调用，也不链接彼此的实现代码，只共享 [`contracts`](contracts/contracts.go) 里的 Activity 契约。

## 流程

[`orders/workflow.go`](orders/workflow.go) 的 `place-order`：

1. `warehouse.reserve` 预留库存。缺货时返回不可重试的 `OutOfStock`，订单结束为 `rejected`。
2. `payments.charge` 扣款。超过 1000 元返回不可重试的 `PaymentDeclined`，Workflow 调用 `warehouse.release` 释放库存（补偿），订单结束为 `payment_declined`。网关超时等临时错误按 RetryPolicy 重试。
3. `warehouse.ship` 发货。
4. 等待 `delivered` Signal（顾客确认收货），或 `auto_confirm_seconds` 后由持久 Timer 自动确认。
5. `orders.notify` 通知顾客，返回订单结果。

进行中的阶段通过 Query `status` 读取。

## 一键演示

在仓库根目录执行：

```bash
./examples/shop/run-demo.sh
```

脚本构建并启动四个进程（tenuto-server 用内存后端，不需要数据库），依次演示：

| 场景 | 观察点 |
| --- | --- |
| 1. 正常下单 | payment-service 第一次扣款后模拟响应丢失；Tenuto 重试，幂等表返回同一笔 `pay_1001`，只扣一次款。随后发送 `delivered` Signal 完成订单 |
| 2. 缺货 | `lamp` 库存为 0，订单 `rejected`，不扣款 |
| 3. 拒付 | 1500 元超过限额，库存预留后被释放，订单 `payment_declined` |
| 4. 支付服务宕机 | 停掉 payment-service 后下单：库存已预留，扣款任务在 `payments` 队列里等待，progress 显示 `charging`。重启 payment-service 后它领取扣款继续，5 秒后 Timer 自动确认收货 |

`SHOP_KEEP=1 ./examples/shop/run-demo.sh` 在演示后保持进程运行，可以继续用 curl 下单。各进程日志在 `examples/shop/.run/logs/`。端口由 `SHOP_TENUTO_PORT`（默认 7400）与 `SHOP_ORDER_PORT`（默认 8080）设置。

## 手动运行

开五个终端，都在 `examples/shop` 目录：

```bash
# 1. Tenuto Server（内存后端）
(cd ../.. && WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:7400 go run ./server/cmd/tenuto-server)

# 2–4. 三个服务
export TENUTO_URL=http://127.0.0.1:7400
go run ./cmd/warehouse-service
PAYMENT_FLAKY=1 go run ./cmd/payment-service
go run ./cmd/order-service

# 5. 下单、查询、确认收货
curl -X POST localhost:8080/orders -d '{"sku":"book","qty":1,"amount":4900,"address":"上海"}'
curl localhost:8080/orders/<order_id>
curl -X POST localhost:8080/orders/<order_id>/delivered
```

初始库存：`book` 100、`mug` 5、`lamp` 0。金额单位是分。随意停止、重启任一服务进程，Run 都会从 Event History 记录的位置继续；内存后端的 tenuto-server 重启后数据清空，持久化部署使用 MySQL，见[部署 Tenuto Server](https://donutnomad.github.io/tenuto/deploy/server)。

在 Console 查看 Run、Event History 和 Blocked 原因：在仓库根目录执行 `make dev-console`，打开 http://localhost:7401 ，Namespace 选 `shop`。

## 代码结构

| 路径 | 内容 |
| --- | --- |
| `contracts/` | 跨服务 Activity 契约：`@RemoteActivity(...)` 命名函数类型与入参出参 |
| `orders/` | `place-order` Workflow、`orders.notify` Activity |
| `payment/`、`warehouse/` | 两个服务的 Activity 实现，以订单号为幂等键 |
| `cmd/*-service/` | 三个进程入口 |
| `internal/svc/` | 连接 Tenuto、启动 Activity Worker 的公共代码 |

`contracts` 与 `orders` 的 `zz_*_gen.go`、`*.json` 由生成器产生。修改 Workflow、Activity 签名或注解后执行：

```bash
go generate ./...
```

`go.mod` 用 `replace` 指向仓库里的 Tenuto 源码。在自己的项目里删掉 `replace`，执行 `go get github.com/donutnomad/tenuto@latest`；接入步骤见[接入已有项目](https://donutnomad.github.io/tenuto/quickstart/integrate)。
