---
title: 接入已有项目
description: 在自己的 Go 模块里引入 Tenuto SDK 与生成器，写注解、生成代码、注册 Workflow 与 Activity、启动 Run
---

# 接入已有项目

本页从一个已有的 Go 模块出发，接入 Tenuto 并跑通一次 Workflow：引入 SDK 与生成器，用注解声明 Workflow 和 Activity，生成调用桥与定义，在 Worker 里注册，从业务代码启动 Run。示例模块是 `example.com/shop`，按自己的模块路径替换。

## 前置条件

| 项 | 要求 |
| --- | --- |
| Go | 1.27 或更高。引入 Tenuto 后 `go.mod` 的 `go` 指令会提升到 `1.27.0` |
| Tenuto Server | 本地开发用内存后端，见[本地启动 Server](#本地启动-server)；生产部署见[部署 Tenuto Server](/deploy/server) |

## 1. 引入 SDK 与生成器

在模块根目录执行：

```bash
go get github.com/donutnomad/tenuto@latest
go get -tool github.com/donutnomad/tenuto/compiler/cmd/tenuto-workflow-gen@latest
```

第一条引入 SDK（`sdk/workflow`、`sdk/activity`、`sdk/client`、`sdk/worker`、`sdk/registration`）。第二条把生成器登记为模块的 tool：`go.mod` 多出一行 `tool github.com/donutnomad/tenuto/compiler/cmd/tenuto-workflow-gen`，之后用 `go tool tenuto-workflow-gen` 调用。生成器与 SDK 来自同一个模块，版本由同一条 `require` 固定，升级时一起升级。

## 2. 用注解声明 Workflow 与 Activity

一个包目录放一个 Workflow，以及它调用的 Activity（Activity 也可以放在独立的包里，见[注解与代码生成](/develop/code-generation)）。

```go
// orders/workflow.go
// Package orders 是订单流程：一个 Workflow 和它调用的两个 Activity。
package orders

//go:generate go tool tenuto-workflow-gen generate .

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/tenuto/sdk/workflow"
)

type OrderInput struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

type OrderOutput struct {
	ChargeID string `json:"chargeId"`
	Status   string `json:"status"`
}

type ChargeInput struct {
	OrderID string `json:"orderId"`
	Amount  int64  `json:"amount"`
}

type ChargeOutput struct {
	ChargeID string `json:"chargeId"`
}

type NotifyInput struct {
	OrderID string `json:"orderId"`
}

type NotifyOutput struct {
	Sent bool `json:"sent"`
}

// @Workflow(type="place-order",name="Place Order",policy=auto_upgrade,version=v1)
func PlaceOrder(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	opts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         workflow.RetryPolicy{MaxAttempts: 5},
	}
	charge, err := CallCharge(ctx, ChargeInput{OrderID: in.OrderID, Amount: in.Amount}, opts)
	if err != nil {
		return OrderOutput{}, err
	}
	if _, err := CallNotify(ctx, NotifyInput{OrderID: in.OrderID}, opts); err != nil {
		return OrderOutput{}, err
	}
	return OrderOutput{ChargeID: charge.ChargeID, Status: "paid"}, nil
}

// @Activity(type="orders.charge")
func Charge(_ context.Context, in ChargeInput) (ChargeOutput, error) {
	// 真实实现调用支付网关，并以 OrderID 作为幂等键：Activity 至少执行一次。
	return ChargeOutput{ChargeID: fmt.Sprintf("ch_%s", in.OrderID)}, nil
}

// @Activity(type="orders.notify")
func Notify(_ context.Context, in NotifyInput) (NotifyOutput, error) {
	return NotifyOutput{Sent: true}, nil
}
```

注解写在函数的文档注释里，格式 `@Kind(key=value,...)`：

| 注解 | key | 含义 |
| --- | --- | --- |
| `@Workflow` | `type`（必填） | Workflow Type，启动 Run 时用它指明运行哪个 Workflow |
| | `name` | Console 中的显示名 |
| | `policy` | `auto_upgrade`（缺省）：运行中的 Run 在下一个 Workflow Task 换到新版本；`pinned`：Run 固定在启动时的版本。见[版本管理](/develop/versioning) |
| | `version` | 业务版本标签 |
| `@Activity` | `type`（必填） | Activity Type，同时决定生成的调用桥名：`orders.charge` → `CallCharge` |
| | `queue` | Activity Task Queue；省略时由执行它的 Worker 决定（`ActivityTaskQueues`，不写时是 `default`） |
| `@RemoteActivity` | `type`（必填）、`queue` | 声明由其他服务实现的 Activity，写在命名函数类型上 |

签名规则：

- Workflow：`func(workflow.Context, In) (Out, error)`。
- Activity：`func(context.Context, In) (Out, error)`，包级普通函数。
- `In` 与 `Out` 是结构体（或 `map[string]any`），在线上编码为 JSON 对象。
- 每个 Activity 调用都设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`，缺少时生成器报出调用点。

Workflow 函数体会被翻译成确定性的 WLang Program 在服务端重放：读时间用 `workflow.Now`、等待用 `workflow.Sleep`，IO、数据库、HTTP 放进 Activity。可用的 Go 写法见 [Workflow 可用的 Go 语法](/develop/go-subset) 与 [确定性约束](/develop/determinism)。

Activity 至少执行一次：Worker 崩溃、结果没有落库时会重试。每个 Activity 都要幂等，例如以业务 ID 作为下游的幂等键。

## 3. 生成代码

```bash
go generate ./...
```

`orders` 目录里多出这些文件：

| 文件 | 内容 |
| --- | --- |
| `zz_orders_contract_gen.go` | 调用桥 `CallCharge`、`CallNotify`，签名断言，`RegisterActivities` |
| `zz_orders_artifacts_gen.go` | 把 `workflow.json` 嵌入二进制，导出 `WorkflowDefinition()` |
| `workflow.json` | Workflow 定义：注解里的 id、name、policy、version，翻译成 WLang 的程序 `program`，以及调用的 Activity 契约 `activities`（type、task_queue、入参出参的结构指纹；审阅时看这一段的 diff 发现契约变化）。嵌入二进制，Worker 启动时登记给 Server |

在 `CallCharge` 第一次生成之前，编辑器会报未定义，属于正常现象。`go build` 不执行 `go:generate`，所以生成产物要和源码一起提交；CI 里先执行 `go generate ./...`，再用 `git diff --exit-code` 检查产物是否与源码一致。每次修改 Workflow、Activity 签名或注解之后都要重新生成；开发时可以用 `go tool tenuto-workflow-gen generate -watch ./orders` 在保存时自动生成。

## 4. 注册 Activity 与 Workflow

两类注册都在 Worker 启动时完成：

- **Activity**：生成的 `RegisterActivities(registry)` 一次登记本包全部 `@Activity` 实现。也可以逐个登记：`activity.RegisterActivityOnQueue(registry, "orders", "orders.charge", Charge)`。Worker 启动时把每个 Activity 的契约（type、输入输出指纹）随路由上报给 Server。
- **Workflow**：把 `WorkflowDefinition()` 放进 `worker.Options.Workflows`，`Start` 时登记到 Client 的 Namespace。版本按 `workflow.json` 的内容 hash 区分：新内容首次登记时成为该 Workflow ID 的 latest，新 Run 使用 latest；同一内容重复登记幂等，latest 保持不变。

```go
// cmd/worker/main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"example.com/shop/orders"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := client.Dial(client.Options{
		HostPort:  os.Getenv("TENUTO_URL"), // 例如 http://localhost:7400
		Namespace: "shop",
		AuthToken: os.Getenv("TENUTO_AUTH_TOKEN"), // Server 未开启认证时为空
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	registry := activity.NewRegistry()
	if err := orders.RegisterActivities(registry); err != nil {
		log.Fatal(err)
	}

	w, err := worker.New(c, worker.Options{
		Workflows:          []registration.Definition{orders.WorkflowDefinition()},
		Activities:         registry,
		BuildID:            "shop-2026.11.13", // 镜像版本或 Git commit
	})
	if err != nil {
		log.Fatal(err)
	}
	w.Start(ctx)
	defer w.Stop()
	if err := w.WaitReady(ctx); err != nil { // 登记失败时返回错误，Worker 不拉取任务
		log.Fatal(err)
	}
	log.Print("worker ready")
	<-ctx.Done()
}
```

- Worker 拉取的 Activity 队列来自 registry：`RegisterActivities` 带上了注解里的 `queue`，不需要再写 `ActivityTaskQueues`。显式设置 `ActivityTaskQueues` 时必须包含这些队列，否则 `worker.New` 返回错误。
- Namespace 不需要预先创建，第一次登记时生效。
- 不放进 `Workflows` 时，也可以在 CI 或发布流程里用生成器登记：`go tool tenuto-workflow-gen generate -url $TENUTO_URL -namespace shop -publish ./orders`（`-publish` 同时激活为 latest）。

## 5. 从业务代码启动 Run

```go
// cmd/start/main.go
package main

import (
	"context"
	"log"
	"os"

	"example.com/shop/orders"
	"github.com/donutnomad/tenuto/sdk/client"
)

func main() {
	ctx := context.Background()
	c, err := client.Dial(client.Options{
		HostPort:  os.Getenv("TENUTO_URL"),
		Namespace: "shop",
		AuthToken: os.Getenv("TENUTO_AUTH_TOKEN"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-1001"},
		"place-order", orders.OrderInput{OrderID: "1001", Amount: 8850})
	if err != nil {
		log.Fatal(err)
	}
	var out orders.OrderOutput
	if err := run.Get(ctx, &out); err != nil {
		log.Fatal(err)
	}
	log.Printf("run %s: %+v", run.GetRunID(), out)
}
```

- 第三个参数是 `@Workflow(type=...)` 的值。
- `StartWorkflowOptions.ID` 是业务上的 Workflow ID：同一个 ID 同时至多一个运行中的 Run，留空时 SDK 生成 UUID。
- 一个 `client.Client` 可以被多个 goroutine 共用，在服务启动时创建一次。Signal、Query、Update、取消与终止见 [Go Client](/sdk/client)。

## 本地启动 Server

```bash
go install github.com/donutnomad/tenuto/server/cmd/tenuto-server@latest
WORKFLOW_BACKEND=memory WORKFLOW_HTTP_ADDR=127.0.0.1:7400 tenuto-server
```

内存后端不需要数据库，进程退出后数据清空。另开两个终端：

```bash
export TENUTO_URL=http://localhost:7400
go run ./cmd/worker   # 输出 workflow definition registered ... 与 worker ready
go run ./cmd/start    # 输出 run <run-id>: {ChargeID:ch_1001 Status:paid}
```

多个服务进程分工的完整模块见[多进程下单示例](/quickstart/shop)。

要在 Console 里查看 Run 和 Event History，按[五分钟快速开始](/quickstart/)在 Tenuto 仓库里启动 Console，并把它指向这个 Server。

## 上生产前

- Server 使用 MySQL 后端，部署、迁移与配置见[部署 Tenuto Server](/deploy/server)；开启认证见[安全部署](/deploy/security)。
- `tenuto-server` 默认在进程内执行 Workflow Task，业务进程可以只运行 Activity：`worker.Options{Role: worker.RoleActivity, ...}`，`Workflows` 仍然放在这个 Worker 里登记。见[运行 Worker](/develop/workers)。
- `ActivityConcurrency` 缺省为 1，按 Activity 的耗时与下游容量调大。
- 修改已上线的 Workflow 前阅读[版本管理](/develop/versioning)：`auto_upgrade` 的运行中 Run 会换到新版本，不兼容的改动用 `workflow.GetVersion` 分支。
- 语义保证、生产配置清单与实测结果见[生产就绪](/operations/production-readiness)。
