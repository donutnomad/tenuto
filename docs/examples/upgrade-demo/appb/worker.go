// Package appb 是 app-b（订单）的 SDK 接入，结构与 appa.Start 相同。
//
// app-b 的 Workflow 调用 app-a 的 Activity。那些 Activity 在 app-b 里只有 remote=true 的
// 契约声明，没有实现：生成的 RegisterActivities 只登记本地的 orders.price，
// app-b 的 Worker 也只消费自己的 orders 队列。跨进程调用不需要额外的接入代码。
package appb

import (
	"context"
	"fmt"
	"time"

	v1 "github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appb/v1"
	v2 "github.com/donutnomad/tenuto/docs/examples/upgrade-demo/appb/v2"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
)

// Queue 是 app-b 消费的 Activity Task Queue，与 @Activity / @Workflow 注解里的 queue 一致。
const Queue = "orders"

// Options 是启动一个 app-b Worker 需要的全部配置。
type Options struct {
	URL       string // Tenuto Server 的 HTTP 地址
	Namespace string
	Release   string // v1 / v2
}

// release 是一个发布版本随二进制带出来的三样东西。
type release struct {
	build      string                         // BuildID：每个发布版本一个，新旧 build 靠它并存
	activities func(*activity.Registry) error // 生成的 RegisterActivities
	workflow   func() registration.Definition // 生成的 WorkflowDefinition
}

var releases = map[string]release{
	"v1": {v1.Build, v1.RegisterActivities, v1.WorkflowDefinition},
	"v2": {v2.Build, v2.RegisterActivities, v2.WorkflowDefinition},
}

// Start 启动 Worker 并等它就绪。调用方在退出时调用返回值的 Stop。
func Start(ctx context.Context, opts Options) (*worker.Worker, error) {
	rel, ok := releases[opts.Release]
	if !ok {
		return nil, fmt.Errorf("app-b: unknown release %q (want v1 or v2)", opts.Release)
	}

	// 1. Registry 同时持有 Activity 的实现与契约（type、version、入参出参指纹）。
	//    生成的 RegisterActivities 把本包全部 @Activity 一次登记进去。
	registry := activity.NewRegistry()
	if err := rel.activities(registry); err != nil {
		return nil, fmt.Errorf("app-b: register activities: %w", err)
	}

	// 2. Client 连接 Server；Worker 经它领取并提交 Workflow Task 与 Activity Task，上报 Activity 路由与契约
	//    （并维持心跳）。Worker 停止后连接随进程退出释放。
	c, err := client.Dial(client.Options{HostPort: opts.URL, Namespace: opts.Namespace})
	if err != nil {
		return nil, err
	}

	// 3. worker.New 创建同时执行 Workflow Task 与 Activity Task 的 Worker。
	w, err := worker.New(c, worker.Options{
		Role:               worker.RoleBoth,
		Activities:         registry,
		ActivityTaskQueues: []string{Queue},
		// Identity 标识这个进程实例，BuildID 标识代码版本。Server 按 BuildID 保存每个版本上报的契约：
		// 新旧版本的 BuildID 不同，两个进程才能同时在线、各自保留自己的契约。任务投到队列，不带 build；
		// 领到任务的进程开始执行前，Server 按它这个 BuildID 的契约对照 Program 的声明。
		Identity: rel.build + "-1",
		BuildID:  rel.build,
		// Workflows 是随二进制发布的 Workflow 定义。Start 在开始 poll 之前登记它：
		// 新内容首次出现时成为 latest，同一份内容重复登记时 latest 保持不变。
		Workflows: []registration.Definition{rel.workflow()},
		// Default 只在 Activity 没有指定队列、多个队列承接它时参与选择队列。
		// Worker.Stop 注销路由，停掉的 build 立刻退出选路。
		ActivityWorkerVersionDefault: true,
	})
	if err != nil {
		return nil, err
	}

	// 4. Start 的顺序：上报 Activity 路由与契约 → 登记 Workflow 定义 → 开始 poll。
	//    路由先于 Program 生效，所以新 Program 成为 latest 的那一刻，它声明的契约已经有 Worker 承接。
	w.Start(ctx)

	// 5. WaitReady 返回上面前两步的结果。登记失败时 Worker 不 poll，错误在这里返回。
	readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := w.WaitReady(readyCtx); err != nil {
		w.Stop()
		return nil, fmt.Errorf("app-b %s: worker not ready: %w", opts.Release, err)
	}
	return w, nil
}
