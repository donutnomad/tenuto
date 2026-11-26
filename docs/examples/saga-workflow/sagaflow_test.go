package sagaflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sagaflow "github.com/donutnomad/tenuto/docs/examples/saga-workflow"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/apperror"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

// scenario 描述一次真实解释器执行：入参、期望终态、Activity 调度顺序、终态错误/输出关键字。
type scenario struct {
	name string
	// scenario 是 OrderInput.Scenario 的取值。
	input string
	// wantCalls 是 history 里 ActivityScheduled 事件的 ActivityType 顺序。
	wantCalls []string
	wantErr   string
	wantOut   string
	// wantType 是 Workflow 终态结构化失败的 Type；空串表示只有 message（untyped 或普通错误）。
	wantType string
	// wantCauseType 是终态失败 cause 的 Type；空串表示没有 cause。
	wantCauseType string
	// wantActivityRetryState 是 ChargePayment 终态 ActivityFailed 的 RetryState
	// （工作流级 RetryState 恒空：生成的 workflow.json 没有 workflow retry_policy）。
	wantActivityRetryState string
	// wantChargeFailures 是 ChargePayment 的中间失败次数（ActivityAttemptFailed = 总尝试数-1）。
	wantChargeFailures int
}

var scenarios = []scenario{
	{
		name:      "success",
		input:     "",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment", "saga.create-shipment"},
		wantOut:   "shipment-o-1",
	},
	{
		name:      "payment_declined_releases_inventory",
		input:     "payment_declined",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment", "saga.release-inventory"},
		wantErr:   "payment declined, reserved inventory released",
		wantType:  "PaymentDeclined",
	},
	{
		// typed 永久失败：Activity 返回 NonRetryable + Type=PaymentDeclined。
		// Workflow 按 type 分支自动补偿，不重试，终态错误携带同一 type。
		name:                   "typed_payment_declined_compensates_by_type",
		input:                  "payment_declined_error",
		wantCalls:              []string{"saga.reserve-inventory", "saga.charge-payment", "saga.release-inventory"},
		wantErr:                "payment declined by provider, reason=insufficient_funds, reserved inventory released",
		wantType:               "PaymentDeclined",
		wantActivityRetryState: "NonRetryableFailure",
		wantChargeFailures:     0,
	},
	{
		// typed 临时失败：可重试，MaxAttempts=3 用尽后终态保留最后一次的 typed 失败。
		// 结果不确定 → 不自动补偿，交给人工核对。
		name:                   "typed_payment_unavailable_retries_then_manual",
		input:                  "payment_unavailable_error",
		wantCalls:              []string{"saga.reserve-inventory", "saga.charge-payment"},
		wantErr:                "charge payment outcome unknown, manual reconciliation required",
		wantType:               "SagaManualReconciliation",
		wantCauseType:          "PaymentUnavailable",
		wantActivityRetryState: "MaximumAttemptsReached",
		wantChargeFailures:     2,
	},
	{
		name:      "shipment_rejected_refunds_then_releases",
		input:     "shipment_rejected",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment", "saga.create-shipment", "saga.refund-payment", "saga.release-inventory"},
		wantErr:   "shipment rejected, compensation done=refund_payment,release_inventory",
		wantType:  "ShipmentRejected",
	},
	{
		name:      "refund_failed_still_releases",
		input:     "shipment_rejected_refund_failed",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment", "saga.create-shipment", "saga.refund-payment", "saga.release-inventory"},
		wantErr:   "compensation_failed=refund_payment ",
		wantType:  "SagaCompensationFailed",
	},
	{
		name:      "payment_unknown_requires_manual_reconciliation",
		input:     "payment_unknown",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment"},
		wantErr:   "charge payment outcome unknown, manual reconciliation required",
		// 普通 error 没有 type：终态 type 是 workflow 自己的 SagaManualReconciliation，
		// cause 是 untyped（Type 为空）。
		wantType: "SagaManualReconciliation",
		// 普通 error 与 typed 临时失败同样按 RetryPolicy 重试：3 次尝试 = 2 次中间失败。
		wantActivityRetryState: "MaximumAttemptsReached",
		wantChargeFailures:     2,
	},
	{
		name:      "payment_unknown_status_is_not_treated_as_success",
		input:     "payment_pending",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment"},
		wantErr:   "charge payment returned unknown status, manual reconciliation required",
		wantType:  "SagaManualReconciliation",
	},
	{
		name:      "shipment_unknown_requires_manual_reconciliation",
		input:     "shipment_unknown",
		wantCalls: []string{"saga.reserve-inventory", "saga.charge-payment", "saga.create-shipment"},
		wantErr:   "create shipment outcome unknown, manual reconciliation required",
		wantType:  "SagaManualReconciliation",
	},
}

// TestSagaOrderScenarios 用真实 WLang 解释器执行生成产物，锁住补偿顺序与失败语义。
func TestSagaOrderScenarios(t *testing.T) {
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			getErr, history := runOrder(t, sc.input)

			if got := scheduledActivityTypes(history); !equalStrings(got, sc.wantCalls) {
				t.Fatalf("activity 调度顺序 = %v, want %v", got, sc.wantCalls)
			}

			if sc.wantErr == "" {
				if getErr != nil {
					t.Fatalf("Get = %v, want completed; events=%+v", getErr, history)
				}
				completed := findEvent(history, historypkg.EventWorkflowCompleted)
				if completed == nil {
					t.Fatalf("缺少 WorkflowCompleted 事件")
				}
				output, err := json.Marshal(completed.WorkflowCompleted.Output)
				if err != nil {
					t.Fatalf("encode output: %v", err)
				}
				if !strings.Contains(string(output), sc.wantOut) {
					t.Fatalf("output = %s, 缺少 %q", output, sc.wantOut)
				}
				return
			}

			// WorkflowRun.Get 返回 *client.WorkflowExecutionError，原因是带同一 Type 的应用错误。
			var executionErr *client.WorkflowExecutionError
			var appErr *apperror.Error
			if !errors.As(getErr, &executionErr) || !errors.As(getErr, &appErr) {
				t.Fatalf("Get = %v, want a workflow execution error; events=%+v", getErr, history)
			}
			if appErr.Type() != sc.wantType || !strings.Contains(appErr.Message(), sc.wantErr) {
				t.Fatalf("Get error type = %q message = %q, want %q / %q", appErr.Type(), appErr.Message(), sc.wantType, sc.wantErr)
			}
			failed := findEvent(history, historypkg.EventWorkflowFailed)
			if failed == nil {
				t.Fatalf("缺少 WorkflowFailed 事件")
			}
			if !strings.Contains(failed.WorkflowFailed.Error, sc.wantErr) {
				t.Fatalf("失败信息 = %q, want 包含 %q", failed.WorkflowFailed.Error, sc.wantErr)
			}
			// 生成的 workflow.json 没有 workflow 级 retry_policy：Run 失败是终态，不会新建 Run。
			if failed.WorkflowFailed.NewExecutionRunID != "" {
				t.Fatalf("默认配置下不应新建 retry Run: %+v", failed.WorkflowFailed)
			}
			if failed.WorkflowFailed.RetryState != "" {
				t.Fatalf("工作流级 RetryState = %q，生成配置没有 workflow retry_policy 时应为空", failed.WorkflowFailed.RetryState)
			}
			// 终态结构化失败：Type 必须与 Activity 上报的 type 一致，message 与 error 镜像一致。
			gotType := ""
			if failed.WorkflowFailed.Failure != nil {
				gotType = failed.WorkflowFailed.Failure.Type
				if failed.WorkflowFailed.Failure.Message != failed.WorkflowFailed.Error {
					t.Fatalf("Failure.Message = %q 与 Error = %q 不一致",
						failed.WorkflowFailed.Failure.Message, failed.WorkflowFailed.Error)
				}
			}
			if gotType != sc.wantType {
				t.Fatalf("TerminalFailure.Type = %q, want %q", gotType, sc.wantType)
			}
			// cause 链：typed 临时失败经重试耗尽后，终态失败的 cause 必须保留最后一次的 typed 失败，
			// 且不得携带空 detail（apperror.New 的可变参数是 details，末尾多余的 nil 会写成 null payload）。
			if sc.wantCauseType != "" {
				if failed.WorkflowFailed.Failure == nil || failed.WorkflowFailed.Failure.Cause == nil {
					t.Fatalf("终态失败缺少 cause: %+v", failed.WorkflowFailed.Failure)
				}
				cause := failed.WorkflowFailed.Failure.Cause
				if cause.Type != sc.wantCauseType {
					t.Fatalf("cause.Type = %q, want %q", cause.Type, sc.wantCauseType)
				}
				if len(cause.Details) != 0 {
					t.Fatalf("cause %s 不应携带 details，实际 %d 项: %+v", cause.Type, len(cause.Details), cause.Details)
				}
			}
			if got := chargeAttemptFailures(history); got != sc.wantChargeFailures {
				t.Fatalf("ChargePayment 中间失败次数 = %d, want %d", got, sc.wantChargeFailures)
			}
			if got := chargeTerminalRetryState(history); got != sc.wantActivityRetryState {
				t.Fatalf("ChargePayment 终态 RetryState = %q, want %q", got, sc.wantActivityRetryState)
			}
		})
	}
}

// TestRegisterActivitiesReportsContracts 锁住生成 helper 的注册结果：
// 5 个本地 Activity 全部进 registry，并带 type 与 wire 指纹契约。
func TestRegisterActivitiesReportsContracts(t *testing.T) {
	registry := wfsdk.NewRegistry()
	if err := sagaflow.RegisterActivities(registry); err != nil {
		t.Fatalf("RegisterActivities: %v", err)
	}
	registered := map[string]bool{}
	for _, activityType := range registry.Types() {
		registered[activityType] = true
	}
	reported := registry.Contracts()
	if len(reported) != 5 {
		t.Fatalf("上报契约数 = %d, want 5: %+v", len(reported), reported)
	}
	for _, contract := range reported {
		if !registered[contract.Type] {
			t.Fatalf("契约 %s 没有对应实现: %v", contract.Type, registered)
		}
		if !strings.HasPrefix(contract.Input, "wire:") || !strings.HasPrefix(contract.Output, "wire:") {
			t.Fatalf("契约元数据不完整: %+v", contract)
		}
	}
}

// runOrder 启动内存后端的 Server，经 sdk/worker 登记生成的 Workflow 定义与 Activity，用 sdk/client 跑一次 Run，
// 返回 WorkflowRun.Get 的结果与这次 Run 的历史。
func runOrder(t *testing.T, scenarioValue string) (error, []historypkg.Event) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	server := assembly.NewServer(assembly.Options{Stores: assembly.Stores{Executions: memory.NewExecutionManager()}, WorkflowQueue: memory.NewTaskQueue(16), ActivityRouter: memory.NewActivityQueueRouter(16)})
	serverCtx, stopServer := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(runtimehttp.NewRuntimeHTTPHandler(server))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.Start()
	t.Cleanup(func() {
		stopServer()
		httpServer.Close()
		server.Stop()
	})

	c, err := client.Dial(client.Options{HostPort: httpServer.URL, Namespace: "saga-test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	registry := wfsdk.NewRegistry()
	if err := sagaflow.RegisterActivities(registry); err != nil {
		t.Fatalf("RegisterActivities: %v", err)
	}
	w, err := worker.New(c, worker.Options{
		Workflows:  []registration.Definition{sagaflow.WorkflowDefinition()},
		Activities: registry,
		Identity:   "saga-test-worker",
		BuildID:    "saga-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	t.Cleanup(w.Stop)
	if err := w.WaitReady(ctx); err != nil {
		t.Fatalf("worker ready: %v", err)
	}

	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: "order-o-1"}, "saga-order", sagaflow.OrderInput{
		OrderID: "o-1", Amount: 1999, Scenario: scenarioValue,
	})
	if err != nil {
		t.Fatalf("ExecuteWorkflow: %v", err)
	}
	var out sagaflow.OrderOutput
	getErr := run.Get(ctx, &out)
	var history []historypkg.Event
	events := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID())
	for events.HasNext() {
		event, err := events.Next()
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		history = append(history, *event)
	}
	return getErr, history
}

func scheduledActivityTypes(history []historypkg.Event) []string {
	var types []string
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityScheduled {
			types = append(types, event.ActivityScheduled.ActivityType)
		}
	}
	return types
}

func findEvent(history []historypkg.Event, eventType historypkg.EventType) *historypkg.Event {
	for index := range history {
		if history[index].EventType() == eventType {
			return &history[index]
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// chargeAttemptFailures 统计 saga.charge-payment 的中间失败次数（ActivityAttemptFailed）。
// 中间失败事件不带 ActivityType，因此先用 ActivityScheduled 建 scheduledEventID → ActivityType 映射。
func chargeAttemptFailures(history []historypkg.Event) int {
	chargeScheduled := map[int64]bool{}
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityScheduled &&
			event.ActivityScheduled != nil &&
			event.ActivityScheduled.ActivityType == "saga.charge-payment" {
			chargeScheduled[event.ID] = true
		}
	}
	count := 0
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityAttemptFailed &&
			event.ActivityAttemptFailed != nil &&
			chargeScheduled[event.ActivityAttemptFailed.ScheduledEventID] {
			count++
		}
	}
	return count
}

// chargeTerminalRetryState 返回 saga.charge-payment 终态 ActivityFailed 的 RetryState。
func chargeTerminalRetryState(history []historypkg.Event) string {
	chargeScheduled := map[int64]bool{}
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityScheduled &&
			event.ActivityScheduled != nil &&
			event.ActivityScheduled.ActivityType == "saga.charge-payment" {
			chargeScheduled[event.ID] = true
		}
	}
	for _, event := range history {
		if event.EventType() == historypkg.EventActivityFailed &&
			event.ActivityFailed != nil &&
			chargeScheduled[event.ActivityFailed.ScheduledEventID] {
			return event.ActivityFailed.RetryState
		}
	}
	return ""
}
