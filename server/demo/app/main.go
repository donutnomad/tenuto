package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing/fstest"
	"time"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
	wfsdk "github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/sdk/registration"
	"github.com/donutnomad/tenuto/sdk/worker"
	"github.com/donutnomad/tenuto/telemetry"
)

type reserveInput struct {
	OrderID string `json:"order_id"`
}

type reserveOutput struct {
	ReservationID string `json:"reservation_id"`
}

type signInput struct {
	ReservationID string `json:"reservation_id"`
}

type signOutput struct {
	Signature string `json:"signature"`
}

type checkBalanceInput struct {
	WalletAddress string `json:"wallet_address"`
	Token         string `json:"token"`
}

type checkBalanceOutput struct {
	Balance float64 `json:"balance"`
}

type transferInput struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	Amount float64 `json:"amount"`
	Token  string  `json:"token"`
}

type transferOutput struct {
	TxHash     string `json:"tx_hash"`
	Successful bool   `json:"successful"`
}

type notifyInput struct {
	Message string `json:"message"`
	Channel string `json:"channel"`
}

type notifyOutput struct {
	Sent bool `json:"sent"`
}

type processPaymentInput struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
	Method  string  `json:"method"`
}

type processPaymentOutput struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

type confirmPaymentInput struct {
	PaymentID string `json:"payment_id"`
}

type confirmPaymentOutput struct {
	ConfirmedAt string `json:"confirmed_at"`
	Receipt     string `json:"receipt"`
}

type longRunningInput struct {
	TaskName string `json:"task_name"`
	Duration int    `json:"duration"` // seconds
}

type longRunningOutput struct {
	TaskName  string `json:"task_name"`
	Processed bool   `json:"processed"`
	Elapsed   int    `json:"elapsed"`
}

type flakyInput struct {
	OrderID string `json:"order_id"`
}

type flakyOutput struct {
	Settled bool `json:"settled"`
}

// mustRegisterActivity 注册 activity；activity type 重复时 demo 启动失败。
func mustRegisterActivity[I, O any](registry *wfsdk.Registry, activityType string, fn func(context.Context, I) (O, error)) {
	if err := wfsdk.RegisterActivity(registry, activityType, "", fn); err != nil {
		log.Fatalf("register activity: %v", err)
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	observability, err := telemetry.NewFromEnv(ctx, telemetry.Config{
		ServiceName:       "tenuto-demo-worker",
		ServiceInstanceID: os.Getenv("HOSTNAME"),
	})
	if err != nil {
		log.Fatalf("initialize worker telemetry: %v", err)
	}
	providers := observability.Providers()
	slog.SetDefault(providers.Logger)
	defer func() {
		if err := observability.Shutdown(context.Background()); err != nil {
			log.Printf("shutdown worker telemetry: %v", err)
		}
	}()

	namespace := "demo"
	serverURL := strings.TrimRight(os.Getenv("WORKFLOW_SERVER_URL"), "/")
	if serverURL == "" {
		serverURL = "http://localhost:7400"
	}
	if err := waitForServerReady(ctx, serverURL, 30*time.Second); err != nil {
		log.Fatalf("wait for workflow server: %v", err)
	}
	log.Printf("connected to workflow server at %s", serverURL)

	// ── 1. Start workers ──
	// Server 开启 WORKFLOW_HTTP_AUTH_TOKEN 时，TENUTO_AUTH_TOKEN 给出其中一个 token。
	c, err := client.Dial(client.Options{
		HostPort:  serverURL,
		Namespace: namespace,
		AuthToken: strings.TrimSpace(os.Getenv("TENUTO_AUTH_TOKEN")),
		Telemetry: providers,
	})
	if err != nil {
		log.Fatalf("dial workflow server: %v", err)
	}
	defer c.Close()

	// Activity worker (all activities on default queue)
	registry := wfsdk.NewRegistry()
	reserveFn := func(ctx context.Context, input reserveInput) (reserveOutput, error) {
		fmt.Printf("  [reserve] order=%s\n", input.OrderID)
		return reserveOutput{ReservationID: "reservation-" + input.OrderID}, nil
	}
	mustRegisterActivity(registry, "custodian.Reserve", reserveFn)
	mustRegisterActivity(registry, "activities.Reserve", reserveFn)

	signFn := func(ctx context.Context, input signInput) (signOutput, error) {
		fmt.Printf("  [sign] reservation=%s\n", input.ReservationID)
		return signOutput{Signature: "sig-" + input.ReservationID}, nil
	}
	mustRegisterActivity(registry, "wallets.Sign", signFn)
	mustRegisterActivity(registry, "activities.Sign", signFn)
	checkBalanceFn := func(ctx context.Context, input checkBalanceInput) (checkBalanceOutput, error) {
		fmt.Printf("  [checkBalance] wallet=%s token=%s\n", input.WalletAddress, input.Token)
		balance := 150.0
		if input.Token == "USDT" {
			balance = 50.0
		}
		return checkBalanceOutput{Balance: balance}, nil
	}
	mustRegisterActivity(registry, "check.Balance", checkBalanceFn)
	mustRegisterActivity(registry, "activities.CheckBalance", checkBalanceFn)
	transferFn := func(ctx context.Context, input transferInput) (transferOutput, error) {
		fmt.Printf("  [transfer] %s → %s amount=%.2f\n", input.From, input.To, input.Amount)
		return transferOutput{TxHash: "0xabc123", Successful: true}, nil
	}
	mustRegisterActivity(registry, "transfer.Send", transferFn)
	mustRegisterActivity(registry, "activities.Transfer", transferFn)
	notifyFn := func(ctx context.Context, input notifyInput) (notifyOutput, error) {
		fmt.Printf("  [notify] channel=%s msg=%s\n", input.Channel, input.Message)
		return notifyOutput{Sent: true}, nil
	}
	mustRegisterActivity(registry, "notify.Send", notifyFn)
	mustRegisterActivity(registry, "activities.Notify", notifyFn)
	paymentProcessFn := func(ctx context.Context, input processPaymentInput) (processPaymentOutput, error) {
		fmt.Printf("  [payment.Process] order=%s amount=%.2f method=%s\n", input.OrderID, input.Amount, input.Method)
		return processPaymentOutput{PaymentID: "pay-" + input.OrderID, Status: "processed"}, nil
	}
	mustRegisterActivity(registry, "payment.Process", paymentProcessFn)
	mustRegisterActivity(registry, "activities.PaymentProcess", paymentProcessFn)
	paymentConfirmFn := func(ctx context.Context, input confirmPaymentInput) (confirmPaymentOutput, error) {
		fmt.Printf("  [payment.Confirm] paymentID=%s\n", input.PaymentID)
		return confirmPaymentOutput{ConfirmedAt: "2026-06-22T12:00:00Z", Receipt: "receipt-" + input.PaymentID}, nil
	}
	mustRegisterActivity(registry, "payment.Confirm", paymentConfirmFn)
	mustRegisterActivity(registry, "activities.PaymentConfirm", paymentConfirmFn)
	longRunningFn := func(ctx context.Context, input longRunningInput) (longRunningOutput, error) {
		dur := input.Duration
		if dur <= 0 {
			dur = 300
		}
		fmt.Printf("  [longRunning] task=%s duration=%ds — started\n", input.TaskName, dur)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		elapsed := 0
		for elapsed < dur {
			select {
			case <-ctx.Done():
				fmt.Printf("  [longRunning] task=%s cancelled after %ds\n", input.TaskName, elapsed)
				return longRunningOutput{}, ctx.Err()
			case <-ticker.C:
				elapsed += 5
				fmt.Printf("  [longRunning] task=%s progress %d/%ds\n", input.TaskName, elapsed, dur)
			}
		}
		fmt.Printf("  [longRunning] task=%s completed\n", input.TaskName)
		return longRunningOutput{TaskName: input.TaskName, Processed: true, Elapsed: dur}, nil
	}
	mustRegisterActivity(registry, "longRunning.Process", longRunningFn)
	mustRegisterActivity(registry, "activities.LongRunning", longRunningFn)

	// demo.Flaky 演示重试：前两次 attempt 故意失败，第三次成功，触发 RetryPolicy
	flakyFn := func(ctx context.Context, input flakyInput) (flakyOutput, error) {
		attempt := wfsdk.GetInfo(ctx).Attempt
		fmt.Printf("  [flaky] order=%s attempt=%d\n", input.OrderID, attempt)
		if attempt < 3 {
			return flakyOutput{}, fmt.Errorf("transient settlement failure for %s (attempt %d)", input.OrderID, attempt)
		}
		return flakyOutput{Settled: true}, nil
	}
	mustRegisterActivity(registry, "demo.Flaky", flakyFn)
	mustRegisterActivity(registry, "activities.Flaky", flakyFn)

	// Typed Update-With-Start demo activity. The public HTTP endpoint waits for
	// this Activity-backed Update handler to return its typed result.
	mustRegisterActivity(registry, "activities.CreateRecord", typedCreateRecordActivity)
	activityWorker, err := worker.New(c, worker.Options{
		Role:                                   worker.RoleActivity,
		Activities:                             registry,
		ActivityTaskQueues:                     []string{"default"},
		Identity:                               "demo-worker-1",
		BuildID:                                "demo@2026-06-18",
		ActivityConcurrency:                    8, // 否则默认 1，长跑的 longRunning.Process 会独占唯一槽位阻塞其他 activity
		ActivityWorkerVersionDefault:           true,
		ActivityWorkerVersionHeartbeatInterval: time.Second,
	})
	if err != nil {
		log.Fatalf("create activity worker: %v", err)
	}
	activityWorker.Start(ctx)
	defer activityWorker.Stop()

	// Wait for workers to be ready
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readyCancel()
	if err := activityWorker.WaitReady(readyCtx); err != nil {
		log.Fatalf("wait activity worker ready: %v", err)
	}

	// ── 2. Register demo workflows ──

	// Simple linear workflow (wlang) — reserve→sign，带 per-call timeout + RetryPolicy
	linearSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func LinearFlow(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
		RetryPolicy: workflow.RetryPolicy{
			MaxAttempts:        3,
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaxInterval:        100 * time.Second,
		},
	}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	signIn := map[string]any{"reservation_id": reserve["reservation_id"]}
	var sign map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Sign, signIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &sign)
	if err != nil {
		return err
	}
	return nil
}
`)
	linearResult, err := sdkgen.GenerateFromSource(linearSrc, sdkgen.Options{
		WorkflowType: "demo-cross-app-flow",
		Name:         "Linear Flow",
		FuncName:     "LinearFlow",
		Filename:     "linear_flow.go",
	})
	if err != nil {
		log.Fatalf("generate linear wlang workflow: %v", err)
	}
	wf1 := linearResult.Workflow

	// Complex workflow with if/foreach (wlang) — foreach 遍历 wallets，if 判断余额阈值
	autoSweepSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func AutoSweep(ctx workflow.Context, input map[string]any) error {
	wallet1, _ := input["wallet_1"].(string)
	wallet2, _ := input["wallet_2"].(string)
	wallets := []string{wallet1, wallet2}
	for _, wallet := range wallets {
		checkIn := map[string]any{
			"wallet_address": wallet,
			"token":          input["token"],
		}
		var balance map[string]any
		err := workflow.ExecuteActivity(ctx, activities.CheckBalance, checkIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &balance)
		if err != nil {
			return err
		}
		bal, _ := balance["balance"].(float64)
		threshold, _ := input["threshold"].(float64)
		if bal >= threshold {
			transferIn := map[string]any{
				"from":   wallet,
				"to":     input["destination"],
				"amount": balance["balance"],
				"token":  input["token"],
			}
			var transferOut map[string]any
			err = workflow.ExecuteActivity(ctx, activities.Transfer, transferIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &transferOut)
			if err != nil {
				return err
			}
			notifyIn := map[string]any{"message": "Transfer completed", "channel": "slack"}
			var notifyOut map[string]any
			err = workflow.ExecuteActivity(ctx, activities.Notify, notifyIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notifyOut)
			if err != nil {
				return err
			}
		} else {
			notifyIn := map[string]any{"message": "Balance too low, skipping", "channel": "log"}
			var notifyOut map[string]any
			err = workflow.ExecuteActivity(ctx, activities.Notify, notifyIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notifyOut)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
`)
	autoSweepResult, err := sdkgen.GenerateFromSource(autoSweepSrc, sdkgen.Options{
		WorkflowType: "demo-auto-sweep",
		Name:         "Auto Sweep (IF + FOREACH)",
		FuncName:     "AutoSweep",
		Filename:     "auto_sweep.go",
	})
	if err != nil {
		log.Fatalf("generate auto-sweep wlang workflow: %v", err)
	}
	wf2 := autoSweepResult.Workflow

	bulkWallets := make([]string, 240)
	for i := range bulkWallets {
		bulkWallets[i] = fmt.Sprintf("%q", fmt.Sprintf("0xBULK%03d", i+1))
	}
	bulkActivitySrc := []byte(fmt.Sprintf(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func BulkActivityLoop(ctx workflow.Context, input map[string]any) error {
	wallets := []string{%s}
	for _, wallet := range wallets {
		var out map[string]any
		if err := workflow.ExecuteActivity(ctx, activities.CheckBalance, map[string]any{"wallet_address": wallet, "token": "ETH"}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
			return err
		}
	}
	return nil
}
`, strings.Join(bulkWallets, ",")))
	bulkActivityResult, err := sdkgen.GenerateFromSource(bulkActivitySrc, sdkgen.Options{
		WorkflowType: "demo-activity-bulk-loop",
		Name:         "Bulk Activity Loop (240 calls)",
		FuncName:     "BulkActivityLoop",
		Filename:     "bulk_activity_loop.go",
	})
	if err != nil {
		log.Fatalf("generate bulk activity workflow: %v", err)
	}
	wfBulkActivity := bulkActivityResult.Workflow

	// wlang workflow (for debugger demo) — uses sdkgen to generate from Go source
	wlangSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func OrderWorkflow(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	isHighPriority, _ := input["is_high_priority"].(bool)
	if isHighPriority {
		notifyIn := map[string]any{"message": "High priority order", "channel": "slack"}
		var notifyResult map[string]any
		err = workflow.ExecuteActivity(ctx, activities.Notify, notifyIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notifyResult)
		if err != nil {
			return err
		}
	}
	signIn := map[string]any{"reservation_id": reserve["reservation_id"]}
	var signResult map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Sign, signIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &signResult)
	if err != nil {
		return err
	}
	return nil
}
`)
	wlangResult, err := sdkgen.GenerateFromSource(wlangSrc, sdkgen.Options{
		WorkflowType: "demo-wlang-order",
		Name:         "Order Processing (wlang)",
		FuncName:     "OrderWorkflow",
		Filename:     "order_workflow.go",
	})
	if err != nil {
		log.Fatalf("generate wlang workflow: %v", err)
	}
	wf3 := wlangResult.Workflow

	// wf3High 复用同一份 wlang 源码，但注册成独立 workflowID：同一 workflowID 不允许
	// 两个并存的 current run，Run 3（normal）与 Run 5（high-priority）必须分属不同 ID。
	wlangHighResult, err := sdkgen.GenerateFromSource(wlangSrc, sdkgen.Options{
		WorkflowType: "demo-wlang-order-high",
		Name:         "Order Processing High Priority (wlang)",
		FuncName:     "OrderWorkflow",
		Filename:     "order_workflow_high.go",
	})
	if err != nil {
		log.Fatalf("generate high-priority wlang workflow: %v", err)
	}
	wf3High := wlangHighResult.Workflow

	// Child workflow (called by parent) — wlang
	childPaymentSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func ChildPayment(ctx workflow.Context, input map[string]any) error {
	processIn := map[string]any{
		"order_id": input["order_id"],
		"amount":   input["amount"],
		"method":   input["method"],
	}
	var process map[string]any
	err := workflow.ExecuteActivity(ctx, activities.PaymentProcess, processIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &process)
	if err != nil {
		return err
	}
	confirmIn := map[string]any{"payment_id": process["payment_id"]}
	var confirm map[string]any
	err = workflow.ExecuteActivity(ctx, activities.PaymentConfirm, confirmIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &confirm)
	if err != nil {
		return err
	}
	return nil
}
`)
	childPaymentResult, err := sdkgen.GenerateFromSource(childPaymentSrc, sdkgen.Options{
		WorkflowType: "demo-child-payment",
		Name:         "Child: Payment Processing",
		FuncName:     "ChildPayment",
		Filename:     "child_payment.go",
	})
	if err != nil {
		log.Fatalf("generate child-payment wlang workflow: %v", err)
	}
	wfChild := childPaymentResult.Workflow

	// Parent workflow with child workflow call (wlang)
	parentSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func OrderWithPayment(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	childInput := map[string]any{
		"order_id": input["order_id"],
		"amount":   input["amount"],
		"method":   input["method"],
	}
	var paymentResult map[string]any
	err = workflow.ExecuteChildWorkflow(ctx, "demo-child-payment", childInput).Get(ctx, &paymentResult)
	if err != nil {
		return err
	}
	signIn := map[string]any{"reservation_id": reserve["reservation_id"]}
	var signResult map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Sign, signIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &signResult)
	if err != nil {
		return err
	}
	return nil
}
`)
	parentResult, err := sdkgen.GenerateFromSource(parentSrc, sdkgen.Options{
		WorkflowType: "demo-parent-with-child",
		Name:         "Order + Child Payment (wlang)",
		FuncName:     "OrderWithPayment",
		Filename:     "order_with_payment.go",
	})
	if err != nil {
		log.Fatalf("generate parent wlang workflow: %v", err)
	}
	wf4 := parentResult.Workflow

	// Long-running standalone workflow (wlang, stays running for cancel/terminate/reset testing)
	longRunningSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func LongRunning(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	longIn := map[string]any{"task_name": "data-processing", "duration": 300}
	var longOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.LongRunning, longIn, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute}).Get(ctx, &longOut)
	if err != nil {
		return err
	}
	signIn := map[string]any{"reservation_id": reserve["reservation_id"]}
	var sign map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Sign, signIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &sign)
	if err != nil {
		return err
	}
	return nil
}
`)
	longRunningResult, err := sdkgen.GenerateFromSource(longRunningSrc, sdkgen.Options{
		WorkflowType: "demo-long-running",
		Name:         "Long Running (5min activity)",
		FuncName:     "LongRunning",
		Filename:     "long_running.go",
	})
	if err != nil {
		log.Fatalf("generate long-running wlang workflow: %v", err)
	}
	wfLongRunning := longRunningResult.Workflow

	// Flaky workflow (wlang): settle activity fails twice then succeeds — 带 per-call RetryPolicy 演示重试
	flakySrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func FlakySettlement(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	settleIn := map[string]any{"order_id": input["order_id"]}
	var settle map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Flaky, settleIn, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: workflow.RetryPolicy{
			MaxAttempts:        5,
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaxInterval:        30 * time.Second,
		},
	}).Get(ctx, &settle)
	if err != nil {
		return err
	}
	return nil
}
`)
	flakyResult, err := sdkgen.GenerateFromSource(flakySrc, sdkgen.Options{
		WorkflowType: "demo-flaky-retry",
		Name:         "Flaky Settlement (retry demo)",
		FuncName:     "FlakySettlement",
		Filename:     "flaky_settlement.go",
	})
	if err != nil {
		log.Fatalf("generate flaky wlang workflow: %v", err)
	}
	wfFlaky := flakyResult.Workflow

	// Long-running child workflow (wlang, called by parent, also stays running)
	longChildSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func LongRunningChild(ctx workflow.Context, input map[string]any) error {
	longIn := map[string]any{
		"task_name": input["task_name"],
		"duration":  input["duration"],
	}
	var longOut map[string]any
	err := workflow.ExecuteActivity(ctx, activities.LongRunning, longIn, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Minute}).Get(ctx, &longOut)
	if err != nil {
		return err
	}
	notifyIn := map[string]any{"message": "Long running child completed", "channel": "slack"}
	var notifyOut map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Notify, notifyIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notifyOut)
	if err != nil {
		return err
	}
	return nil
}
`)
	longChildResult, err := sdkgen.GenerateFromSource(longChildSrc, sdkgen.Options{
		WorkflowType: "demo-long-child",
		Name:         "Child: Long Running Task",
		FuncName:     "LongRunningChild",
		Filename:     "long_child.go",
	})
	if err != nil {
		log.Fatalf("generate long-child wlang workflow: %v", err)
	}
	wfLongChild := longChildResult.Workflow

	// Parent workflow with long-running child (wlang)
	longParentSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func LongParentWithChild(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
	if err != nil {
		return err
	}
	childInput := map[string]any{
		"task_name": "child-batch-job",
		"duration":  300,
	}
	var childResult map[string]any
	err = workflow.ExecuteChildWorkflow(ctx, "demo-long-child", childInput).Get(ctx, &childResult)
	if err != nil {
		return err
	}
	signIn := map[string]any{"reservation_id": reserve["reservation_id"]}
	var signResult map[string]any
	err = workflow.ExecuteActivity(ctx, activities.Sign, signIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &signResult)
	if err != nil {
		return err
	}
	return nil
}
`)
	longParentResult, err := sdkgen.GenerateFromSource(longParentSrc, sdkgen.Options{
		WorkflowType: "demo-long-parent-child",
		Name:         "Long Parent + Child (wlang)",
		FuncName:     "LongParentWithChild",
		Filename:     "long_parent_with_child.go",
	})
	if err != nil {
		log.Fatalf("generate long parent wlang workflow: %v", err)
	}
	wfLongParent := longParentResult.Workflow

	// ── workflow.Go 结构化并发演示 ──

	// Demo A: fan-out 并发——3 条 lane 各查一个钱包余额，并发调度、全部完成后 join。
	// 展示协程级并发：同一 WFT 内并发 schedule 3 个 check.Balance，时间线上并行重叠。
	fanoutSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func FanOutBalances(ctx workflow.Context, input map[string]any) error {
	wg := workflow.NewWaitGroup(ctx)
	var err1, err2, err3 error
	wg.Go(ctx, func(ctx workflow.Context) {
		in := map[string]any{"wallet_address": "0xHOT1", "token": "ETH"}
		var out map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.CheckBalance, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); e != nil {
			err1 = e
			return
		}
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		in := map[string]any{"wallet_address": "0xHOT2", "token": "USDT"}
		var out map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.CheckBalance, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); e != nil {
			err2 = e
			return
		}
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		in := map[string]any{"wallet_address": "0xHOT3", "token": "ETH"}
		var out map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.CheckBalance, in, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); e != nil {
			err3 = e
			return
		}
	})
	wg.Wait(ctx)
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	if err3 != nil {
		return err3
	}
	return nil
}
`)
	fanoutResult, err := sdkgen.GenerateFromSource(fanoutSrc, sdkgen.Options{
		WorkflowType: "demo-go-fanout",
		Name:         "workflow.Go Fan-Out (concurrent balances)",
		FuncName:     "FanOutBalances",
		Filename:     "fanout_balances.go",
	})
	if err != nil {
		log.Fatalf("generate fan-out workflow: %v", err)
	}
	wfFanout := fanoutResult.Workflow

	// Demo B: 多步 lane——每条 lane 内是有依赖的顺序链 reserve→sign，
	// 两条 lane 并行推进。lane 内 sign 必须等本 lane 的 reserve 完成才发起，
	// 兄弟 lane 不被本 lane 的依赖链阻塞。这是 workflow.Go 独有的能力，也正是
	// per-lane 结构化键（/0#a1、/1#a1）保证 exactly-once 的场景。
	multiStepSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func TwoLaneReserveSign(ctx workflow.Context, input map[string]any) error {
	wg := workflow.NewWaitGroup(ctx)
	var errA, errB error
	wg.Go(ctx, func(ctx workflow.Context) {
		var reserve map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.Reserve, map[string]any{"order_id": "order-lane-A"}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve); e != nil {
			errA = e
			return
		}
		var sign map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.Sign, map[string]any{"reservation_id": reserve["reservation_id"]}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &sign); e != nil {
			errA = e
			return
		}
	})
	wg.Go(ctx, func(ctx workflow.Context) {
		var reserve map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.Reserve, map[string]any{"order_id": "order-lane-B"}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve); e != nil {
			errB = e
			return
		}
		var sign map[string]any
		if e := workflow.ExecuteActivity(ctx, activities.Sign, map[string]any{"reservation_id": reserve["reservation_id"]}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &sign); e != nil {
			errB = e
			return
		}
	})
	wg.Wait(ctx)
	if errA != nil {
		return errA
	}
	if errB != nil {
		return errB
	}
	return nil
}
`)
	multiStepResult, err := sdkgen.GenerateFromSource(multiStepSrc, sdkgen.Options{
		WorkflowType: "demo-go-multistep-lane",
		Name:         "workflow.Go Multi-Step Lanes (reserve→sign per lane)",
		FuncName:     "TwoLaneReserveSign",
		Filename:     "two_lane_reserve_sign.go",
	})
	if err != nil {
		log.Fatalf("generate multi-step lane workflow: %v", err)
	}
	wfMultiStep := multiStepResult.Workflow

	// 同一个 Go 循环体会生成多条动态 routine，Trace 依靠 RoutineID 区分实例，
	// 同时保留每轮闭包自己的 wallet 变量和 Activity invocation。
	loopFanoutSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func LoopFanOutBalances(ctx workflow.Context, input map[string]any) error {
	wallet1, _ := input["wallet_1"].(string)
	wallet2, _ := input["wallet_2"].(string)
	wallet3, _ := input["wallet_3"].(string)
	wallets := []string{wallet1, wallet2, wallet3}
	wg := workflow.NewWaitGroup(ctx)
	for _, wallet := range wallets {
		wg.Go(ctx, func(ctx workflow.Context) {
			checkIn := map[string]any{
				"wallet_address": wallet,
				"token":          input["token"],
			}
			var out map[string]any
			if err := workflow.ExecuteActivity(ctx, activities.CheckBalance, checkIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out); err != nil {
				return
			}
		})
	}
	wg.Wait(ctx)
	return nil
}
`)
	loopFanoutResult, err := sdkgen.GenerateFromSource(loopFanoutSrc, sdkgen.Options{
		WorkflowType: "demo-go-loop-fanout",
		Name:         "workflow.Go Loop Fan-Out",
		FuncName:     "LoopFanOutBalances",
		Filename:     "loop_fanout_balances.go",
	})
	if err != nil {
		log.Fatalf("generate loop fan-out workflow: %v", err)
	}
	wfLoopFanout := loopFanoutResult.Workflow

	// Trace debugger suite: sequential child, ContinueAsNew child, and a parent
	// that starts several children before awaiting them.
	traceSequentialSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func TraceSequentialChild(ctx workflow.Context, input map[string]any) error {
	reserveIn := map[string]any{"order_id": input["order_id"]}
	var reserve map[string]any
	if err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve); err != nil {
		return err
	}
	notifyIn := map[string]any{"message": "Sequential child completed", "channel": "trace"}
	var notifyOut map[string]any
	return workflow.ExecuteActivity(ctx, activities.Notify, notifyIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notifyOut)
}
`)
	traceSequentialResult, err := sdkgen.GenerateFromSource(traceSequentialSrc, sdkgen.Options{
		WorkflowType: "demo-trace-sequential-child",
		Name:         "Trace Suite: Sequential Child",
		FuncName:     "TraceSequentialChild",
		Filename:     "trace_sequential_child.go",
	})
	if err != nil {
		log.Fatalf("generate trace sequential child: %v", err)
	}
	wfTraceSequentialChild := traceSequentialResult.Workflow
	tracePaymentResult, err := sdkgen.GenerateFromSource(childPaymentSrc, sdkgen.Options{
		WorkflowType: "demo-trace-payment-child",
		Name:         "Trace Suite: Payment Child",
		FuncName:     "ChildPayment",
		Filename:     "trace_payment_child.go",
	})
	if err != nil {
		log.Fatalf("generate trace payment child: %v", err)
	}
	wfTracePaymentChild := tracePaymentResult.Workflow

	traceCANSrc := []byte(`package flows
import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/activities"
)

func TraceCAN(ctx workflow.Context, input map[string]any) error {
	continued, _ := input["continued"].(bool)
	if continued {
		reserveIn := map[string]any{"order_id": input["order_id"]}
		var reserve map[string]any
		err := workflow.ExecuteActivity(ctx, activities.Reserve, reserveIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
		if err != nil {
			return err
		}
		signIn := map[string]any{"reservation_id": reserve["reservation_id"]}
		var sign map[string]any
		err = workflow.ExecuteActivity(ctx, activities.Sign, signIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &sign)
		if err != nil {
			return err
		}
		return nil
	}
	notifyIn := map[string]any{
		"message": "ContinueAsNew entry segment",
		"channel": "trace",
	}
	var notify map[string]any
	err := workflow.ExecuteActivity(ctx, activities.Notify, notifyIn, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &notify)
	if err != nil {
		return err
	}
	continuedInput := map[string]any{
		"order_id": input["order_id"],
		"continued": true,
	}
	return workflow.ContinueAsNew(ctx, "demo-trace-can-child", continuedInput)
}
`)
	traceCANResult, err := sdkgen.GenerateFromSource(traceCANSrc, sdkgen.Options{
		WorkflowType: "demo-trace-can-child",
		Name:         "Trace Suite: CAN Child",
		FuncName:     "TraceCAN",
		Filename:     "trace_can_child.go",
	})
	if err != nil {
		log.Fatalf("generate trace CAN child workflow: %v", err)
	}
	wfTraceCAN := traceCANResult.Workflow

	traceCANRootSrc := []byte(strings.ReplaceAll(
		string(traceCANSrc),
		"demo-trace-can-child",
		"demo-trace-can-root",
	))
	traceCANRootResult, err := sdkgen.GenerateFromSource(traceCANRootSrc, sdkgen.Options{
		WorkflowType: "demo-trace-can-root",
		Name:         "Trace Suite: Root CAN",
		FuncName:     "TraceCAN",
		Filename:     "trace_can_root.go",
	})
	if err != nil {
		log.Fatalf("generate trace CAN root workflow: %v", err)
	}
	wfTraceCANRoot := traceCANRootResult.Workflow

	traceSuiteParentSrc := []byte(`package flows
import (
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

func TraceChildDebugSuite(ctx workflow.Context, input map[string]any) error {
	paymentInput := map[string]any{
		"order_id": input["order_id"],
		"amount": 88.5,
		"method": "trace-card",
	}
	payment := workflow.ExecuteChildWorkflow(ctx, "demo-trace-payment-child", paymentInput)
	sequentialInput := map[string]any{
		"order_id": input["order_id"],
	}
	sequential := workflow.ExecuteChildWorkflow(ctx, "demo-trace-sequential-child", sequentialInput)
	continuedInput := map[string]any{
		"order_id": input["order_id"],
	}
	continued := workflow.ExecuteChildWorkflow(ctx, "demo-trace-can-child", continuedInput)
	selector := workflow.NewSelector(ctx)
	selector.AddChildFuture(ctx, continued, func(f workflow.ChildWorkflowFuture) {
	})
	var paymentOut map[string]any
	if err := payment.Get(ctx, &paymentOut); err != nil {
		return err
	}
	var sequentialOut map[string]any
	if err := sequential.Get(ctx, &sequentialOut); err != nil {
		return err
	}
	return nil
}
`)
	traceSuiteParentResult, err := sdkgen.GenerateFromSource(traceSuiteParentSrc, sdkgen.Options{
		WorkflowType: "demo-trace-child-debug-suite",
		Name:         "Trace Child Debug Suite",
		FuncName:     "TraceChildDebugSuite",
		Filename:     "trace_child_debug_suite.go",
	})
	if err != nil {
		log.Fatalf("generate trace child debug suite: %v", err)
	}
	wfTraceSuiteParent := traceSuiteParentResult.Workflow
	typedApprovalConfig, err := typedApprovalWorkflowConfig()
	if err != nil {
		log.Fatalf("generate typed approval workflow: %v", err)
	}
	noHandlerConfig, err := noUpdateHandlerWorkflowConfig()
	if err != nil {
		log.Fatalf("generate no-handler workflow: %v", err)
	}

	// Workflow worker：本进程生成的定义随 Worker 启动登记（与生成器 embed 的 workflow.json 走同一条路径）。
	var definitions []registration.Definition
	for _, wf := range []*program.WorkflowConfig{
		wf1, wf2, wf3, wf3High, wfChild, wf4, wfLongRunning, wfFlaky,
		wfLongChild, wfLongParent, wfFanout, wfMultiStep, wfLoopFanout, wfBulkActivity, wfTraceSequentialChild,
		wfTracePaymentChild, wfTraceCAN, wfTraceCANRoot, wfTraceSuiteParent, typedApprovalConfig, noHandlerConfig,
	} {
		definition, err := definitionOf(wf)
		if err != nil {
			log.Fatalf("encode workflow %s: %v", wf.Type, err)
		}
		definitions = append(definitions, definition)
	}
	wfWorker, err := worker.New(c, worker.Options{
		Role:                worker.RoleWorkflow,
		Workflows:           definitions,
		WorkflowConcurrency: 1,
		Identity:            "tenuto-demo-workflow-worker-1",
		BuildID:             "tenuto-demo@2026-06-08",
	})
	if err != nil {
		log.Fatalf("create workflow worker: %v", err)
	}
	wfWorker.Start(ctx)
	defer wfWorker.Stop()
	registerCtx, registerCancel := context.WithTimeout(ctx, 10*time.Second)
	defer registerCancel()
	if err := wfWorker.WaitReady(registerCtx); err != nil {
		log.Fatalf("wait workflow worker ready: %v", err)
	}
	publicHTTPAddr := strings.TrimSpace(os.Getenv("DEMO_HTTP_ADDR"))
	if publicHTTPAddr == "" {
		publicHTTPAddr = "0.0.0.0:7403"
	}
	publicHTTPServer := &http.Server{
		Addr:              publicHTTPAddr,
		Handler:           newPublicCreateRecordHandler(c),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      2 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("demo public HTTP API listening on %s", publicHTTPAddr)
		if err := publicHTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("demo public HTTP API stopped: %v", err)
		}
	}()

	// ── 3. Start demo workflow runs ──

	// Run 1: simple linear
	runID1, err := startRun(ctx, c, wf1.Type, map[string]any{"order_id": "order-1001"})
	if err != nil {
		log.Fatalf("start workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID1, map[string]string{"business_id": "biz-demo", "order_id": "order-1001"})
	log.Printf("linear workflow run started: %s", runID1)

	// Run 2: complex with branches
	runID2, err := startRun(ctx, c, wf2.Type, map[string]any{
		"wallet_1":    "0xAAA111",
		"wallet_2":    "0xBBB222",
		"token":       "ETH",
		"threshold":   100.0,
		"destination": "0xVAULT",
	})

	if err != nil {
		log.Fatalf("start auto-sweep workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID2, map[string]string{"business_id": "biz-demo", "type": "auto-sweep"})
	log.Printf("auto-sweep workflow run started: %s", runID2)
	log.Printf("TRACE_LOOP_ACTIVITY_URL=http://localhost:7401/demo/runs/%s/trace", runID2)

	runBulkActivity, err := startRun(ctx, c, wfBulkActivity.Type, map[string]any{})
	if err != nil {
		log.Fatalf("start bulk activity workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runBulkActivity, map[string]string{"business_id": "biz-demo", "type": "bulk-activity-loop"})
	log.Printf("TRACE_BULK_ACTIVITY_URL=http://localhost:7401/demo/runs/%s/trace", runBulkActivity)

	// Run 3: wlang workflow (debugger demo, priority=normal → if false)
	runID3, err := startRun(ctx, c, wf3.Type, map[string]any{"order_id": "order-2001"})
	if err != nil {
		log.Fatalf("start wlang workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID3, map[string]string{"business_id": "biz-demo", "type": "wlang-order"})
	log.Printf("wlang workflow run started: %s", runID3)

	// Run 4: parent with child workflow
	runID4, err := startRun(ctx, c, wf4.Type, map[string]any{
		"order_id": "order-3001",
		"amount":   250.0,
		"method":   "credit_card",
	})

	if err != nil {
		log.Fatalf("start parent workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID4, map[string]string{"business_id": "biz-demo", "type": "parent-child"})
	log.Printf("parent+child workflow run started: %s", runID4)

	// Run 5: wlang workflow with is_high_priority=true (if true branch) — 用独立 workflowID 避免 current run 冲突
	runID5, err := startRun(ctx, c, wf3High.Type, map[string]any{
		"order_id":         "order-2002",
		"is_high_priority": true,
	})

	if err != nil {
		log.Fatalf("start high-priority workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID5, map[string]string{"business_id": "biz-demo", "type": "wlang-order-high"})
	log.Printf("high-priority wlang workflow run started: %s", runID5)

	// Run 6: long-running standalone (stays running for cancel/terminate/reset testing)
	runID6, err := startRun(ctx, c, "demo-long-running", map[string]any{"order_id": "order-long-1"})
	if err != nil {
		log.Fatalf("start long-running workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID6, map[string]string{"business_id": "biz-demo", "type": "long-running"})
	log.Printf("long-running workflow run started: %s (will stay running ~5min)", runID6)

	// Run: flaky workflow (settle activity fails twice then succeeds — demonstrates retry attempts)
	runFlaky, err := startRun(ctx, c, wfFlaky.Type, map[string]any{"order_id": "order-flaky-1"})
	if err != nil {
		log.Fatalf("start flaky workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runFlaky, map[string]string{"business_id": "biz-demo", "type": "flaky-retry"})
	log.Printf("flaky retry workflow run started: %s", runFlaky)

	// Run 7: parent with long-running child (stays running for cancel/terminate/reset testing)
	runID7, err := startRun(ctx, c, wfLongParent.Type, map[string]any{"order_id": "order-long-2"})
	if err != nil {
		log.Fatalf("start long parent+child workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runID7, map[string]string{"business_id": "biz-demo", "type": "long-parent-child"})
	log.Printf("long parent+child workflow run started: %s (will stay running ~5min)", runID7)

	// Run: workflow.Go fan-out（3 条 lane 并发查余额）
	runFanout, err := startRun(ctx, c, wfFanout.Type, map[string]any{"order_id": "order-fanout-1"})
	if err != nil {
		log.Fatalf("start fan-out workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runFanout, map[string]string{"business_id": "biz-demo", "type": "go-fanout"})
	log.Printf("workflow.Go fan-out run started: %s", runFanout)

	// Run: workflow.Go 多步 lane（每条 lane 内 reserve→sign 顺序链，兄弟 lane 并行）
	runMultiStep, err := startRun(ctx, c, wfMultiStep.Type, map[string]any{"order_id": "order-multistep-1"})
	if err != nil {
		log.Fatalf("start multi-step lane workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runMultiStep, map[string]string{"business_id": "biz-demo", "type": "go-multistep"})
	log.Printf("workflow.Go multi-step lane run started: %s", runMultiStep)

	// Run: for 循环每轮启动一条 workflow.Go lane。
	runLoopFanout, err := startRun(ctx, c, wfLoopFanout.Type, map[string]any{
		"wallet_1": "0xLOOP1",
		"wallet_2": "0xLOOP2",
		"wallet_3": "0xLOOP3",
		"token":    "ETH",
	})

	if err != nil {
		log.Fatalf("start loop fan-out workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runLoopFanout, map[string]string{"business_id": "biz-demo", "type": "go-loop-fanout"})
	log.Printf("TRACE_LOOP_GO_URL=http://localhost:7401/demo/runs/%s/trace", runLoopFanout)

	// Run: complete Trace child suite (payment child + sequential child + CAN child).
	runTraceSuite, err := startRun(ctx, c, wfTraceSuiteParent.Type, map[string]any{"order_id": "order-trace-suite-1"})
	if err != nil {
		log.Fatalf("start trace child debug suite: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runTraceSuite, map[string]string{"business_id": "biz-demo", "type": "trace-child-debug-suite"})
	log.Printf("TRACE_TEST_URL=http://localhost:7401/demo/runs/%s/trace", runTraceSuite)

	// Run: root ContinueAsNew path, useful for testing Segment navigation without a Parent boundary.
	runTraceCANRoot, err := startRun(ctx, c, wfTraceCANRoot.Type, map[string]any{"order_id": "order-trace-can-root-1"})
	if err != nil {
		log.Fatalf("start trace CAN root workflow: %v", err)
	}
	_ = c.UpsertSearchAttributes(ctx, "", runTraceCANRoot, map[string]string{"business_id": "biz-demo", "type": "trace-can-root"})
	log.Printf("TRACE_CAN_ROOT_URL=http://localhost:7401/demo/runs/%s/trace", runTraceCANRoot)

	// ── Create demo schedules ──
	_, _ = c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		WorkflowType:   "demo-cross-app-flow",
		Description:    "Automated reserve-and-sign every 5 minutes",
		CronExpression: "*/5 * * * *",
		TimeZone:       "UTC",
		Args:           []any{map[string]any{"order_id": "scheduled-001", "user_id": "system"}},
		OverlapPolicy:  client.ScheduleOverlapPolicySkip,
	})

	_, _ = c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		WorkflowType:   "demo-auto-sweep",
		Description:    "Hourly wallet sweep across all accounts",
		CronExpression: "0 * * * *",
		TimeZone:       "America/New_York",
		Args:           []any{map[string]any{"wallet_1": "0xSCHEDULED1", "wallet_2": "0xSCHEDULED2", "token": "ETH", "threshold": 100.0, "destination": "0xVAULT"}},
		OverlapPolicy:  client.ScheduleOverlapPolicySkip,
		Paused:         true,
	})

	_, _ = c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		WorkflowType:   "demo-wlang-order",
		Description:    "Daily order processing via wlang",
		CronExpression: "0 9 * * 1-5",
		TimeZone:       "Asia/Shanghai",
		Args:           []any{map[string]any{"order_id": "daily-001", "user_id": "cron-user"}},
		OverlapPolicy:  client.ScheduleOverlapPolicyAllowAll,
	})

	// Wait for runs to complete
	go func() {
		for _, rid := range []string{runID1, runID2, runID3, runID4, runID5, runFanout, runMultiStep, runLoopFanout, runBulkActivity, runTraceSuite} {
			if err := c.GetWorkflow(ctx, "", rid).Get(ctx, nil); err != nil {
				log.Printf("run %s ended: %v", rid, err)
			} else {
				log.Printf("run %s completed", rid)
			}
		}
	}()

	// ── 4. Block until Ctrl+C ──
	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = publicHTTPServer.Shutdown(shutdownCtx)
	log.Println("stopped")
}

func waitForServerReady(ctx context.Context, serverURL string, timeout time.Duration) error {
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(readyCtx, http.MethodGet, serverURL+"/health/ready", nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-readyCtx.Done():
			return readyCtx.Err()
		case <-ticker.C:
		}
	}
}

// startRun 启动一个 Run 并返回它的 Run ID。
func startRun(ctx context.Context, c client.Client, workflowType string, input map[string]any) (string, error) {
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{}, workflowType, input)
	if err != nil {
		return "", err
	}
	return run.GetRunID(), nil
}

// definitionOf 把运行时生成的配置包装成 Worker 启动时登记的定义。
func definitionOf(wf *program.WorkflowConfig) (registration.Definition, error) {
	data, err := json.Marshal(wf)
	if err != nil {
		return registration.Definition{}, err
	}
	return registration.Definition{FS: fstest.MapFS{registration.ArtifactFile: &fstest.MapFile{Data: data}}}, nil
}
