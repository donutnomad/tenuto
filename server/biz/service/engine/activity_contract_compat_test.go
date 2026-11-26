package engine

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	historypkg "github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/usecase/activitytasks"
)

// Worker 一侧的 payments.charge 入参与出参，Go 类型与 Workflow 侧的描述符各自独立。
type compatChargeIn struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

type compatChargeInAdded struct {
	OrderID   string `json:"order_id"`
	Amount    int64  `json:"amount"`
	Warehouse string `json:"warehouse"`
}

type compatChargeInRetyped struct {
	OrderID string `json:"order_id"`
	Amount  string `json:"amount"`
}

type compatReceipt struct {
	Receipt string `json:"receipt"`
}

type compatReceiptAdded struct {
	Receipt string `json:"receipt"`
	Fee     int64  `json:"fee"`
}

// compatWorkflow 用 wire 指纹与类型描述符声明 payments.charge 的入参 {order_id, amount} 与出参 {receipt}（readsFee 时出参多一个 fee）。
func compatWorkflow(id string, readsFee bool) *program.WorkflowConfig {
	input := programtest.Record("example.com/orders", "orders.ChargeInput",
		programtest.Field{JSON: "order_id", Type: "string"}, programtest.Field{JSON: "amount", Type: "int64"})
	receiptFields := []programtest.Field{{JSON: "receipt", Type: "string"}}
	if readsFee {
		receiptFields = append(receiptFields, programtest.Field{JSON: "fee", Type: "int64"})
	}
	output := programtest.Record("example.com/orders", "orders.Receipt", receiptFields...)
	types := map[string]program.WLangTypeDescriptor{input.ID: input, output.ID: output}
	// 与生成器一致：ActivityRef 写 wire 指纹，结构在 Program.Types 里。
	inputFingerprint, err := program.WLangWireFingerprint(input.ID, types)
	if err != nil {
		panic(err)
	}
	outputFingerprint, err := program.WLangWireFingerprint(output.ID, types)
	if err != nil {
		panic(err)
	}
	wf := contractWorkflowConfig(id, map[string]program.ActivityRef{
		"charge": {Type: "payments.charge", TaskQueue: "payments", Input: inputFingerprint, Output: outputFingerprint},
	})
	wf.Program.Types = types
	return wf
}

// startCompatRun 启动一个 pinned Run，Program 是 compatWorkflow，并让它调度 payments.charge 到 payments 队列。
func startCompatRun(h *buildRoutingHarness, id string, readsFee bool) string {
	h.t.Helper()
	ref, err := h.server.Definitions().RegisterWorkflowInNamespace(protocol.DefaultNamespace, compatWorkflow(id, readsFee))
	if err != nil {
		h.t.Fatal(err)
	}
	run := &execution.Run{
		ID: "run-" + id, Namespace: protocol.DefaultNamespace, WorkflowID: ref.WorkflowType, WorkflowType: ref.WorkflowType,
		WorkflowConfigVersion: ref.Version, Policy: execution.RunPolicyPinned, Status: historypkg.StatusRunning, CreatedAt: h.clk.Now(),
	}
	createStartedRunForTest(h.t, h.mgr, run)
	wftID := appendOpenWorkflowTaskForTest(h.t, h.mgr, run.ID)
	cmd := historypkg.Command{Type: historypkg.CmdScheduleActivity, ActivityID: "charge", ActivityType: "payments.charge", TaskQueue: "payments", StartToCloseTimeout: time.Minute}
	if err := h.server.HandleWorkflowTaskCompleted(h.server.WorkflowTaskContext(context.Background(), run.ID, wftID), run.ID, wftID, []historypkg.Command{cmd}, ""); err != nil {
		h.t.Fatal(err)
	}
	return run.ID
}

func compatContract[I, O any](t *testing.T) activity.Contract {
	t.Helper()
	contract, err := activity.ContractOf[I, O]("payments.charge")
	if err != nil {
		t.Fatal(err)
	}
	return contract
}

// 契约在领取时按数据流向判兼容：Worker 的入参新增字段、Workflow 读的出参多出 Worker 不返回的字段时，指纹不同但可以执行；
// Worker 改了入参字段类型、或返回 Workflow 不认识的出参字段时拒绝领取。
func TestActivityClaimAcceptsDirectionallyCompatibleContract(t *testing.T) {
	// 结构相同时 Go 类型与描述符得到同一个指纹，走指纹相等。
	same := compatContract[compatChargeIn, compatReceipt](t)
	declared := dispatch.DeclaredActivityContract(compatWorkflow("compat-same", false), "payments.charge")
	if declared == nil || declared.InputWire == nil || declared.OutputWire == nil {
		t.Fatalf("declared contract without wire structures: %+v", declared)
	}
	if declared.Input != same.Input || declared.Output != same.Output {
		t.Fatalf("declared %s/%s, worker %s/%s: the descriptor and the Go type must produce the same fingerprint", declared.Input, declared.Output, same.Input, same.Output)
	}

	for _, test := range []struct {
		name     string
		contract activity.Contract
		readsFee bool
		execute  bool
	}{
		{name: "worker input adds a field", contract: compatContract[compatChargeInAdded, compatReceipt](t), execute: true},
		{name: "workflow reads an output field the worker does not return", contract: compatContract[compatChargeIn, compatReceipt](t), readsFee: true, execute: true},
		{name: "worker input changes a field type", contract: compatContract[compatChargeInRetyped, compatReceipt](t)},
		{name: "worker output returns a field the workflow does not read", contract: compatContract[compatChargeIn, compatReceiptAdded](t)},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newBuildRoutingHarness(t)
			h.register("pay", "pay@new", "payments", test.contract)
			startCompatRun(h, "compat", test.readsFee)
			task, ok := h.poll("pay", "pay@new", "payments", 2*time.Second)
			if !ok {
				t.Fatal("the queued task was not offered to the worker")
			}
			result := h.begin(task)
			if test.execute && !result.Execute {
				t.Fatalf("compatible worker refused: %+v", result)
			}
			if !test.execute && (result.Execute || result.Reason != activitytasks.ReasonWorkerContractMismatch) {
				t.Fatalf("begin = %+v, want refused with %s", result, activitytasks.ReasonWorkerContractMismatch)
			}
		})
	}
}
