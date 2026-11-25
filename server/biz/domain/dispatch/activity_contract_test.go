package dispatch

import (
	"strings"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
)

func reportedWorker(contracts ...protocol.ActivityContract) protocol.ActivityWorkerVersion {
	worker := protocol.ActivityWorkerVersion{
		Identity:      "pay-worker-1",
		BuildID:       "pay@v1",
		TaskQueues:    []string{"payments"},
		ActivityTypes: []string{"payments.charge"},
	}
	worker.Contracts = contracts
	return worker
}

func resolveChargeRoute(t *testing.T, worker protocol.ActivityWorkerVersion) ActivityRoute {
	t.Helper()
	catalog := NewActivityRouteCatalog(clocktest.NewFake(time.Now()))
	if err := catalog.RegisterActivityWorkerVersion(worker); err != nil {
		t.Fatal(err)
	}
	route, err := catalog.ResolveActivityRoute(ActivityRouteRef{ActivityID: "charge", Type: "payments.charge"})
	if err != nil {
		t.Fatal(err)
	}
	return route
}

func TestCheckActivityContractMatchesReportedWorker(t *testing.T) {
	route := resolveChargeRoute(t, reportedWorker(protocol.ActivityContract{
		Type: "payments.charge", Input: "ChargeInput.v2", Output: "Receipt.v2",
	}))
	report := CheckActivityContract(protocol.ActivityContract{
		Type: "payments.charge", Input: "ChargeInput.v2", Output: "Receipt.v2",
	}, route)

	if !report.Compatible() {
		t.Fatalf("expected compatible report, got %+v", report)
	}
}

// Worker 没有为该类型上报契约时，Workflow 声明的每个维度都不匹配。
func TestCheckActivityContractRejectsWorkerWithoutContract(t *testing.T) {
	route := resolveChargeRoute(t, reportedWorker())
	report := CheckActivityContract(protocol.ActivityContract{Type: "payments.charge", Input: "ChargeInput.v2"}, route)

	if report.Compatible() {
		t.Fatalf("a worker without a contract must not match a declared one: %+v", report)
	}
	if err := report.MismatchError(); !strings.Contains(err.Error(), `input expected "ChargeInput.v2" actual ""`) {
		t.Fatalf("unexpected mismatch error: %v", err)
	}
}

// Workflow 没有声明任何维度时不对照。
func TestCheckActivityContractSkipsUndeclaredWorkflow(t *testing.T) {
	route := resolveChargeRoute(t, reportedWorker())
	report := CheckActivityContract(protocol.ActivityContract{Type: "payments.charge"}, route)
	if !report.Compatible() {
		t.Fatalf("an undeclared contract must not be compared: %+v", report)
	}
}

func TestCheckActivityContractReportsMismatch(t *testing.T) {
	route := resolveChargeRoute(t, reportedWorker(protocol.ActivityContract{
		Type: "payments.charge", Input: "ChargeInput.v1", Output: "Receipt.v1",
	}))
	report := CheckActivityContract(protocol.ActivityContract{
		Type: "payments.charge", Input: "ChargeInput.v2",
	}, route)

	if report.Compatible() {
		t.Fatalf("expected incompatible report, got %+v", report)
	}
	err := report.MismatchError()
	if err == nil {
		t.Fatal("expected a mismatch error")
	}
	for _, want := range []string{
		`"payments.charge"`,
		`"pay-worker-1"`,
		`input expected "ChargeInput.v2" actual "ChargeInput.v1"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
	if strings.Contains(err.Error(), "output") {
		t.Fatalf("undeclared dimension must not be reported: %v", err)
	}
}

func TestDeclaredActivityContractsCollectsAllSymbols(t *testing.T) {
	program := programpkg.WLangProgram{
		Schema:           programpkg.WLangProgramSchemaV2,
		Lang:             programpkg.WLangLanguageV1,
		Program:          []any{map[string]any{"return": nil}},
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
	}
	wf := &programpkg.WorkflowConfig{
		Type:    "contract-branches",
		Program: program,
		Activities: map[string]programpkg.ActivityRef{
			// 未选中的 GetVersion 分支同样参与声明。
			"charge.v2": {Type: "payments.charge", TaskQueue: "payments", Input: "ChargeInput.v2", Output: "Receipt.v2"},
			"reserve":   {Type: "inventory.reserve", TaskQueue: "inventory", Input: "ReserveInput"},
			"refund":    {Type: "payments.refund", TaskQueue: "payments", Output: "RefundOutput"},
		},
	}

	declared := DeclaredActivityContracts(wf)
	for activityType, want := range map[string]protocol.ActivityContract{
		"payments.charge":   {Type: "payments.charge", Input: "ChargeInput.v2", Output: "Receipt.v2"},
		"inventory.reserve": {Type: "inventory.reserve", Input: "ReserveInput"},
		"payments.refund":   {Type: "payments.refund", Output: "RefundOutput"},
	} {
		contract, ok := declared[activityType]
		if !ok {
			t.Fatalf("missing declared contract for %s: %+v", activityType, declared)
		}
		if contract != want {
			t.Fatalf("%s contract=%+v want %+v", activityType, contract, want)
		}
	}
}

// 未声明契约的引用不进入索引，调度期因此完全不产生对照。
func TestDeclaredActivityContractsSkipsUndeclaredRefs(t *testing.T) {
	declared := DeclaredActivityContracts(&programpkg.WorkflowConfig{
		Type:    "plain",
		Program: programtest.Noop(),
		Activities: map[string]programpkg.ActivityRef{
			"charge": {Symbol: "charge", Type: "payments.charge"},
		},
	})
	if len(declared) != 0 {
		t.Fatalf("unexpected declarations: %+v", declared)
	}
}

// 领取时按领取 Worker 自己这个 build 的上报对照声明；同一 Identity 的其他 build 的上报不算数。
func TestCheckClaimingWorkerContract(t *testing.T) {
	declaring := func(input string) *programpkg.WorkflowConfig {
		return &programpkg.WorkflowConfig{Type: "claim", Program: programtest.Noop(), Activities: map[string]programpkg.ActivityRef{
			"charge": {Type: "payments.charge", TaskQueue: "payments", Input: input},
		}}
	}
	v1 := reportedWorker(protocol.ActivityContract{Type: "payments.charge", Input: "ChargeInput.v1"})
	v2 := reportedWorker(protocol.ActivityContract{Type: "payments.charge", Input: "ChargeInput.v2"})
	v2.BuildID = "pay@v2"
	for _, test := range []struct {
		name          string
		workflow      *programpkg.WorkflowConfig
		registrations []protocol.ActivityWorkerVersion
		build         string
		compatible    bool
	}{
		{name: "matching build", workflow: declaring("ChargeInput.v1"), registrations: []protocol.ActivityWorkerVersion{v1, v2}, build: "pay@v1", compatible: true},
		{name: "other build of the same worker", workflow: declaring("ChargeInput.v1"), registrations: []protocol.ActivityWorkerVersion{v1, v2}, build: "pay@v2"},
		{name: "unregistered build", workflow: declaring("ChargeInput.v1"), registrations: []protocol.ActivityWorkerVersion{v1}, build: "pay@v3"},
		{name: "no declaration", workflow: declaring(""), build: "pay@v3", compatible: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := CheckClaimingWorkerContract(test.workflow, test.registrations, protocol.DefaultNamespace, "pay-worker-1", test.build, "payments", "payments.charge")
			if (err == nil) != test.compatible {
				t.Fatalf("CheckClaimingWorkerContract = %v, want compatible=%v", err, test.compatible)
			}
		})
	}
}
