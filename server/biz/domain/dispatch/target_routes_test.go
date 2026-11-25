package dispatch

import (
	"errors"
	"strings"
	"testing"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/program/programtest"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock/clocktest"
)

func paymentsWorkflow(activities ...programpkg.ActivityRef) *programpkg.WorkflowConfig {
	refs := make(map[string]programpkg.ActivityRef, len(activities))
	for _, ref := range activities {
		refs[ref.Symbol] = ref
	}
	return &programpkg.WorkflowConfig{Type: "payments", Program: programtest.Noop(), Activities: refs}
}

func chargeActivity() programpkg.ActivityRef {
	return programpkg.ActivityRef{Symbol: "charge", Type: "payments.charge", TaskQueue: "payments"}
}

func reserveActivity() programpkg.ActivityRef {
	return programpkg.ActivityRef{Symbol: "reserve", Type: "inventory.reserve", TaskQueue: "inventory"}
}

// TargetWorkerConflict 判定候选内容的静态 Activity 引用能否由 Run 固定的目标 Worker 承接；离线的目标不算冲突。
func TestTargetWorkerConflict(t *testing.T) {
	payWorker := protocol.ActivityWorkerVersion{
		Namespace: "default", Identity: "pay-worker", BuildID: "b1",
		TaskQueues: []string{"payments"}, ActivityTypes: []string{"payments.charge"},
	}
	catalog := NewActivityRouteCatalog(clocktest.NewFake(time.Now()))
	if err := catalog.RegisterActivityWorkerVersion(payWorker); err != nil {
		t.Fatal(err)
	}
	active := []protocol.ActivityWorkerVersion{payWorker}
	for _, test := range []struct {
		name      string
		catalog   *ActivityRouteCatalog
		namespace string
		target    string
		workflow  *programpkg.WorkflowConfig
		active    []protocol.ActivityWorkerVersion
		wantErr   string
	}{
		{
			name: "Happy Path: the target covers every activity", catalog: catalog, namespace: "default", target: "pay-worker",
			workflow: paymentsWorkflow(chargeActivity()), active: active,
		},
		{
			name: "Error Case: the target cannot run a new activity", catalog: catalog, namespace: "default", target: "pay-worker",
			workflow: paymentsWorkflow(chargeActivity(), reserveActivity()), active: active,
			wantErr: `workflow content v2 conflicts with run target "pay-worker": target worker "pay-worker" cannot handle activity "inventory.reserve"`,
		},
		{
			name: "Edge Case: a run without a target never conflicts", catalog: catalog, namespace: "default", target: "",
			workflow: paymentsWorkflow(chargeActivity(), reserveActivity()), active: active,
		},
		{
			name: "Edge Case: an offline target waits instead of conflicting", catalog: catalog, namespace: "default", target: "pay-worker",
			workflow: paymentsWorkflow(chargeActivity(), reserveActivity()), active: nil,
		},
		{
			name: "Edge Case: a target active only in another namespace is offline here", catalog: catalog, namespace: "tenant-b", target: "pay-worker",
			workflow: paymentsWorkflow(chargeActivity(), reserveActivity()), active: active,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.catalog.TargetWorkerConflict(test.namespace, test.target, "v2", test.workflow, test.active)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("TargetWorkerConflict() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("TargetWorkerConflict() = %v, want %q", err, test.wantErr)
			}
		})
	}
}

// ResolveTargetedActivityRoute 按目标 Worker 最近心跳的那份能承接引用的登记选路，过期的登记同样可用。
func TestResolveTargetedActivityRoute(t *testing.T) {
	older := time.Date(2026, time.November, 11, 8, 0, 0, 0, time.UTC)
	newer := older.Add(time.Minute)
	registration := func(build string, queues []string, types []string, heartbeat time.Time) protocol.ActivityWorkerVersion {
		return protocol.ActivityWorkerVersion{
			Namespace: "default", Identity: "pay-worker", BuildID: build, TaskQueues: queues, ActivityTypes: types,
			LastHeartbeatAt: heartbeat, ExpiresAt: heartbeat.Add(time.Second),
		}
	}
	ref := ActivityRouteRef{Namespace: "default", TargetWorkerIdentity: "pay-worker", ActivityID: "charge", Type: "payments.charge"}
	for _, test := range []struct {
		name          string
		registrations []protocol.ActivityWorkerVersion
		ref           ActivityRouteRef
		want          ActivityRoute
		wantErr       error
	}{
		{
			name: "Happy Path: the most recent heartbeat wins",
			registrations: []protocol.ActivityWorkerVersion{
				registration("b1", []string{"payments"}, []string{"payments.charge"}, older),
				registration("b2", []string{"payments-v2"}, []string{"payments.charge"}, newer),
			},
			ref:  ref,
			want: ActivityRoute{Namespace: "default", Identity: "pay-worker", Type: "payments.charge", TaskQueue: "payments-v2", BuildID: "b2"},
		},
		{
			name: "Happy Path: a newer registration that cannot run the activity is skipped",
			registrations: []protocol.ActivityWorkerVersion{
				registration("b1", []string{"payments"}, []string{"payments.charge"}, older),
				registration("b2", []string{"payments"}, []string{"payments.refund"}, newer),
			},
			ref:  ref,
			want: ActivityRoute{Namespace: "default", Identity: "pay-worker", Type: "payments.charge", TaskQueue: "payments", BuildID: "b1"},
		},
		{
			name: "Happy Path: the requested queue narrows the registrations",
			registrations: []protocol.ActivityWorkerVersion{
				registration("b1", []string{"payments"}, []string{"payments.charge"}, older),
				registration("b2", []string{"payments-v2"}, []string{"payments.charge"}, newer),
			},
			ref:  ActivityRouteRef{Namespace: "default", TargetWorkerIdentity: "pay-worker", ActivityID: "charge", Type: "payments.charge", TaskQueue: "payments"},
			want: ActivityRoute{Namespace: "default", Identity: "pay-worker", Type: "payments.charge", TaskQueue: "payments", BuildID: "b1"},
		},
		{
			name: "Edge Case: without a usable registration the task goes to the requested queue for the target",
			registrations: []protocol.ActivityWorkerVersion{
				registration("b1", []string{"payments"}, []string{"payments.refund"}, older),
			},
			ref:  ref,
			want: ActivityRoute{Namespace: "default", Identity: "pay-worker", Type: "payments.charge", TaskQueue: "default"},
		},
		{
			name:          "Error Case: a declared contract needs a registration to compare with",
			registrations: nil,
			ref:           ActivityRouteRef{Namespace: "default", TargetWorkerIdentity: "pay-worker", ActivityID: "charge", Type: "payments.charge", Version: "v2"},
			wantErr:       ErrActivityRouteUnavailable,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolveTargetedActivityRoute(test.registrations, test.ref)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("ResolveTargetedActivityRoute() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got.Contracts, got.Weight, got.Default, got.ExpiresAt = nil, 0, false, time.Time{}
			if got.Namespace != test.want.Namespace || got.Identity != test.want.Identity || got.Type != test.want.Type ||
				got.TaskQueue != test.want.TaskQueue || got.BuildID != test.want.BuildID {
				t.Fatalf("ResolveTargetedActivityRoute() = %+v, want %+v", got, test.want)
			}
		})
	}
}

// CheckDeclaredActivityContract 只在 Workflow 为该类型声明了契约时对照 Worker 的上报。
func TestCheckDeclaredActivityContract(t *testing.T) {
	declaring := &programpkg.WorkflowConfig{
		Type: "contract",
		Program: programpkg.WLangProgram{
			Schema: programpkg.WLangProgramSchemaV2, Lang: programpkg.WLangLanguageV1,
			Program: []any{map[string]any{"return": nil}}, HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
		},
		Activities: map[string]programpkg.ActivityRef{
			"charge": {Type: "payments.charge", TaskQueue: "payments", Version: "v2"},
		},
	}
	matching := ActivityRoute{Type: "payments.charge", Contracts: []protocol.ActivityContract{{Type: "payments.charge", Version: "v2"}}}
	stale := ActivityRoute{Type: "payments.charge", Contracts: []protocol.ActivityContract{{Type: "payments.charge", Version: "v1"}}}
	for _, test := range []struct {
		name         string
		workflow     *programpkg.WorkflowConfig
		route        ActivityRoute
		activityType string
		wantErr      bool
	}{
		{name: "Happy Path: the reported contract matches", workflow: declaring, route: matching, activityType: "payments.charge"},
		{name: "Error Case: the reported contract differs", workflow: declaring, route: stale, activityType: "payments.charge", wantErr: true},
		{name: "Error Case: the worker reported no contract", workflow: declaring, route: ActivityRoute{Type: "payments.charge"}, activityType: "payments.charge", wantErr: true},
		{name: "Edge Case: an undeclared type is not compared", workflow: declaring, route: stale, activityType: "payments.refund"},
		{name: "Edge Case: a workflow without declarations is not compared", workflow: paymentsWorkflow(chargeActivity()), route: stale, activityType: "payments.charge"},
		{name: "Edge Case: no workflow", workflow: nil, route: stale, activityType: "payments.charge"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := CheckDeclaredActivityContract(test.workflow, test.route, test.activityType)
			if (err != nil) != test.wantErr {
				t.Fatalf("CheckDeclaredActivityContract() = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
