package activity

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type chargeInput struct {
	Amount int `json:"amount"`
}

type chargeOutput struct {
	Receipt string `json:"receipt"`
}

func charge(context.Context, chargeInput) (chargeOutput, error) {
	return chargeOutput{Receipt: "ok"}, nil
}

// Error Case：同一个 activity type 第二次注册返回带 type 的错误，首次注册的实现保持生效。
func TestRegistryRejectsDuplicateType(t *testing.T) {
	registry := NewRegistry()
	first := func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"impl": "first"}, nil
	}
	second := func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"impl": "second"}, nil
	}
	if err := registry.Register("payments.charge", first); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	err := registry.Register("payments.charge", second)
	if err == nil || !strings.Contains(err.Error(), `"payments.charge"`) {
		t.Fatalf("duplicate registration error = %v, want one naming payments.charge", err)
	}
	if err := RegisterActivity(registry, "payments.charge", charge); err == nil || !strings.Contains(err.Error(), `"payments.charge"`) {
		t.Fatalf("typed duplicate registration error = %v, want one naming payments.charge", err)
	}

	fn, err := registry.Get("payments.charge")
	if err != nil {
		t.Fatal(err)
	}
	out, _ := fn(context.Background(), nil)
	if out["impl"] != "first" {
		t.Fatalf("registered implementation = %v, want first", out["impl"])
	}
}

// 每次登记都带契约：强类型登记的指纹来自 I、O，map 登记的指纹来自 map[string]any。
func TestRegistryReportsAContractForEveryActivity(t *testing.T) {
	registry := NewRegistry()
	if err := RegisterActivity(registry, "payments.charge", charge); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("audit.log", func(context.Context, map[string]any) (map[string]any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	typed, err := ContractOf[chargeInput, chargeOutput]("payments.charge")
	if err != nil {
		t.Fatal(err)
	}
	untyped, err := ContractOf[map[string]any, map[string]any]("audit.log")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := registry.Contracts(), []Contract{untyped, typed}; !reflect.DeepEqual(got, want) {
		t.Fatalf("contracts = %+v, want %+v", got, want)
	}
	if got, want := registry.Types(), []string{"audit.log", "payments.charge"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	for _, contract := range registry.Contracts() {
		if contract.Input == "" || contract.Output == "" {
			t.Fatalf("contract %+v has an empty fingerprint", contract)
		}
	}
}

// 第 7 项：标量、切片、指针的输入输出在登记时报错，错误写出 activity type、哪一侧、Go 类型和改法。
func TestRegisterActivityRejectsNonObjectIO(t *testing.T) {
	registry := NewRegistry()
	cases := map[string]struct {
		err  error
		want string
	}{
		"scalar input": {RegisterActivity(registry, "text.length",
			func(context.Context, string) (chargeOutput, error) { return chargeOutput{}, nil }), "input type string"},
		"scalar output": {RegisterActivity(registry, "text.length",
			func(context.Context, chargeInput) (int, error) { return 0, nil }), "output type int"},
		"slice input": {RegisterActivity(registry, "text.length",
			func(context.Context, []chargeInput) (chargeOutput, error) { return chargeOutput{}, nil }), "input type []activity.chargeInput"},
		"pointer output": {RegisterActivity(registry, "text.length",
			func(context.Context, chargeInput) (*chargeOutput, error) { return nil, nil }), "output type *activity.chargeOutput"},
		"typed map input": {RegisterActivity(registry, "text.length",
			func(context.Context, map[string]string) (chargeOutput, error) { return chargeOutput{}, nil }), "input type map[string]string"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if tc.err == nil {
				t.Fatal("registration accepted a non-object signature")
			}
			for _, want := range []string{tc.want, `"text.length"`, "struct or map[string]any"} {
				if !strings.Contains(tc.err.Error(), want) {
					t.Fatalf("error %q does not contain %q", tc.err.Error(), want)
				}
			}
		})
	}
	if len(registry.Types()) != 0 {
		t.Fatalf("rejected activities were registered: %v", registry.Types())
	}
}

// TestRegistryRecordsQueues 锁住队列记录：RegisterActivityOnQueue 带队列，RegisterActivity 与 Register 不带。
func TestRegistryRecordsQueues(t *testing.T) {
	type in struct {
		ID string `json:"id"`
	}
	type out struct {
		OK bool `json:"ok"`
	}
	fn := func(context.Context, in) (out, error) { return out{OK: true}, nil }
	registry := NewRegistry()
	if err := RegisterActivityOnQueue(registry, "payments", "payments.charge", fn); err != nil {
		t.Fatal(err)
	}
	if err := RegisterActivityOnQueue(registry, "payments", "payments.refund", fn); err != nil {
		t.Fatal(err)
	}
	if err := RegisterActivityOnQueue(registry, "orders", "orders.notify", fn); err != nil {
		t.Fatal(err)
	}
	if err := RegisterActivity(registry, "audit.log", fn); err != nil {
		t.Fatal(err)
	}
	if got := registry.Queues(); !reflect.DeepEqual(got, []string{"orders", "payments"}) {
		t.Fatalf("Queues = %v", got)
	}
	if got := registry.Unqueued(); !reflect.DeepEqual(got, []string{"audit.log"}) {
		t.Fatalf("Unqueued = %v", got)
	}
	if got := registry.QueueOf("payments.refund"); got != "payments" {
		t.Fatalf("QueueOf(payments.refund) = %q", got)
	}
	if got := registry.QueueOf("audit.log"); got != "" {
		t.Fatalf("QueueOf(audit.log) = %q, want empty", got)
	}
}
