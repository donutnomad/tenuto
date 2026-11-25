package sdkgen

import (
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/wlang/go2wlang"
)

func TestGenerateFromSourceBuildsProgramAndUserView(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type Input struct {
	VIP bool
	Kind string
}

func OrderWorkflow(ctx workflow.Context, input Input) error {
	var reserve map[string]any
	if input.VIP {
		err := workflow.ExecuteActivity(ctx, activities.NotifyVIP, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
		if err != nil {
			return err
		}
	} else {
		err := workflow.ExecuteActivity(ctx, activities.NotifyNormal, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
		if err != nil {
			return err
		}
	}
	switch input.Kind {
	case "card":
		err := workflow.ExecuteActivity(ctx, activities.ChargeCard, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
		if err != nil {
			return err
		}
	default:
		err := workflow.ExecuteActivity(ctx, activities.ChargeCash, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &reserve)
		if err != nil {
			return err
		}
	}
	return nil
}
`)

	result, err := GenerateFromSource(src, Options{
		WorkflowType: "order-flow",
		Name:         "订单流程",
		FuncName:     "OrderWorkflow",
		Filename:     "order_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Workflow.Type != "order-flow" || result.Workflow.Name != "订单流程" {
		t.Fatalf("workflow id/name = %q/%q", result.Workflow.Type, result.Workflow.Name)
	}
	if result.Workflow.Program.Schema != program.WLangProgramSchemaV2 {
		t.Fatal("sdkgen program must use artifact v2")
	}
	if result.Workflow.Program.HandlerABISchema != program.WLangHandlerABISchemaV1 {
		t.Fatalf("handler ABI schema = %q", result.Workflow.Program.HandlerABISchema)
	}
	var inputDescriptorFound bool
	for _, descriptor := range result.Workflow.Program.Types {
		if descriptor.LanguageName == "flows.Input" {
			inputDescriptorFound = true
			if descriptor.Fingerprint == "" {
				t.Fatal("input descriptor fingerprint is empty")
			}
		}
	}
	if !inputDescriptorFound {
		t.Fatalf("flows.Input descriptor missing: %+v", result.Workflow.Program.Types)
	}
	taggedSource := strings.Replace(string(src), "VIP bool", "VIP bool `json:\"vip\"`", 1)
	tagged, err := GenerateFromSource([]byte(taggedSource), Options{
		WorkflowType: "order-flow",
		Name:         "订单流程",
		FuncName:     "OrderWorkflow",
		Filename:     "order_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	taggedVersion, err := program.WorkflowConfigVersion(tagged.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	resultVersion, err := program.WorkflowConfigVersion(result.Workflow)
	if err != nil {
		t.Fatal(err)
	}
	if taggedVersion == resultVersion {
		t.Fatal("type descriptor change did not affect full artifact version")
	}
	if len(result.Workflow.Activities) != 4 {
		t.Fatalf("expected four activity symbols, got %+v", result.Workflow.Activities)
	}
	data := string(result.ProgramJSON)
	for _, want := range []string{
		`"if"`,
		`"var": "input.Kind"`,
		`"symbol": "activities.NotifyVIP"`,
		`"symbol": "activities.ChargeCard"`,
	} {
		if !strings.Contains(data, want) {
			t.Fatalf("missing %s in %s", want, data)
		}
	}
}

func TestGenerateFromSourceReturnsGo2WLangDiagnostic(t *testing.T) {
	src := []byte(`package flows
func Bad(input string) string {
	return (&([]string{input}[0]))[0:]
}
`)
	_, err := GenerateFromSource(src, Options{WorkflowType: "bad", FuncName: "Bad", Filename: "bad.go"})
	if err == nil {
		t.Fatal("expected diagnostic")
	}
	var diag *go2wlang.DiagnosticError
	if !errors.As(err, &diag) {
		t.Fatalf("expected DiagnosticError, got %T %[1]v", err)
	}
}

// Activity 输入是任意表达式（下标、调用）时照常生成。
func TestGenerateFromSourceAcceptsArbitraryActivityInputExpression(t *testing.T) {
	src := []byte(`package flows

import (
	"time"
	workflow "github.com/donutnomad/tenuto/sdk/workflow"
	"example.com/app/activities"
)

type Input struct {
	Items []string
}

func IndexWorkflow(ctx workflow.Context, input Input) error {
	var out map[string]any
	return workflow.ExecuteActivity(ctx, activities.Capture, input.Items[0], workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &out)
}
`)
	result, err := GenerateFromSource(src, Options{
		WorkflowType: "index-input",
		FuncName:     "IndexWorkflow",
		Filename:     "index_workflow.go",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.Workflow.Activities["activities.Capture"]; !ok {
		t.Fatalf("activities = %+v", result.Workflow.Activities)
	}
}
