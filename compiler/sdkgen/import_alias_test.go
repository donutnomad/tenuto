package sdkgen

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// sdk/workflow 以别名导入时，生成器按别名识别 workflow 包调用。
func TestGenerateFromSource_HonoursWorkflowImportAlias(t *testing.T) {
	generate := func(body string) (Result, error) {
		src := `package flows

import (
	"time"
	wf "github.com/donutnomad/tenuto/sdk/workflow"
	"github.com/example/activities"
)

func W(ctx wf.Context, input map[string]any) error {
` + body + `
	return nil
}
`
		return GenerateFromSource([]byte(src), Options{WorkflowType: "alias-flow", FuncName: "W", Filename: "alias.go"})
	}

	t.Run("wait group lane activity is collected", func(t *testing.T) {
		result, err := generate(`
	wg := wf.NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx wf.Context) {
		_ = wf.ExecuteActivity(ctx, activities.Charge, input, wf.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, nil)
	})
	wg.Wait(ctx)`)
		require.NoError(t, err)
		require.Contains(t, result.Workflow.Activities, "activities.Charge")
	})

	t.Run("workflow.Go lane activity is collected", func(t *testing.T) {
		result, err := generate(`
	wf.Go(ctx, func(ctx wf.Context) {
		_ = wf.ExecuteActivity(ctx, activities.Charge, input, wf.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, nil)
	})`)
		require.NoError(t, err)
		require.Contains(t, result.Workflow.Activities, "activities.Charge")
	})

	t.Run("fire-and-forget ExecuteActivity is collected", func(t *testing.T) {
		result, err := generate(`
	wf.ExecuteActivity(ctx, activities.Charge, input, wf.ActivityOptions{StartToCloseTimeout: time.Minute})`)
		require.NoError(t, err)
		require.Contains(t, result.Workflow.Activities, "activities.Charge")
	})

	t.Run("update handler signature is validated", func(t *testing.T) {
		_, err := generate(`
	_ = wf.SetUpdateHandler(ctx, "set", func(value string) error { return nil })`)
		require.ErrorContains(t, err, "first parameter must be workflow.Context")
	})

	t.Run("GetInfo adds the Info descriptor", func(t *testing.T) {
		result, err := generate(`
	_ = wf.GetInfo(ctx)`)
		require.NoError(t, err)
		require.Contains(t, result.Workflow.Program.Types, "github.com/donutnomad/tenuto/sdk/workflow.Info")
	})
}

// 其他接收者上的同名方法不属于 sdk/workflow，SetUpdateHandler 的签名校验只作用于 workflow 包调用。
func TestScanUpdateHandler_IgnoresOtherReceivers(t *testing.T) {
	result, err := GenerateFromSource([]byte(`package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context, input map[string]any) error {
	ch := workflow.NewChannel(ctx)
	ch.SetUpdateHandler(ctx)
	return nil
}
`), Options{WorkflowType: "other-receiver", FuncName: "W", Filename: "other.go"})
	require.NoError(t, err)
	require.NotEmpty(t, result.ProgramJSON)
}
