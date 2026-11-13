package sdkgen

import (
	"testing"
)

// TestGeneratedArtifactsAlwaysPassRegistrationValidation 回答一个上线风险问题：
// 新增的注册期校验会不会拒掉 sdkgen 自己产出的 artifact？
//
// 这条之所以能作为风险结论，是因为全仓非测试代码里 Schema=v2 的产地只有
// generator.go:85 一处 —— 只要 sdkgen 的产出恒定合法，注册期校验就不会误伤
// 任何正常渠道生成的 artifact，剩下的风险只在手工拼装的 artifact 上。
//
// 覆盖的形态刻意包含 typed / 非 typed 两侧，以及会往 Types 里塞额外 descriptor
// 的 GetInfo 路径（addWorkflowInfoDescriptors），那是 entry 之外唯一动 Types 的地方。
func TestGeneratedArtifactsAlwaysPassRegistrationValidation(t *testing.T) {
	cases := []struct {
		name string
		src  string
		fn   string
	}{
		{
			name: "typed 入参与出参",
			src: `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type In struct {
	Amount int64 ` + "`json:\"amount\"`" + `
}

type Out struct {
	OK bool ` + "`json:\"ok\"`" + `
}

func W(ctx workflow.Context, in In) (Out, error) {
	return Out{OK: true}, nil
}`,
			fn: "W",
		},
		{
			name: "指针入参",
			src: `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type In struct {
	Amount int64 ` + "`json:\"amount\"`" + `
}

func W(ctx workflow.Context, in *In) error { return nil }`,
			fn: "W",
		},
		{
			name: "嵌套 record",
			src: `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type Nested struct {
	N int64 ` + "`json:\"n\"`" + `
}

type In struct {
	Child Nested ` + "`json:\"child\"`" + `
}

func W(ctx workflow.Context, in In) error { return nil }`,
			fn: "W",
		},
		{
			name: "无入参",
			src: `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

func W(ctx workflow.Context) error { return nil }`,
			fn: "W",
		},
		{
			// GetInfo 会往 Types 里补 workflow.Info 系列 descriptor，
			// 是 entry 之外唯一改动 Types 的路径。
			name: "使用 GetInfo",
			src: `package orders

import "github.com/donutnomad/tenuto/sdk/workflow"

type In struct {
	Amount int64 ` + "`json:\"amount\"`" + `
}

func W(ctx workflow.Context, in In) error {
	info := workflow.GetInfo(ctx)
	_ = info
	return nil
}`,
			fn: "W",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			artifact := generatedArtifact(t, tc.src, tc.fn)

			// 注册期走的就是这个校验（server.RegisterWorkflowInNamespace →
			// config.ValidateWorkflowWLangArtifacts → ValidateArtifact）。
			if err := artifact.ValidateArtifact(); err != nil {
				t.Fatalf("sdkgen 产出的 artifact 通不过注册期校验: %v", err)
			}

			// 同时守住不变式：entry 指向的 ID 必须在 Types 里，否则运行时
			// DecodeJSONValue 会报 "no JSON descriptor"。
			for _, ref := range []struct{ field, id string }{
				{"Input", artifact.Entry.Input},
				{"Output", artifact.Entry.Output},
			} {
				if ref.id == "" {
					continue
				}
				if _, ok := artifact.Types[ref.id]; !ok {
					t.Fatalf("Entry.%s = %q 不在 Types 中（keys: %v）",
						ref.field, ref.id, typeKeys(artifact.Types))
				}
			}
		})
	}
}
