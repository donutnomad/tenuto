package interpreter

import (
	"reflect"
	"testing"

	"github.com/donutnomad/tenuto/program"
)

const resultTargetsSrc = `package flows

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type Typed struct {
	Tags  []string
	Inner Nested
}

type Nested struct {
	Tags []string
}

type Loose struct {
	Inner map[string]any
}

func W(ctx workflow.Context, input map[string]any) (int, error) {
	opts := workflow.ActivityOptions{StartToCloseTimeout: time.Minute}
	var a Typed
	if err := workflow.ExecuteActivity(ctx, "single", input, opts).Get(ctx, &a); err != nil {
		return 0, err
	}
	var b Typed
	if err := workflow.ExecuteActivity(ctx, "mixed", input, opts).Get(ctx, &b); err != nil {
		return 0, err
	}
	var c Loose
	if err := workflow.ExecuteActivity(ctx, "mixed", input, opts).Get(ctx, &c); err != nil {
		return 0, err
	}
	_, ok := c.Inner["Tags"]
	if ok {
		return len(a.Tags) + len(b.Tags) + 1, nil
	}
	return len(a.Tags) + len(b.Tags), nil
}
`

// 同一个 Activity type 的结果在不同调用处 Get 进不同的类型时，结果进入解释器时不知道哪一处会读它：按其中一个
// 类型去掉 null 字段会改变另一个类型的读法（Loose.Inner 是 map[string]any，null 的键在 Go 里存在），所以原样交给
// 解释器。只有一个解码类型的位置照常规范化。
func TestResultNormalizerSkipsConflictingTargets(t *testing.T) {
	normalizer := newResultNormalizer(compileSource(t, resultTargetsSrc))
	payload := func() map[string]any {
		return map[string]any{"Tags": nil, "Inner": map[string]any{"Tags": nil}}
	}
	mixed := normalizer.normalize(program.PayloadActivityResult, "mixed", payload())
	if !reflect.DeepEqual(mixed, payload()) {
		t.Fatalf("mixed targets normalized to %v, want the payload unchanged", mixed)
	}
	single := normalizer.normalize(program.PayloadActivityResult, "single", payload())
	if want := (map[string]any{"Inner": map[string]any{}}); !reflect.DeepEqual(single, want) {
		t.Fatalf("single target normalized to %v, want %v", single, want)
	}
}
