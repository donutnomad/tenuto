package interpreter

import (
	"encoding/json"
	"strings"
	"testing"

	wlregistry "github.com/donutnomad/wlang/registry"
	"github.com/donutnomad/wlang/types"
)

func typedTestRegistry(t *testing.T) *wlregistry.Registry {
	t.Helper()
	d := types.TypeDescriptor{
		ID:           "orders.PaymentOutput",
		LanguageName: "orders.PaymentOutput",
		Kind:         "record",
		Fields: []types.FieldDescriptor{
			{GoName: "Amount", JSONName: "amount", TypeID: "int64"},
			{GoName: "OK", JSONName: "ok", TypeID: "boolean"},
		},
	}
	d.Fingerprint = types.DescriptorFingerprint(d)
	reg := wlregistry.New()
	if err := reg.BindDescriptors(map[string]types.TypeDescriptor{d.ID: d}); err != nil {
		t.Fatalf("bind descriptors: %v", err)
	}
	return reg
}

func typedTestRecord() *types.Record {
	v := types.NewRecord("orders.PaymentOutput", map[string]types.Value{
		"Amount": types.NewValue("int64", int64(4200)),
		"OK":     types.NewValue("boolean", true),
	})
	rec, _ := types.AsRecord(v)
	return rec
}

// TestRecordToMapPreservesFields 断言 typed record 经持久化边界
// 后不再变成空 map，且 JSON tag 生效、大整数不失真。
func TestRecordToMapPreservesFields(t *testing.T) {
	reg := typedTestRegistry(t)
	got, err := RecordToMap(reg, typedTestRecord())
	if err != nil {
		t.Fatalf("recordToMap: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("typed record 仍然变成了空 map —— 静默丢数据未修复")
	}
	// JSON tag 生效：键是 amount/ok（小写），不是 Go 字段名 Amount/OK。
	amount, ok := got["amount"]
	if !ok {
		t.Fatalf("缺少 amount 字段（JSON tag 未生效？）: %#v", got)
	}
	// UseNumber 保精度：不应被转成 float64。
	num, ok := amount.(json.Number)
	if !ok {
		t.Fatalf("amount 应为 json.Number（UseNumber 未生效），实际 %T", amount)
	}
	if num.String() != "4200" {
		t.Fatalf("amount 值失真: %s", num)
	}
	if got["ok"] != true {
		t.Fatalf("ok 字段丢失或错误: %#v", got["ok"])
	}
}

// TestRecordToMapFallsBackForPlainValues 锁「非 record 值行为与 anyToMap 一致」，
// 保证调用点替换是安全的。
func TestRecordToMapFallsBackForPlainValues(t *testing.T) {
	reg := typedTestRegistry(t)
	for _, tc := range []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"plain map", map[string]any{"a": 1}},
		{"struct", struct {
			X int `json:"x"`
		}{X: 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RecordToMap(reg, tc.value)
			if err != nil {
				t.Fatalf("recordToMap: %v", err)
			}
			want := anyToMap(tc.value)
			if len(got) != len(want) {
				t.Fatalf("与 anyToMap 行为不一致\n got: %#v\nwant: %#v", got, want)
			}
		})
	}
}

// TestRecordToMapNilRegistryFallsBack：reg 为 nil 时不能 panic，回落旧行为。
func TestRecordToMapNilRegistryFallsBack(t *testing.T) {
	got, err := RecordToMap(nil, typedTestRecord())
	if err != nil {
		t.Fatalf("nil registry 不应报错: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("nil registry 应回落 anyToMap 的空 map 行为: %#v", got)
	}
}

// TestMustRecordToMapPanicsOnEncodeFailure：编码失败必须炸，不能静默回落。
//
// 静默回落会写入一个空 map 并当作成功继续推进——activity 拿到空输入、历史里
// 留下空载荷，而没有任何信号。这正是本次改造要消灭的那类 bug，不能在错误分支
// 上重新引入。panic 会被 Execute 主协程的 recover 接住转成 WFT 失败。
func TestMustRecordToMapPanicsOnEncodeFailure(t *testing.T) {
	emptyReg := wlregistry.New() // 没有 bind 任何 descriptor → EncodeJSONValue 必失败
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("descriptor 缺失时应 panic，而非静默回落成空 map")
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic 值应是 error，实际 %T", r)
		}
		if !strings.Contains(err.Error(), "encode workflow payload") {
			t.Fatalf("panic 信息应可辨识: %v", err)
		}
	}()
	mustRecordToMap(emptyReg, typedTestRecord())
}

// TestMustRecordToMapPassesThroughPlainValues：非 record 值不受影响，
// 行为与 anyToMap 一致（这条路径不会 panic）。
func TestMustRecordToMapPassesThroughPlainValues(t *testing.T) {
	reg := typedTestRegistry(t)
	got := mustRecordToMap(reg, map[string]any{"a": 1})
	if len(got) != 1 || got["a"] != 1 {
		t.Fatalf("普通 map 应原样通过: %#v", got)
	}
}

// TestMapFromAnyPreservesRecordFields：channel 交付 typed record 时也不能丢字段。
//
// Send(ctx, v any) 接受任意值，用户可以直接塞一个有名 DTO。channel 不跨持久化
// 边界，但接收方拿到空 map 且无任何信号，在内存里同样难查。
func TestMapFromAnyPreservesRecordFields(t *testing.T) {
	reg := typedTestRegistry(t)
	got := mapFromAny(reg, typedTestRecord())
	if len(got) == 0 {
		t.Fatal("channel 交付的 typed record 变成了空 map")
	}
	if got["amount"] == nil || got["ok"] != true {
		t.Fatalf("字段丢失: %#v", got)
	}
}

// TestMapFromAnyHandlesPlainValues：非 record 值保持原有行为，
// registry 为 nil（activeAPI 未注入）时也不能炸。
func TestMapFromAnyHandlesPlainValues(t *testing.T) {
	reg := typedTestRegistry(t)
	if got := mapFromAny(reg, nil); got != nil {
		t.Fatalf("nil 应返回 nil: %#v", got)
	}
	if got := mapFromAny(reg, map[string]any{"a": 1}); got["a"] != 1 {
		t.Fatalf("普通 map 应原样通过: %#v", got)
	}
	if got := mapFromAny(nil, map[string]any{"a": 1}); got["a"] != 1 {
		t.Fatalf("registry 为 nil 时普通 map 仍应通过: %#v", got)
	}
}

// TestUnpackTupleIsRequiredForBodyReturn 固化一个前提：Go 侧 (T, error)
// 在 WLang 是 tuple，裸 switch 三个分支全部落空。
//
// 这条锁住的是「为什么必须先 UnpackTuple」——若哪天 result 形态变了，
// 这个测试会失败并提示重新审视 executeWLang 的返回值分派。
func TestUnpackTupleIsRequiredForBodyReturn(t *testing.T) {
	rec := typedTestRecord()
	tuple := types.NewValue("tuple<orders.PaymentOutput,null>", []any{rec, nil})

	// 不先 UnpackTuple 的裸 switch：三个分支全部落空。
	matched := ""
	switch tuple.Go().(type) {
	case error:
		matched = "error"
	case map[string]any:
		matched = "map"
	case *types.Record:
		matched = "record"
	}
	if matched != "" {
		t.Fatalf("tuple 竟然命中了 %s 分支（前提变了）", matched)
	}

	// 解 tuple 后才拿得到 record。
	vals, ok := types.UnpackTuple(tuple)
	if !ok || len(vals) != 2 {
		t.Fatalf("UnpackTuple 失败: ok=%v n=%d", ok, len(vals))
	}
	if _, isRecord := vals[0].Go().(*types.Record); !isRecord {
		t.Fatalf("tuple[0] 应是 *types.Record，实际 %T", vals[0].Go())
	}
	if vals[1].TypeName() != types.TNull {
		t.Fatalf("tuple[1] 应是 null，实际 %s", vals[1].TypeName())
	}

	// 端到端：解包后经 recordToMap 应得到完整字段。
	out, err := RecordToMap(typedTestRegistry(t), vals[0].Go())
	if err != nil {
		t.Fatalf("recordToMap: %v", err)
	}
	if len(out) == 0 {
		t.Fatal("解包后仍丢数据")
	}
}
