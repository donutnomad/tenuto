package program

import (
	"encoding/json"
	"strings"
	"testing"

	wlangtypes "github.com/donutnomad/wlang/types"
)

// 不带 entry 的 artifact 的 canonical JSON 基线。
//
// 锁的是**序列化形状**，不是某个具体版本号：Entry 是 struct，`omitempty` 对它
// 不生效（Go 1.24 前 struct 永不被 omit），会让每个非 typed artifact 都平白多出
// 一个 "entry":{} 噪音字段。omitzero 才能让零值真正消失。
//
// 顺带保证版本号只随**真实内容**变化——配置的内容版本包含 json.Marshal(p)，
// 噪音字段会让无关的 artifact 也产生新版本。
const baselineNoEntryJSON = `{"schema":"wlang/program/v2","lang":"wflang/v1","program":[{"return":{"literal":{"type":"null","value":null}}}],"imports":{"workflow":"workflow"},"handler_abi_schema":"workflow/update-handler-abi/v1"}`

func baselineProgram() WLangProgram {
	return WLangProgram{
		Schema:  WLangProgramSchemaV2,
		Lang:    WLangLanguageV1,
		Program: []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}}},
		Imports: map[string]string{"workflow": "workflow"},

		HandlerABISchema: WLangHandlerABISchemaV1,
	}
}

// TestZeroEntryKeepsArtifactBytesIdentical 是 omitzero 的回归锁。
//
// Entry 是 struct，`omitempty` 对 struct **不生效**（Go 1.24 前 struct 永不被
// omit），只有 `omitzero` 能让零值真正不出现在 JSON 里。若 tag 被改回 omitempty，
// 这里会看到 "entry":{} 混进 canonical JSON。
func TestZeroEntryKeepsArtifactBytesIdentical(t *testing.T) {
	canonical, err := json.Marshal(baselineProgram())
	if err != nil {
		t.Fatal(err)
	}
	if got := string(canonical); got != baselineNoEntryJSON {
		t.Fatalf("零值 Entry 改变了 canonical JSON（omitzero 未生效？）\n got: %s\nwant: %s", got, baselineNoEntryJSON)
	}
	if strings.Contains(string(canonical), "entry") {
		t.Fatalf("零值 Entry 泄漏进 payload: %s", canonical)
	}
}

// TestZeroEntryDoesNotAffectContentVersion：版本号只随真实内容变化。
//
// 不硬编码具体哈希（那会变成每次改 struct 都要更新的死数字），而是断言
// 「有没有显式写零值 Entry」这件事本身不影响版本 —— 即零值不参与序列化。
func TestZeroEntryDoesNotAffectContentVersion(t *testing.T) {
	implicit, err := programVersion(baselineProgram())
	if err != nil {
		t.Fatal(err)
	}
	explicit := baselineProgram()
	explicit.Entry = WLangEntryDescriptor{} // 显式零值
	got, err := programVersion(explicit)
	if err != nil {
		t.Fatal(err)
	}
	if got != implicit {
		t.Fatalf("零值 Entry 影响了版本号（omitzero 未生效？）\n got: %s\nwant: %s", got, implicit)
	}
}

// TestEntryChangesContentVersion 是上一条的反面：类型真的变了就该产生新版本。
func TestEntryChangesContentVersion(t *testing.T) {
	base, err := programVersion(baselineProgram())
	if err != nil {
		t.Fatal(err)
	}
	typed := baselineProgram()
	typed.Entry = WLangEntryDescriptor{InputVar: "in", Input: "orders.In"}
	typed.Types = map[string]WLangTypeDescriptor{"orders.In": recordDescriptor("orders.In")}
	next, err := programVersion(typed)
	if err != nil {
		t.Fatal(err)
	}
	if base == next {
		t.Fatalf("entry 变化未产生新版本: %s", base)
	}
}

func recordDescriptor(id string) WLangTypeDescriptor {
	d := wlangtypes.TypeDescriptor{
		ID:           id,
		LanguageName: id,
		Kind:         "record",
		Fields:       []wlangtypes.FieldDescriptor{{GoName: "Amount", JSONName: "amount", TypeID: "int64"}},
	}
	d.Fingerprint = wlangtypes.DescriptorFingerprint(d)
	return d
}

// TestValidateEntryRejectsUnknownDescriptor：填了 entry 就必须指向真实 descriptor。
func TestValidateEntryRejectsUnknownDescriptor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		entry WLangEntryDescriptor
		want  string
	}{
		{"input 无 descriptor", WLangEntryDescriptor{InputVar: "in", Input: "orders.Missing"}, `entry input "orders.Missing" has no type descriptor`},
		{"output 无 descriptor", WLangEntryDescriptor{Output: "orders.Missing"}, `entry output "orders.Missing" has no type descriptor`},
		{"只有 Input 没有 InputVar", WLangEntryDescriptor{Input: "orders.In"}, `entry input "orders.In" has no input_var`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := baselineProgram()
			p.Entry = tc.entry
			err := p.ValidateArtifact()
			if err == nil {
				t.Fatalf("期望报错，实际通过")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误信息不含 %q: %v", tc.want, err)
			}
		})
	}
}

// TestValidateEntryAcceptsZeroAndValid：零值放行（非 typed workflow 必须能注册），
// 只有 InputVar（map[string]any 入参）放行，填对了也放行。
func TestValidateEntryAcceptsZeroAndValid(t *testing.T) {
	zero := baselineProgram()
	if err := zero.ValidateArtifact(); err != nil {
		t.Fatalf("零值 Entry 应放行: %v", err)
	}

	untyped := baselineProgram()
	untyped.Entry = WLangEntryDescriptor{InputVar: "req"}
	if err := untyped.ValidateArtifact(); err != nil {
		t.Fatalf("只有 InputVar 的 map[string]any 入参应放行: %v", err)
	}

	valid := baselineProgram()
	valid.Types = map[string]WLangTypeDescriptor{
		"orders.In":  recordDescriptor("orders.In"),
		"orders.Out": recordDescriptor("orders.Out"),
	}
	valid.Entry = WLangEntryDescriptor{InputVar: "in", Input: "orders.In", Output: "orders.Out"}
	if err := valid.ValidateArtifact(); err != nil {
		t.Fatalf("合法 Entry 应放行: %v", err)
	}
}

// TestEntrySurvivesEnvelopeRoundTrip：UnmarshalJSON 的 envelope 分支必须带上 Entry，
// 否则 artifact 存进去再读出来 entry 就没了（静默丢失）。
func TestEntrySurvivesEnvelopeRoundTrip(t *testing.T) {
	src := baselineProgram()
	src.Types = map[string]WLangTypeDescriptor{"orders.In": recordDescriptor("orders.In")}
	src.Entry = WLangEntryDescriptor{InputVar: "in", Input: "orders.In"}

	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	var got WLangProgram
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Entry != src.Entry {
		t.Fatalf("entry 未经 envelope 往返保留\n got: %+v\nwant: %+v", got.Entry, src.Entry)
	}
}
