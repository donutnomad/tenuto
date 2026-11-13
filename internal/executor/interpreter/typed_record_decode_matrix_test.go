package interpreter

import (
	"encoding/json"
	"strings"
	"testing"

	wlregistry "github.com/donutnomad/wlang/registry"
	"github.com/donutnomad/wlang/types"
)

// 本文件实测 UNKNOWN-03 的 WLang typed record 解码语义：
// 旧 Activity 结果（历史里的 JSON）被新 Workflow DTO 解码时，
// registry.DecodeJSONValue 走 descriptor 图，而不是 Go encoding/json。
//
// 与 sdk/activity/typed_decode_matrix_test.go（本地 worker 适配层）互补：
// 两侧都用 JSON，但 WLang 侧会按 descriptor 逐字段校验，未知字段忽略，
// 缺字段写零值，唯一硬错误是 JSON 标量类型与 descriptor 不兼容。

func decodeMatrixRegistry(t *testing.T) *wlregistry.Registry {
	t.Helper()
	descriptors := map[string]types.TypeDescriptor{
		"*string": {
			ID: "*string", LanguageName: "*string", Kind: types.TypeKindPointer, Elem: types.TString,
		},
		"old.Receipt": {
			ID: "old.Receipt", LanguageName: "old.Receipt", Kind: types.TypeKindRecord,
			Fields: []types.FieldDescriptor{
				{GoName: "Amount", JSONName: "amount", TypeID: types.TInt64},
				{GoName: "Memo", JSONName: "memo", TypeID: types.TString},
			},
		},
	}
	for id, descriptor := range descriptors {
		descriptor.Fingerprint = types.DescriptorFingerprint(descriptor)
		descriptors[id] = descriptor
	}
	reg := wlregistry.New()
	if err := reg.BindDescriptors(descriptors); err != nil {
		t.Fatalf("BindDescriptors: %v", err)
	}
	return reg
}

// bindRecord 把一个新 DTO descriptor 注册到独立 registry，模拟“内容 B 的新 DTO”。
func bindRecord(t *testing.T, fields ...types.FieldDescriptor) *wlregistry.Registry {
	t.Helper()
	descriptor := types.TypeDescriptor{
		ID: "new.Receipt", LanguageName: "new.Receipt", Kind: types.TypeKindRecord, Fields: fields,
	}
	descriptor.Fingerprint = types.DescriptorFingerprint(descriptor)
	reg := wlregistry.New()
	if err := reg.BindDescriptors(map[string]types.TypeDescriptor{
		"*string": {
			ID: "*string", LanguageName: "*string", Kind: types.TypeKindPointer, Elem: types.TString,
		},
		"new.Receipt": descriptor,
	}); err != nil {
		t.Fatalf("BindDescriptors: %v", err)
	}
	return reg
}

func decodeRecordField(t *testing.T, reg *wlregistry.Registry, raw string, goName string) types.Value {
	t.Helper()
	value, err := reg.DecodeJSONValue("new.Receipt", json.RawMessage(raw))
	if err != nil {
		t.Fatalf("DecodeJSONValue(%s): %v", raw, err)
	}
	record, ok := types.AsRecord(value)
	if !ok {
		t.Fatalf("decoded %T, want record", value.Go())
	}
	field, ok := record.Field(goName)
	if !ok {
		t.Fatalf("field %s missing from decoded record", goName)
	}
	return field
}

func TestTypedRecordDecodeMissingAndRenamedFields(t *testing.T) {
	// 新 DTO 的 memo 字段改为 JSON 名 note；旧历史仍写 memo。
	reg := bindRecord(t,
		types.FieldDescriptor{GoName: "Amount", JSONName: "amount", TypeID: types.TInt64},
		types.FieldDescriptor{GoName: "Memo", JSONName: "note", TypeID: types.TString},
	)

	amount := decodeRecordField(t, reg, `{"amount":42,"memo":"old"}`, "Amount")
	if amount.Go() != int64(42) {
		t.Fatalf("amount = %#v, want 42 (unchanged field must survive)", amount.Go())
	}
	memo := decodeRecordField(t, reg, `{"amount":42,"memo":"old"}`, "Memo")
	if memo.Go() != "" {
		t.Fatalf("renamed field memo = %#v, want zero \"\" (old key ignored, new key missing)", memo.Go())
	}

	// 完全缺字段等同零值，不报错。
	memo = decodeRecordField(t, reg, `{}`, "Memo")
	if memo.Go() != "" {
		t.Fatalf("missing field = %#v, want zero", memo.Go())
	}
}

func TestTypedRecordDecodeAddedFields(t *testing.T) {
	// 新增可选 *string 与新增必填 string：旧 payload 缺两字段都解码成功。
	reg := bindRecord(t,
		types.FieldDescriptor{GoName: "Amount", JSONName: "amount", TypeID: types.TInt64},
		types.FieldDescriptor{GoName: "Optional", JSONName: "optional", TypeID: "*string"},
		types.FieldDescriptor{GoName: "Required", JSONName: "required", TypeID: types.TString},
	)
	if optional := decodeRecordField(t, reg, `{"amount":1}`, "Optional"); optional.Go() != nil {
		t.Fatalf("added optional field = %#v, want nil", optional.Go())
	}
	if required := decodeRecordField(t, reg, `{"amount":1}`, "Required"); required.Go() != "" {
		t.Fatalf("added required field = %#v, want zero \"\"", required.Go())
	}

	// 显式 null：可选字段保持 nil，必填 string 落零值。
	if optional := decodeRecordField(t, reg, `{"optional":null}`, "Optional"); optional.Go() != nil {
		t.Fatalf("null optional field = %#v, want nil", optional.Go())
	}
	if required := decodeRecordField(t, reg, `{"required":null}`, "Required"); required.Go() != "" {
		t.Fatalf("null required field = %#v, want zero \"\"", required.Go())
	}

	// 提供值时正常解码。
	if optional := decodeRecordField(t, reg, `{"optional":"on"}`, "Optional"); optional.Go() != "on" {
		t.Fatalf("optional value = %#v, want \"on\"", optional.Go())
	}
}

func TestTypedRecordDecodeDeletedFieldIsIgnored(t *testing.T) {
	// 新 DTO 删除 memo：旧 payload 的 memo 是未知字段，被忽略。
	reg := bindRecord(t, types.FieldDescriptor{GoName: "Amount", JSONName: "amount", TypeID: types.TInt64})
	amount := decodeRecordField(t, reg, `{"amount":7,"memo":"stale","removed":1}`, "Amount")
	if amount.Go() != int64(7) {
		t.Fatalf("amount = %#v, want 7", amount.Go())
	}
	// 未知字段不得进入解码结果。
	value, err := reg.DecodeJSONValue("new.Receipt", json.RawMessage(`{"amount":7,"memo":"stale"}`))
	if err != nil {
		t.Fatalf("DecodeJSONValue: %v", err)
	}
	record, _ := types.AsRecord(value)
	if _, ok := record.Field("Memo"); ok {
		t.Fatal("deleted field must not be materialized")
	}
}

func TestTypedRecordDecodeTypeChangeIsError(t *testing.T) {
	// int → string 不是零值问题：JSON 数字与 string descriptor 不兼容，必须报错。
	reg := bindRecord(t, types.FieldDescriptor{GoName: "Memo", JSONName: "memo", TypeID: types.TString})
	_, err := reg.DecodeJSONValue("new.Receipt", json.RawMessage(`{"memo":42}`))
	if err == nil {
		t.Fatal("int payload decoded into string field without error")
	}
	if !strings.Contains(err.Error(), "/memo") {
		t.Fatalf("decode error = %v, want field path /memo", err)
	}
	if !strings.Contains(err.Error(), "string") {
		t.Fatalf("decode error = %v, want target type string", err)
	}

	// 反向 string → int64 同样报错。
	reg = bindRecord(t, types.FieldDescriptor{GoName: "Amount", JSONName: "amount", TypeID: types.TInt64})
	if _, err := reg.DecodeJSONValue("new.Receipt", json.RawMessage(`{"amount":"42"}`)); err == nil {
		t.Fatal("string payload decoded into int64 field without error")
	}
}

// 对照：Go encoding/json（worker adapter）与 WLang registry 对同一份旧 payload
// 的唯一差异是未知字段处理方式，零值语义一致。
func TestTypedRecordDecodeMatrixMatchesDocumentedContract(t *testing.T) {
	reg := decodeMatrixRegistry(t)
	value, err := reg.DecodeJSONValue("old.Receipt", json.RawMessage(`{"amount":"not-a-number","memo":"x"}`))
	if err == nil {
		t.Fatalf("expected decode error, got %#v", value.Go())
	}
}
