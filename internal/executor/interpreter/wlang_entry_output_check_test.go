package interpreter

import (
	"strings"
	"testing"

	wlregistry "github.com/donutnomad/wlang/registry"
	"github.com/donutnomad/wlang/types"
)

func outputCheckRegistry(t *testing.T) *wlregistry.Registry {
	t.Helper()
	reg := wlregistry.New()
	descriptors := map[string]types.TypeDescriptor{}
	for _, id := range []string{"orders.Out", "orders.Other"} {
		d := types.TypeDescriptor{
			ID:           id,
			LanguageName: id,
			Kind:         "record",
			Fields:       []types.FieldDescriptor{{GoName: "OK", JSONName: "ok", TypeID: "boolean"}},
		}
		d.Fingerprint = types.DescriptorFingerprint(d)
		descriptors[id] = d
	}
	if err := reg.BindDescriptors(descriptors); err != nil {
		t.Fatalf("bind: %v", err)
	}
	return reg
}

func recordValue(typeName string) types.Value {
	return types.NewRecord(typeName, map[string]types.Value{
		"OK": types.NewValue("boolean", true),
	})
}

// TestCheckEntryOutputTypeRejectsMismatch 是 Entry.Output 的**真实消费者**。
//
// 在此之前 Entry.Output 只被写入 artifact 和校验"存在性"，运行时没有任何代码读它 ——
// 它参与版本哈希却不起作用。这条锁住它确实能挡住「artifact 声明返回 A、body 实际
// 返回 B」：这类不一致会让下游按 A 去解码一份 B 的数据，错在消费端而非产生端。
func TestCheckEntryOutputTypeRejectsMismatch(t *testing.T) {
	reg := outputCheckRegistry(t)
	err := checkEntryOutputType(reg, "orders.Out", recordValue("orders.Other"))
	if err == nil {
		t.Fatal("声明与实际类型不符时应报错")
	}
	if !strings.Contains(err.Error(), "type mismatch") {
		t.Fatalf("错误信息应指明类型不符: %v", err)
	}
}

func TestCheckEntryOutputTypeAcceptsMatch(t *testing.T) {
	reg := outputCheckRegistry(t)
	if err := checkEntryOutputType(reg, "orders.Out", recordValue("orders.Out")); err != nil {
		t.Fatalf("类型一致时不应报错: %v", err)
	}
}

// TestCheckEntryOutputTypeSkipsWhenNotDeclared：没有 typed 出参的 workflow
// （error-only、标量返回）不受影响。
func TestCheckEntryOutputTypeSkipsWhenNotDeclared(t *testing.T) {
	reg := outputCheckRegistry(t)
	if err := checkEntryOutputType(reg, "", recordValue("orders.Other")); err != nil {
		t.Fatalf("未声明 Output 时不应校验: %v", err)
	}
	if err := checkEntryOutputType(nil, "orders.Out", recordValue("orders.Out")); err != nil {
		t.Fatalf("registry 为 nil 时不应校验: %v", err)
	}
	if err := checkEntryOutputType(reg, "orders.Out", types.NewValue(types.TNull, nil)); err != nil {
		t.Fatalf("null 出参无类型可比，不应报错: %v", err)
	}
}

// TestCheckEntryOutputTypeNormalizesNameForms 锁住这条校验为什么不能用字符串直接比：
// Entry.Output 存的是 descriptor ID，而 value.TypeName() 返回 LanguageName，
// 两者拼写可以不同（指针类型的 ID 带 "*" 前缀）。DescriptorID 负责归一。
func TestCheckEntryOutputTypeNormalizesNameForms(t *testing.T) {
	reg := wlregistry.New()
	d := types.TypeDescriptor{
		ID:           "github.com/acme/orders.Out",
		LanguageName: "orders.Out",
		Kind:         "record",
		Fields:       []types.FieldDescriptor{{GoName: "OK", JSONName: "ok", TypeID: "boolean"}},
	}
	d.Fingerprint = types.DescriptorFingerprint(d)
	if err := reg.BindDescriptors(map[string]types.TypeDescriptor{d.ID: d}); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// 声明用 canonical ID，实际值带的是 LanguageName —— 直接 == 会误判不符。
	if err := checkEntryOutputType(reg, d.ID, recordValue(d.LanguageName)); err != nil {
		t.Fatalf("两种拼写指向同一类型，应判为一致: %v", err)
	}
}
