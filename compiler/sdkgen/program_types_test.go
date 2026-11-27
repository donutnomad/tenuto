package sdkgen

import (
	"strings"
	"testing"

	wlangtypes "github.com/donutnomad/wlang/types"

	programpkg "github.com/donutnomad/tenuto/program"
)

const namedBasicSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Status string
type Prio int
type Level Prio

type Task struct {
	Status Status
	Prio   Prio
	Level  Level
	List   []Status
	Names  []string
	ByKey  map[Status]Prio
	Ptr    *Prio
}

type Out struct{ N int }

func W(ctx workflow.Context, in Task) (Out, error) {
	xs := []Status{"a"}
	var a any = in.Status
	if _, ok := a.(Status); ok {
		xs = append(xs, in.Status)
	}
	m := map[string][]Status{}
	m["k"] = xs
	return Out{N: len(xs) + len(m) + int(in.Prio)}, nil
}
`

// 底层是基础类型的具名类型在类型表、入口与程序里都换成底层类型；[]Status 与 []string 合成一份描述符。
func TestNormalizeProgramTypesUsesUnderlyingBasicTypes(t *testing.T) {
	result, err := GenerateFromSource([]byte(namedBasicSrc), Options{WorkflowType: "named", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	types := result.Workflow.Program.Types
	for id, descriptor := range types {
		if strings.Contains(id, "Status") || strings.Contains(id, "Prio") || strings.Contains(id, "Level") || strings.Contains(descriptor.LanguageName, "Status") {
			t.Errorf("descriptor %s (%s) still names a basic named type", id, descriptor.LanguageName)
		}
		if descriptor.Fingerprint != wlangtypes.DescriptorFingerprint(descriptor) {
			t.Errorf("descriptor %s fingerprint is stale", id)
		}
	}
	task := types["flows.Task"]
	fields := map[string]string{}
	for _, field := range task.Fields {
		fields[field.GoName] = field.TypeID
	}
	want := map[string]string{"Status": "string", "Prio": "int64", "Level": "int64", "List": "[]string", "Names": "[]string", "ByKey": "map[string]int64", "Ptr": "*int64"}
	for name, typeID := range want {
		if fields[name] != typeID {
			t.Errorf("field %s type = %q, want %q", name, fields[name], typeID)
		}
	}
	program := string(result.ProgramJSON)
	for _, stale := range []string{"flows.Status", "flows.Prio", "flows.Level"} {
		if strings.Contains(program, stale) {
			t.Errorf("program still mentions %s:\n%s", stale, program)
		}
	}
	if !strings.Contains(program, `"array\u003cstring\u003e"`) {
		t.Errorf("program misses the canonical type names:\n%s", program)
	}
}

const namedCollectionSrc = `package flows

import workflow "github.com/donutnomad/tenuto/sdk/workflow"

type Status string
type Tags []string
type Codes []Status
type Stock map[string]int

func (t Tags) Has(x string) bool {
	for _, v := range t {
		if v == x {
			return true
		}
	}
	return false
}

func (s Stock) Reserve(sku string, n int) bool {
	if s[sku] < n {
		return false
	}
	s[sku] -= n
	return true
}

type In struct {
	Tags  Tags
	Codes Codes
	Stock Stock
}

type Out struct{ N int }

func W(ctx workflow.Context, in In) (Out, error) {
	var extra Tags
	extra = append(extra, in.Tags...)
	n := 0
	if extra.Has("a") && in.Stock.Reserve("x", 1) {
		n = len(in.Codes)
	}
	return Out{N: n}, nil
}
`

// 底层是 slice、map 的具名类型同样换成底层集合类型（元素里的具名基础类型一并换掉）；方法的接收者形参随之是
// 底层类型。
func TestNormalizeProgramTypesUsesUnderlyingCollections(t *testing.T) {
	result, err := GenerateFromSource([]byte(namedCollectionSrc), Options{WorkflowType: "named", FuncName: "W", Filename: "w.go"})
	if err != nil {
		t.Fatal(err)
	}
	fields := map[string]string{}
	for _, field := range result.Workflow.Program.Types["flows.In"].Fields {
		fields[field.GoName] = field.TypeID
	}
	want := map[string]string{"Tags": "[]string", "Codes": "[]string", "Stock": "map[string]int64"}
	for name, typeID := range want {
		if fields[name] != typeID {
			t.Errorf("field %s type = %q, want %q", name, fields[name], typeID)
		}
	}
	for id, descriptor := range result.Workflow.Program.Types {
		if descriptor.Kind == wlangtypes.TypeKindAlias {
			t.Errorf("alias descriptor %s (%s) is left", id, descriptor.LanguageName)
		}
	}
	program := string(result.ProgramJSON)
	for _, stale := range []string{"flows.Tags", "flows.Codes", "flows.Stock", "flows.Status"} {
		if strings.Contains(program, stale) {
			t.Errorf("program still mentions %s:\n%s", stale, program)
		}
	}
}

// rename 只替换完整的名字，前后是类型语法的分隔符。
func TestRenameReplacesWholeTypeNames(t *testing.T) {
	names := map[string]string{"flows.Status": "string", "flows.Prio": "int64"}
	for input, want := range map[string]string{
		"flows.Status":                    "string",
		"array<flows.Status>":             "array<string>",
		"map<flows.Status,flows.Prio>":    "map<string,int64>",
		"[]flows.Status":                  "[]string",
		"[3]flows.Prio":                   "[3]int64",
		"*flows.Prio":                     "*int64",
		"flows.StatusList":                "flows.StatusList",
		"array<other.flows.Status>":       "array<other.flows.Status>",
		"map<string,array<flows.Status>>": "map<string,array<string>>",
	} {
		if got := rename(input, names); got != want {
			t.Errorf("rename(%q) = %q, want %q", input, got, want)
		}
	}
}

// map<string,V> 的 V 是集合时补齐 V 与 map 的描述符；已有的描述符不重复登记，含类型表之外的类型时不补。
func TestCollectionDescriptorsForMapValues(t *testing.T) {
	table := map[string]programpkg.WLangTypeDescriptor{
		"flows.Item": {ID: "flows.Item", LanguageName: "flows.Item", Kind: wlangtypes.TypeKindRecord},
		"[]string":   {ID: "[]string", LanguageName: "array<string>", Kind: wlangtypes.TypeKindSlice, Elem: "string"},
	}
	program := []any{
		map[string]any{"let": map[string]any{"a": map[string]any{"map": map[string]any{"type": []any{"string", "array<string>"}, "entries": []any{}}}}},
		map[string]any{"let": map[string]any{"b": map[string]any{"zero": "map<string,array<any>>"}}},
		map[string]any{"let": map[string]any{"c": map[string]any{"map": map[string]any{"type": []any{"string", "map<string,array<flows.Item>>"}, "entries": []any{}}}}},
		map[string]any{"let": map[string]any{"d": map[string]any{"zero": "map<string,array<*flows.Item>>"}}},
		map[string]any{"let": map[string]any{"e": map[string]any{"zero": "map<string,int64>"}}},
	}
	added := collectionDescriptors(program, table)
	want := map[string]programpkg.WLangTypeDescriptor{
		"map[string][]string":                {LanguageName: "map<string,array<string>>", Kind: wlangtypes.TypeKindMap, Key: "string", Elem: "[]string"},
		"[]any":                              {LanguageName: "array<any>", Kind: wlangtypes.TypeKindSlice, Elem: "any"},
		"map[string][]any":                   {LanguageName: "map<string,array<any>>", Kind: wlangtypes.TypeKindMap, Key: "string", Elem: "[]any"},
		"[]flows.Item":                       {LanguageName: "array<flows.Item>", Kind: wlangtypes.TypeKindSlice, Elem: "flows.Item"},
		"map[string][]flows.Item":            {LanguageName: "map<string,array<flows.Item>>", Kind: wlangtypes.TypeKindMap, Key: "string", Elem: "[]flows.Item"},
		"map[string]map[string][]flows.Item": {LanguageName: "map<string,map<string,array<flows.Item>>>", Kind: wlangtypes.TypeKindMap, Key: "string", Elem: "map[string][]flows.Item"},
	}
	if len(added) != len(want) {
		t.Fatalf("added = %v, want %d descriptors", added, len(want))
	}
	for id, expected := range want {
		got, ok := added[id]
		if !ok || got.LanguageName != expected.LanguageName || got.Kind != expected.Kind || got.Key != expected.Key || got.Elem != expected.Elem {
			t.Errorf("descriptor %s = %+v, want %+v", id, got, expected)
		}
		if got.Fingerprint != wlangtypes.DescriptorFingerprint(got) {
			t.Errorf("descriptor %s fingerprint is stale", id)
		}
	}
}
