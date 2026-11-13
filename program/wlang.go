package program

import (
	"fmt"

	"github.com/donutnomad/tenuto/protocol"
	wlangregistry "github.com/donutnomad/wlang/registry"
	wlangtypes "github.com/donutnomad/wlang/types"
)

const (
	WLangProgramSchemaV2              = "wlang/program/v2"
	WLangLanguageV1                   = "wflang/v1"
	WLangHandlerABISchemaV1           = "workflow/update-handler-abi/v1"
	WLangProgramV2Capability          = "wlang-program-schema:wlang/program/v2"
	WLangTypeDescriptorV1Capability   = "wlang-type-descriptor-v1"
	WLangUpdateHandlerABIV1Capability = "wlang-update-handler-abi-v1"
)

type WLangTypeDescriptor = wlangtypes.TypeDescriptor

// WLangEntryDescriptor 描述 workflow 入口的类型边界：入参绑哪个变量名、
// 业务入参与业务返回值各自的 type descriptor ID。
//
// Input/Output 存的是 Types 这个 map 的 **key**（descriptor ID），不是
// types.Value.TypeName() 那种 LanguageName——两者不相等，需要比较时用
// registry.DescriptorID() 转换。指针入参的 ID 带 "*" 前缀。
//
// Input/Output 为空表示该侧不做 typed 绑定，走 map[string]any 路径：
// error-only 返回、map[string]any 入参、无名/多名形参都属于这种情况。
type WLangEntryDescriptor struct {
	// InputVar 是 Go 形参名，入口有可引用的形参时总是填写（typed 与 map[string]any 都填）。
	// 翻译产物的 body 直接按该名字引用入口参数，所以 engine 必须用这个名字绑定入参，不能自己另起一个。
	InputVar string `json:"input_var,omitempty"`
	Input    string `json:"input,omitempty"`
	Output   string `json:"output,omitempty"`
}

// WLangProgram 是 Workflow 的 WLang 程序产物：语句、import 表、类型描述符与入口类型边界。
type WLangProgram struct {
	Schema  string                         `json:"schema,omitempty"`
	Lang    string                         `json:"lang,omitempty"`
	Program []any                          `json:"program"`
	Imports map[string]string              `json:"imports,omitempty"`
	Types   map[string]WLangTypeDescriptor `json:"types,omitempty"`
	// Entry 必须用 omitzero 而非 omitempty：它是 struct，omitempty 对 struct
	// **不生效**（Go 1.24 前 struct 永不被 omit），会让每个非 typed artifact
	// 平白多出一个 "entry":{} 噪音字段；配置的内容版本按序列化结果计算，
	// 噪音字段会让无关的 artifact 也产生新版本。
	// 同 store.go 的 EnqueuedAt 先例。
	Entry            WLangEntryDescriptor `json:"entry,omitzero"`
	HandlerABISchema string               `json:"handler_abi_schema,omitempty"`
}

// NewWLangProgram 用当前 schema、语言和 handler ABI 封装一段 wlang 语句。
func NewWLangProgram(stmts []any) WLangProgram {
	return WLangProgram{
		Schema:           WLangProgramSchemaV2,
		Lang:             WLangLanguageV1,
		Program:          stmts,
		HandlerABISchema: WLangHandlerABISchemaV1,
	}
}

func (p WLangProgram) RequiredCapabilities() []string {
	if p.Schema != WLangProgramSchemaV2 {
		return []string{"wlang-program-schema:" + p.Schema}
	}
	return []string{WLangProgramV2Capability, WLangTypeDescriptorV1Capability, WLangUpdateHandlerABIV1Capability}
}

func CurrentWLangCapabilities() []string {
	return []string{WLangProgramV2Capability, WLangTypeDescriptorV1Capability, WLangUpdateHandlerABIV1Capability}
}

type UnsupportedWLangCapabilityError struct {
	Capability string
}

func (e *UnsupportedWLangCapabilityError) Error() string {
	return fmt.Sprintf("workflow worker does not support required capability %q", e.Capability)
}

// Is 把它归为请求引起的失败：程序要求的能力超出 Worker 的声明。
func (e *UnsupportedWLangCapabilityError) Is(target error) bool {
	return target == protocol.ErrInvalidRequest
}

func (p WLangProgram) ValidateCapabilities(supported []string) error {
	set := make(map[string]struct{}, len(supported))
	for _, capability := range supported {
		set[capability] = struct{}{}
	}
	for _, required := range p.RequiredCapabilities() {
		if _, ok := set[required]; !ok {
			return &UnsupportedWLangCapabilityError{Capability: required}
		}
	}
	return nil
}

func (p WLangProgram) ValidateArtifact() error {
	if len(p.Program) == 0 {
		return protocol.InvalidRequestf("wlang program is empty")
	}
	if p.Schema != WLangProgramSchemaV2 {
		return protocol.InvalidRequestf("unsupported wlang program schema %q", p.Schema)
	}
	if p.Lang != WLangLanguageV1 {
		return protocol.InvalidRequestf("wlang program %s requires language %q, got %q", p.Schema, WLangLanguageV1, p.Lang)
	}
	if p.HandlerABISchema != WLangHandlerABISchemaV1 {
		return protocol.InvalidRequestf("wlang program %s requires handler ABI schema %q, got %q", p.Schema, WLangHandlerABISchemaV1, p.HandlerABISchema)
	}
	descriptors := make(map[string]wlangtypes.TypeDescriptor, len(p.Types))
	for id, descriptor := range p.Types {
		if descriptor.Fingerprint == "" {
			return protocol.InvalidRequestf("wlang type descriptor %q has empty fingerprint", id)
		}
		descriptors[id] = descriptor
	}
	if err := wlangregistry.New().BindDescriptors(descriptors); err != nil {
		return protocol.InvalidRequestf("validate wlang type descriptors: %w", err)
	}
	return p.validateEntry()
}

// validateEntry 校验 entry 指向的 descriptor 确实存在。
//
// 零值 Entry 一律放行：无入参、纯 error 返回的 workflow 没有 entry metadata。
// 规则是「填了就必须有效」，不是「必须填」。
func (p WLangProgram) validateEntry() error {
	// 有 Input 必须有 InputVar：只有类型没有变量名，engine 不知道绑到哪。
	// 只有 InputVar 表示 map[string]any 入参，按原样绑定。
	if p.Entry.Input != "" && p.Entry.InputVar == "" {
		return protocol.InvalidRequestf("wlang entry input %q has no input_var", p.Entry.Input)
	}
	for _, ref := range []struct{ field, id string }{
		{"input", p.Entry.Input},
		{"output", p.Entry.Output},
	} {
		if ref.id == "" {
			continue
		}
		if _, ok := p.Types[ref.id]; !ok {
			return protocol.InvalidRequestf("wlang entry %s %q has no type descriptor", ref.field, ref.id)
		}
	}
	return nil
}
