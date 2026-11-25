package program

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/donutnomad/tenuto/protocol"
	wlangregistry "github.com/donutnomad/wlang/registry"
)

// PayloadPositionKind 是 History 里一类由 Program 解码的载荷。
type PayloadPositionKind string

const (
	PayloadWorkflowInput     PayloadPositionKind = "workflow input"
	PayloadSignal            PayloadPositionKind = "signal"
	PayloadActivityInput     PayloadPositionKind = "activity input"
	PayloadActivityResult    PayloadPositionKind = "activity result"
	PayloadChildResult       PayloadPositionKind = "child workflow result"
	PayloadUpdateArgument    PayloadPositionKind = "update argument"
	PayloadSideEffect        PayloadPositionKind = "side effect"
	PayloadMutableSideEffect PayloadPositionKind = "mutable side effect"
	PayloadErrorDetail       PayloadPositionKind = "error detail"
)

// PayloadPosition 标识 History 里的一类载荷：Name 是 Signal 名、Activity type、子 Workflow Type、Update 名或
// MutableSideEffect ID，Index 是 Update 参数的序号。同一位置的载荷由 Program 解码成同一个类型。SideEffect 的值与
// 应用错误的 detail（`workflow.ApplicationErrorDetail`）各自只有一个位置。
type PayloadPosition struct {
	Kind  PayloadPositionKind
	Name  string
	Index int
}

func (p PayloadPosition) String() string {
	switch p.Kind {
	case PayloadWorkflowInput:
		return "workflow input"
	case PayloadSignal:
		return fmt.Sprintf("signal %q", p.Name)
	case PayloadActivityInput:
		return fmt.Sprintf("input of pending activity %q", p.Name)
	case PayloadActivityResult:
		return fmt.Sprintf("result of activity %q", p.Name)
	case PayloadChildResult:
		return fmt.Sprintf("result of child workflow %q", p.Name)
	case PayloadUpdateArgument:
		return fmt.Sprintf("argument %d of update %q", p.Index, p.Name)
	case PayloadSideEffect:
		return "side effect value"
	case PayloadMutableSideEffect:
		return fmt.Sprintf("mutable side effect %q", p.Name)
	case PayloadErrorDetail:
		return "application error detail"
	}
	return string(p.Kind) + " " + p.Name
}

// named 报告这类位置是否按名字区分。
func (k PayloadPositionKind) named() bool {
	switch k {
	case PayloadWorkflowInput, PayloadSideEffect, PayloadErrorDetail:
		return false
	}
	return true
}

// PayloadType 是 Program 在一个位置把载荷解码成的类型：Name 是 WLang 的类型名（如 order.Approval），Wire 是它的
// JSON 结构。
type PayloadType struct {
	Name string
	Wire protocol.WireType
}

// UnresolvedPayloadSite 是 Program 里一处解码 History 载荷、但位置或类型无法静态确定的地方。
type UnresolvedPayloadSite struct {
	// Position 是能确定的部分：Kind 为空表示 future 追溯不到创建它的调用，种类也无法从声明类型得出；按名字区分的
	// 种类 Name 为空表示名字不是可折叠的常量。
	Position PayloadPosition
	// Type 是解码目标的类型名，空表示无法确定；Wire 是它的 JSON 结构，类型名没有 JSON 描述时为 nil。
	Type string
	Wire *protocol.WireType
	// Path 是解码处在 WLang 程序里的路径，如 program[11].expr.AddSignalFuture[3].fn.do[0].expr.Get；
	// 同包函数在 `let.__wlang_func_<函数名>` 下。
	Path string
	// Reason 说明无法确定的原因。
	Reason string
}

func (s UnresolvedPayloadSite) String() string {
	what := "a payload"
	switch kind := s.Position.Kind; {
	case kind == "":
	case !kind.named() || s.Position.Name != "":
		what = s.Position.String()
	default:
		what = "a " + string(kind) + " with a non-constant name"
	}
	target := "an unknown type"
	if s.Type != "" {
		target = s.Type
	}
	return fmt.Sprintf("%s decoded into %s at %s (%s)", what, target, s.Path, s.Reason)
}

// PayloadTypeSet 是 PayloadTypes 的结果。
type PayloadTypeSet struct {
	// Positions 是每个位置解码用的类型，按类型名去重。
	Positions map[PayloadPosition][]PayloadType
	// Unresolved 是位置或类型无法静态确定的解码处，按路径排序。
	Unresolved []UnresolvedPayloadSite
	// shapes 是类型表里全部类型的 JSON 结构指纹，排序去重。
	shapes []string
}

// PayloadIncompatibility 是 candidate 可能读不回 History 里已有载荷的一处：
//   - Unresolved 为 nil：Position 上按当前 Program 的类型 Current 写下的载荷在候选 Program 的类型 Candidate 下
//     读不回原值，Issues 是字段差异；
//   - Unresolved 不为 nil：UnresolvedIn（"current" 或 "candidate"）Program 里有一处位置或类型无法静态确定的解码，
//     且两份 Program 在这类载荷上的类型不同，检查无法证明载荷仍能读回；Position 是 Unresolved.Position。
type PayloadIncompatibility struct {
	Position     PayloadPosition
	Current      string
	Candidate    string
	Issues       []protocol.WireIncompatibility
	Unresolved   *UnresolvedPayloadSite
	UnresolvedIn string
}

func (i PayloadIncompatibility) String() string {
	if i.Unresolved != nil {
		kind := "payload"
		if i.Position.Kind != "" {
			kind = string(i.Position.Kind)
		}
		return fmt.Sprintf("%s program: %s; %s types differ between the two versions", i.UnresolvedIn, i.Unresolved, kind)
	}
	issues := make([]string, 0, len(i.Issues))
	for _, issue := range i.Issues {
		issues = append(issues, issue.String())
	}
	return fmt.Sprintf("%s: %s -> %s: %s", i.Position, i.Current, i.Candidate, strings.Join(issues, "; "))
}

// ComparePayloadTypes 对照 current 与 candidate 两份 Program 在每个载荷位置上解码用的类型，返回 candidate 可能读不回
// current 载荷的地方，按消息排序。
//
// 已确定的位置按 protocol.CompareWireTypes 判定。一个位置在 candidate 里可能有多处解码（同名 Signal 在两个分支里各收
// 一次）：candidate 的每个类型至少与 current 在该位置的一个类型兼容才算兼容。只有 candidate 解码的位置不判定（current
// 不读它，History 里的载荷来自 Program 之外）。
//
// 任一份 Program 有位置或类型无法确定的解码处（见 PayloadTypes）时按载荷种类保守判定：一个种类在两份 Program 里的
// 未确定解码处（名字、序号与类型的 JSON 结构）组成的多重集合不同，或其中有类型未知的解码处而两份类型表的 JSON 结构
// 集合不同，这个种类在两份 Program 里的全部未确定解码处都报告为不兼容；种类未知的解码处自成一类。类型的 JSON 结构
// 全部相同（例如只改了包名）时不报告。
func ComparePayloadTypes(current, candidate *WorkflowConfig) ([]PayloadIncompatibility, error) {
	currentTypes, err := PayloadTypes(current)
	if err != nil {
		return nil, fmt.Errorf("current program: %w", err)
	}
	candidateTypes, err := PayloadTypes(candidate)
	if err != nil {
		return nil, fmt.Errorf("candidate program: %w", err)
	}
	var out []PayloadIncompatibility
	for position, newTypes := range candidateTypes.Positions {
		oldTypes := currentTypes.Positions[position]
		if len(oldTypes) == 0 {
			continue
		}
		for _, newType := range newTypes {
			var first *PayloadIncompatibility
			compatible := false
			for _, oldType := range oldTypes {
				issues := protocol.CompareWireTypes(oldType.Wire, newType.Wire)
				if len(issues) == 0 {
					compatible = true
					break
				}
				if first == nil {
					first = &PayloadIncompatibility{Position: position, Current: oldType.Name, Candidate: newType.Name, Issues: issues}
				}
			}
			if !compatible {
				out = append(out, *first)
			}
		}
	}
	out = append(out, unresolvedIncompatibilities(currentTypes, candidateTypes)...)
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

// unresolvedIncompatibilities 是 ComparePayloadTypes 的保守判定。
func unresolvedIncompatibilities(current, candidate PayloadTypeSet) []PayloadIncompatibility {
	type signature struct {
		entries []string
		unknown bool
	}
	signatures := func(sites []UnresolvedPayloadSite) map[PayloadPositionKind]*signature {
		out := map[PayloadPositionKind]*signature{}
		for _, site := range sites {
			sig := out[site.Position.Kind]
			if sig == nil {
				sig = &signature{}
				out[site.Position.Kind] = sig
			}
			shape := "?"
			switch {
			case site.Wire != nil:
				shape = protocol.WireFingerprint(*site.Wire)
			case site.Type != "":
				shape = "type:" + site.Type
			default:
				sig.unknown = true
			}
			sig.entries = append(sig.entries, fmt.Sprintf("%s|%d|%s", site.Position.Name, site.Position.Index, shape))
		}
		for _, sig := range out {
			sort.Strings(sig.entries)
		}
		return out
	}
	currentSigs, candidateSigs := signatures(current.Unresolved), signatures(candidate.Unresolved)
	shapesEqual := slices.Equal(current.shapes, candidate.shapes)
	differs := func(kind PayloadPositionKind) bool {
		a, b := currentSigs[kind], candidateSigs[kind]
		if a == nil || b == nil {
			return true
		}
		return !slices.Equal(a.entries, b.entries) || ((a.unknown || b.unknown) && !shapesEqual)
	}
	var out []PayloadIncompatibility
	for _, side := range []struct {
		name  string
		sites []UnresolvedPayloadSite
	}{{"current", current.Unresolved}, {"candidate", candidate.Unresolved}} {
		for _, site := range side.sites {
			if differs(site.Position.Kind) {
				out = append(out, PayloadIncompatibility{Position: site.Position, Unresolved: &site, UnresolvedIn: side.name})
			}
		}
	}
	return out
}

// PayloadTypes 从 Program 的语句与类型表找出每个载荷位置解码用的类型：
//   - Workflow 输入：Entry.Input；
//   - Activity 结果、子 Workflow 结果、Signal、SideEffect / MutableSideEffect 的值：`Get` 的 out 变量的类型。future
//     是直接调用、保存它的变量、future 切片的元素、同包函数的 future 形参（按调用处的实参）或 Selector 回调的形参
//     （AddFuture / AddSignalFuture / AddChildFuture 加入的 future）；
//   - Activity 输入：ExecuteActivity 的输入实参的类型；
//   - Update 参数：SetUpdateHandler 闭包（或同包函数）的形参类型（跳过 workflow.Context）；
//   - 应用错误的 detail：ApplicationErrorDetail 的 out 变量的类型。
//
// 名字（Signal 名、Activity type、子 Workflow Type、MutableSideEffect ID、Update 名）取常量：字面量（Go 常量已由翻译器
// 折叠成字面量）、每处赋值都是同一常量的变量或形参、常量的 `+` 拼接、实参全是常量的 fmt.Sprintf。变量的类型取
// `var v T` / `v := T{...}` 的声明、形参类型、入口参数类型、同包函数的返回类型（含多返回值）、另一个变量的类型与
// 字段访问（`v.Field`）。变量按作用域查找：每个 fn 字面量（同包函数、闭包、回调）是一个作用域，`let` 在当前作用域
// 声明，`set` 赋给外层已声明的变量；同一作用域里同名变量各处的类型都算上。
//
// 位置或类型无法确定的解码处记入 Unresolved，ComparePayloadTypes 对它们保守判定。Selector 的 AddReceive 回调里
// Receive 的值来自 Workflow 内部的 Channel，不是 History 载荷。
func PayloadTypes(wf *WorkflowConfig) (PayloadTypeSet, error) {
	if wf == nil {
		return PayloadTypeSet{}, nil
	}
	reg := wlangregistry.New()
	if err := reg.BindDescriptors(wf.Program.Types); err != nil {
		return PayloadTypeSet{}, fmt.Errorf("bind type descriptors: %w", err)
	}
	scan := payloadScan{
		workflow: wf,
		reg:      reg,
		symbols:  wf.SymbolRefs(),
		parents:  []int{-1},
		vars:     map[varKey]*varInfo{},
		funcs:    map[varKey]string{},
		fnNodes:  map[string]map[string]any{},
		fnScopes: map[string]int{},
		out:      map[PayloadPosition][]PayloadType{},
	}
	// 生成器在内存里产出的语句含 Go 结构体，经存储读回后是 JSON 树；统一转成 JSON 树再遍历。
	raw, err := json.Marshal(wf.Program.Program)
	if err != nil {
		return PayloadTypeSet{}, fmt.Errorf("encode program: %w", err)
	}
	var statements any
	if err := json.Unmarshal(raw, &statements); err != nil {
		return PayloadTypeSet{}, fmt.Errorf("decode program: %w", err)
	}
	scan.walk(statements, "program", 0)
	if entry := wf.Program.Entry; entry.Input != "" {
		if entry.InputVar != "" {
			info := scan.declare(0, entry.InputVar)
			info.declared = append(info.declared, entry.Input)
		}
		scan.addSite(PayloadPosition{Kind: PayloadWorkflowInput}, []string{entry.Input}, "entry.input", "")
	}
	// 同包函数的形参按调用处的实参赋值。
	for _, call := range scan.calls {
		fnPath, ok := scan.lookupFunc(call.scope, call.fn)
		if !ok {
			continue
		}
		for i, name := range paramNames(scan.fnNodes[fnPath]) {
			if i < len(call.args) {
				info := scan.declare(scan.fnScopes[fnPath], name)
				info.assigns = append(info.assigns, scopedExpr{expr: call.args[i], scope: call.scope})
			}
		}
	}
	scan.resolveSites()
	sort.Slice(scan.unresolved, func(i, j int) bool { return scan.unresolved[i].String() < scan.unresolved[j].String() })
	return PayloadTypeSet{Positions: scan.out, Unresolved: scan.unresolved, shapes: scan.typeShapes()}, nil
}

// hostCall 是一次 workflow 包宿主函数调用：name 是函数名，args 去掉了 pkg 实参，从 ctx 开始。
type hostCall struct {
	name string
	args []any
}

// scopedExpr 是一个表达式与它所在的作用域。作用域按 fn 字面量划分：0 是顶层，每个 fn 字面量的形参与函数体是一个
// 子作用域。
type scopedExpr struct {
	expr  any
	scope int
}

// tupleItem 是多返回值赋值 `let [a, b] = expr` 里第 index 个名字的值。
type tupleItem struct {
	expr  any
	index int
}

type varKey struct {
	scope int
	name  string
}

// varInfo 是一个变量：每处赋值的表达式（同包函数的形参是每个调用处的实参）与声明类型（形参、入口参数）。
type varInfo struct {
	assigns  []scopedExpr
	declared []string
}

// decodeSite 是一处把载荷解码到 out 变量的调用：Get 的 future 表达式，或 ApplicationErrorDetail。
type decodeSite struct {
	future scopedExpr
	detail bool
	out    string
	scope  int
	path   string
}

type siteCall struct {
	call  hostCall
	scope int
	path  string
}

// fnCall 是对同包函数（`let __wlang_func_x = fn ...`）的一次调用。
type fnCall struct {
	fn    string
	args  []any
	scope int
}

type payloadScan struct {
	workflow *WorkflowConfig
	reg      *wlangregistry.Registry
	symbols  map[string]ActivityRef
	// parents 是每个作用域的外层作用域；vars 是各作用域里的变量；funcs 是 let 绑定 fn 字面量的变量 → fn 字面量的
	// 路径，fnNodes 与 fnScopes 按路径记录 fn 字面量与它的作用域。
	parents    []int
	vars       map[varKey]*varInfo
	funcs      map[varKey]string
	fnNodes    map[string]map[string]any
	fnScopes   map[string]int
	calls      []fnCall
	sites      []decodeSite
	activities []siteCall
	updates    []siteCall
	out        map[PayloadPosition][]PayloadType
	unresolved []UnresolvedPayloadSite
}

var futureHostCalls = map[string]bool{
	"ExecuteActivity": true, "ExecuteChildWorkflow": true, "WaitSignal": true, "SideEffect": true, "MutableSideEffect": true,
}

// selectorFutureCases 是 Selector 加入 future case 的方法；实参是 [selector, ctx, future, callback]。
var selectorFutureCases = map[string]bool{"AddFuture": true, "AddSignalFuture": true, "AddChildFuture": true}

// futureKinds 是 future 类型对应的载荷种类；SideEffectFuture 可能是 SideEffect 或 MutableSideEffect，种类未知。
var futureKinds = map[string]PayloadPositionKind{
	"workflow.SignalFuture": PayloadSignal, "workflow.ActivityFuture": PayloadActivityResult, "workflow.ChildWorkflowFuture": PayloadChildResult,
}

func (s *payloadScan) walk(node any, path string, scope int) {
	switch value := node.(type) {
	case []any:
		for i, item := range value {
			s.walk(item, fmt.Sprintf("%s[%d]", path, i), scope)
		}
	case map[string]any:
		for key, child := range value {
			childPath := path + "." + key
			switch key {
			case "fn":
				s.walkFunc(asMap(child), childPath, scope)
				continue
			case "let", "set":
				s.recordAssign(key, child, childPath, scope)
			case "call":
				call := asMap(child)
				if fn, ok := asMap(call["fn"])["var"].(string); ok {
					s.calls = append(s.calls, fnCall{fn: fn, args: asSlice(call["args"]), scope: scope})
				}
			case "Get", "ApplicationErrorDetail":
				args := asSlice(child)
				if len(args) < 2 {
					break
				}
				out, _ := asMap(args[len(args)-1])["out"].(string)
				if out == "" {
					break
				}
				site := decodeSite{out: out, scope: scope, path: childPath, detail: key == "ApplicationErrorDetail"}
				if !site.detail {
					site.future = scopedExpr{expr: args[0], scope: scope}
				}
				s.sites = append(s.sites, site)
			case "ExecuteActivity":
				s.activities = append(s.activities, siteCall{call: hostCall{name: key, args: hostArgs(child)}, scope: scope, path: childPath})
			case "SetUpdateHandler":
				s.updates = append(s.updates, siteCall{call: hostCall{name: key, args: hostArgs(child)}, scope: scope, path: childPath})
			}
			s.walk(child, childPath, scope)
			// Selector 的 future case：回调的第一个形参就是加入的 future。
			if args := asSlice(child); selectorFutureCases[key] && len(args) == 4 {
				fnPath := fmt.Sprintf("%s[3].fn", childPath)
				if names := paramNames(s.fnNodes[fnPath]); len(names) > 0 {
					info := s.declare(s.fnScopes[fnPath], names[0])
					info.assigns = append(info.assigns, scopedExpr{expr: args[2], scope: scope})
				}
			}
		}
	}
}

// walkFunc 为 fn 字面量开一个子作用域，声明形参后遍历函数体。
func (s *payloadScan) walkFunc(fn map[string]any, path string, parent int) {
	scope := len(s.parents)
	s.parents = append(s.parents, parent)
	s.fnNodes[path], s.fnScopes[path] = fn, scope
	for _, param := range asSlice(fn["params"]) {
		if pair := asSlice(param); len(pair) == 2 {
			name, _ := pair[0].(string)
			typeName, _ := pair[1].(string)
			if name != "" && typeName != "" {
				info := s.declare(scope, name)
				info.declared = append(info.declared, typeName)
			}
		}
	}
	for key, child := range fn {
		if key != "params" {
			s.walk(child, path+"."+key, scope)
		}
	}
}

// recordAssign 记录赋值：`{name: expr}` 或多返回值的 `[[a, b], expr]`。let 在当前作用域声明变量，set 赋给外层
// 已声明的同名变量。
func (s *payloadScan) recordAssign(key string, child any, path string, scope int) {
	target := func(name string) *varInfo {
		if key == "set" {
			if info, ok := s.lookup(scope, name); ok {
				return info
			}
		}
		return s.declare(scope, name)
	}
	if pair := asSlice(child); len(pair) == 2 {
		for index, name := range asSlice(pair[0]) {
			if name, _ := name.(string); name != "" {
				info := target(name)
				info.assigns = append(info.assigns, scopedExpr{expr: tupleItem{expr: pair[1], index: index}, scope: scope})
			}
		}
		return
	}
	for name, expr := range asMap(child) {
		info := target(name)
		info.assigns = append(info.assigns, scopedExpr{expr: expr, scope: scope})
		if _, ok := asMap(expr)["fn"]; ok && key == "let" {
			s.funcs[varKey{scope: scope, name: name}] = path + "." + name + ".fn"
		}
	}
}

func (s *payloadScan) declare(scope int, name string) *varInfo {
	key := varKey{scope: scope, name: name}
	if s.vars[key] == nil {
		s.vars[key] = &varInfo{}
	}
	return s.vars[key]
}

// lookup 从 scope 向外找名为 name 的变量。
func (s *payloadScan) lookup(scope int, name string) (*varInfo, bool) {
	for ; scope >= 0; scope = s.parents[scope] {
		if info, ok := s.vars[varKey{scope: scope, name: name}]; ok {
			return info, true
		}
	}
	return nil, false
}

// lookupFunc 从 scope 向外找 let 绑定的同包函数，返回 fn 字面量的路径。
func (s *payloadScan) lookupFunc(scope int, name string) (string, bool) {
	for ; scope >= 0; scope = s.parents[scope] {
		if path, ok := s.funcs[varKey{scope: scope, name: name}]; ok {
			return path, true
		}
	}
	return "", false
}

// resolveSites 在遍历完成后为每处解码确定位置与类型。
func (s *payloadScan) resolveSites() {
	for _, activity := range s.activities {
		args := activity.call.args
		if len(args) < 3 {
			continue
		}
		position := PayloadPosition{Kind: PayloadActivityInput, Name: s.activityType(args, activity.scope)}
		reason := ""
		if position.Name == "" {
			reason = "the activity is not a registered function or a constant type"
		}
		s.addSite(position, s.exprTypes(scopedExpr{expr: args[2], scope: activity.scope}, nil), activity.path+"[3]", reason)
	}
	for _, site := range s.sites {
		types := s.varTypes(site.out, site.scope, nil)
		if site.detail {
			s.addSite(PayloadPosition{Kind: PayloadErrorDetail}, types, site.path, "")
			continue
		}
		calls, traced := s.futureCalls(site.future, nil)
		for _, call := range calls {
			position, reason := s.resultPosition(call)
			s.addSite(position, types, site.path, reason)
		}
		if !traced {
			var kind PayloadPositionKind
			for _, typeName := range s.exprTypes(site.future, nil) {
				if kind == "" {
					kind = futureKinds[typeName]
				}
			}
			s.addSite(PayloadPosition{Kind: kind}, types, site.path, "the future cannot be traced to the call that created it")
		}
	}
	for _, update := range s.updates {
		args := update.call.args
		if len(args) < 3 {
			continue
		}
		name, ok := s.constString(scopedExpr{expr: args[1], scope: update.scope}, nil)
		reason := ""
		if !ok {
			reason = "the update name is not a constant"
		}
		fn := asMap(asMap(args[2])["fn"])
		if handler, ok := asMap(args[2])["var"].(string); ok && fn == nil {
			if path, ok := s.lookupFunc(update.scope, handler); ok {
				fn = s.fnNodes[path]
			}
		}
		if fn == nil {
			s.addSite(PayloadPosition{Kind: PayloadUpdateArgument, Name: name}, nil, update.path+"[3]", "the update handler is not a function literal or a same-package function")
			continue
		}
		index := 0
		for _, param := range asSlice(fn["params"]) {
			pair := asSlice(param)
			if len(pair) != 2 {
				continue
			}
			typeName, _ := pair[1].(string)
			if typeName == "workflow.Context" {
				continue
			}
			s.addSite(PayloadPosition{Kind: PayloadUpdateArgument, Name: name, Index: index}, []string{typeName}, update.path+"[3]", reason)
			index++
		}
	}
}

// addSite 记录一处解码：reason 为空、类型都有 JSON 描述时计入 out，否则记为未确定的解码处。
func (s *payloadScan) addSite(position PayloadPosition, typeNames []string, path, reason string) {
	if len(typeNames) == 0 {
		if reason == "" {
			reason = "the type of the decode target cannot be determined"
		}
		s.unresolved = append(s.unresolved, UnresolvedPayloadSite{Position: position, Path: path, Reason: reason})
		return
	}
	for _, typeName := range typeNames {
		site := UnresolvedPayloadSite{Position: position, Type: s.displayName(typeName), Path: path, Reason: reason}
		wire, ok := s.resolve(typeName)
		if ok {
			site.Wire = &wire
		} else if site.Reason == "" {
			site.Reason = "type " + site.Type + " has no JSON description"
		}
		if site.Reason != "" {
			s.unresolved = append(s.unresolved, site)
			continue
		}
		s.addResolved(position, site.Type, wire)
	}
}

func (s *payloadScan) addResolved(position PayloadPosition, typeName string, wire protocol.WireType) {
	for _, existing := range s.out[position] {
		if existing.Name == typeName {
			return
		}
	}
	s.out[position] = append(s.out[position], PayloadType{Name: typeName, Wire: wire})
}

// resultPosition 是 call 的 future 被 Get 时载荷所在的位置；名字不是常量时 reason 说明原因，位置只有 Kind。call 的
// 实参按 call 所在的作用域求值，由 futureCalls 记在 call.scope。
func (s *payloadScan) resultPosition(call scopedCall) (PayloadPosition, string) {
	switch call.name {
	case "ExecuteActivity":
		if activityType := s.activityType(call.args, call.scope); activityType != "" {
			return PayloadPosition{Kind: PayloadActivityResult, Name: activityType}, ""
		}
		return PayloadPosition{Kind: PayloadActivityResult}, "the activity is not a registered function or a constant type"
	case "SideEffect":
		return PayloadPosition{Kind: PayloadSideEffect}, ""
	}
	kind := map[string]PayloadPositionKind{
		"ExecuteChildWorkflow": PayloadChildResult, "WaitSignal": PayloadSignal, "MutableSideEffect": PayloadMutableSideEffect,
	}[call.name]
	if len(call.args) >= 2 {
		if name, ok := s.constString(scopedExpr{expr: call.args[1], scope: call.scope}, nil); ok {
			return PayloadPosition{Kind: kind, Name: name}, ""
		}
	}
	return PayloadPosition{Kind: kind}, "the " + string(kind) + " name is not a constant"
}

// scopedCall 是一次宿主调用与它所在的作用域。
type scopedCall struct {
	hostCall
	scope int
}

// activityType 取 ExecuteActivity 第一个业务实参指向的 Activity type：符号按 Activities 映射，常量字符串即 type。
func (s *payloadScan) activityType(args []any, scope int) string {
	if len(args) < 2 {
		return ""
	}
	if symbol, ok := asMap(args[1])["symbol"].(string); ok {
		return s.symbols[symbol].Type
	}
	name, _ := s.constString(scopedExpr{expr: args[1], scope: scope}, nil)
	return name
}

// futureCalls 把 future 表达式追溯到创建它的宿主调用：直接调用、变量（每处赋值）、future 切片的元素。traced 为
// false 表示至少有一个来源追溯不到。
func (s *payloadScan) futureCalls(e scopedExpr, seen map[*varInfo]bool) (calls []scopedCall, traced bool) {
	if call, ok := asHostCall(e.expr); ok && futureHostCalls[call.name] {
		return []scopedCall{{hostCall: call, scope: e.scope}}, true
	}
	node := asMap(e.expr)
	if items, ok := asMap(node["array"])["items"].([]any); ok {
		traced = true
		for _, item := range items {
			found, ok := s.futureCalls(scopedExpr{expr: item, scope: e.scope}, seen)
			calls, traced = append(calls, found...), traced && ok
		}
		return calls, traced
	}
	if args := asSlice(node["arr.get"]); len(args) > 0 {
		return s.futureCalls(scopedExpr{expr: args[0], scope: e.scope}, seen)
	}
	name, _ := node["var"].(string)
	info, ok := s.lookup(e.scope, name)
	if !ok || seen[info] || len(info.assigns) == 0 {
		return nil, false
	}
	seen = with(seen, info)
	traced = true
	for _, assigned := range info.assigns {
		found, ok := s.futureCalls(assigned, seen)
		calls, traced = append(calls, found...), traced && ok
	}
	return calls, traced
}

// exprTypes 是表达式的类型名：struct 字面量、零值、切片字面量、字面量、变量、同包函数调用的返回值。
func (s *payloadScan) exprTypes(e scopedExpr, seen map[*varInfo]bool) []string {
	if item, ok := e.expr.(tupleItem); ok {
		if returns := s.callReturns(scopedExpr{expr: item.expr, scope: e.scope}); item.index < len(returns) {
			return []string{returns[item.index]}
		}
		return nil
	}
	if typeName := declaredType(e.expr); typeName != "" {
		return []string{typeName}
	}
	node := asMap(e.expr)
	if elem, ok := asMap(node["array"])["elem"].(string); ok {
		return []string{"array<" + elem + ">"}
	}
	if literal := asMap(node["literal"]); literal != nil {
		if typeName, _ := literal["type"].(string); typeName != "null" && typeName != "" {
			return []string{typeName}
		}
		return nil
	}
	if args := asSlice(node["arr.get"]); len(args) > 0 {
		var elems []string
		for _, typeName := range s.exprTypes(scopedExpr{expr: args[0], scope: e.scope}, seen) {
			if elem, ok := genericBody(typeName, "array<"); ok {
				elems = append(elems, elem)
			}
		}
		return elems
	}
	if name, ok := node["var"].(string); ok {
		return s.varTypes(name, e.scope, seen)
	}
	if returns := s.callReturns(e); len(returns) > 0 {
		return returns[:1]
	}
	return nil
}

// callReturns 是同包函数调用的声明返回类型。
func (s *payloadScan) callReturns(e scopedExpr) []string {
	name, _ := asMap(asMap(asMap(e.expr)["call"])["fn"])["var"].(string)
	path, ok := s.lookupFunc(e.scope, name)
	if !ok {
		return nil
	}
	var returns []string
	for _, item := range asSlice(s.fnNodes[path]["returns"]) {
		typeName, _ := item.(string)
		returns = append(returns, typeName)
	}
	return returns
}

// varTypes 是变量路径（`v` 或 `v.Field.Sub`）的类型名：变量的声明类型与每处赋值的类型，再逐级取字段类型。
func (s *payloadScan) varTypes(path string, scope int, seen map[*varInfo]bool) []string {
	name, fields, _ := strings.Cut(path, ".")
	info, ok := s.lookup(scope, name)
	if !ok || seen[info] {
		return nil
	}
	seen = with(seen, info)
	var types []string
	add := func(typeName string) {
		if typeName != "" && !slices.Contains(types, typeName) {
			types = append(types, typeName)
		}
	}
	for _, typeName := range info.declared {
		add(typeName)
	}
	for _, assigned := range info.assigns {
		for _, typeName := range s.exprTypes(assigned, seen) {
			add(typeName)
		}
	}
	if fields == "" {
		return types
	}
	var out []string
	for _, typeName := range types {
		if fieldType := s.fieldType(typeName, strings.Split(fields, ".")); fieldType != "" && !slices.Contains(out, fieldType) {
			out = append(out, fieldType)
		}
	}
	return out
}

// fieldType 沿 Go 字段名逐级取 record 字段的类型 ID（经过指针与别名）。
func (s *payloadScan) fieldType(typeName string, fields []string) string {
	for _, field := range fields {
		id, ok := s.reg.DescriptorID(strings.TrimPrefix(typeName, "*"))
		if !ok {
			return ""
		}
		descriptor := s.workflow.Program.Types[id]
		for descriptor.Kind == "alias" || descriptor.Kind == "pointer" {
			next := descriptor.Underlying
			if descriptor.Kind == "pointer" {
				next = descriptor.Elem
			}
			descriptor = s.workflow.Program.Types[next]
		}
		typeName = ""
		for _, candidate := range descriptor.Fields {
			if candidate.GoName == field {
				typeName = candidate.TypeID
			}
		}
		if typeName == "" {
			return ""
		}
	}
	return typeName
}

// constString 把表达式折叠成字符串常量。
func (s *payloadScan) constString(e scopedExpr, seen map[*varInfo]bool) (string, bool) {
	value, ok := s.constValue(e, seen)
	text, isString := value.(string)
	return text, ok && isString
}

// constValue 把表达式折叠成常量：字面量、每处赋值都是同一常量的变量（含每个调用处实参都相同的形参）、字符串的
// `+`、实参全是常量的 fmt.Sprintf。
func (s *payloadScan) constValue(e scopedExpr, seen map[*varInfo]bool) (any, bool) {
	node := asMap(e.expr)
	if literal := asMap(node["literal"]); literal != nil {
		text, _ := literal["value"].(string)
		switch literal["type"] {
		case "string":
			return text, true
		case "boolean":
			return text == "true", true
		case "int64":
			value, err := strconv.ParseInt(text, 10, 64)
			return value, err == nil
		case "float64":
			value, err := strconv.ParseFloat(text, 64)
			return value, err == nil
		}
		return nil, false
	}
	if operands := asSlice(node["+"]); len(operands) == 2 {
		left, leftOK := s.constString(scopedExpr{expr: operands[0], scope: e.scope}, seen)
		right, rightOK := s.constString(scopedExpr{expr: operands[1], scope: e.scope}, seen)
		return left + right, leftOK && rightOK
	}
	if call, ok := asHostCall(e.expr); ok && call.name == "Sprintf" && asMap(asSlice(node["Sprintf"])[0])["pkg"] == "fmt" && len(call.args) > 0 {
		format, ok := s.constString(scopedExpr{expr: call.args[0], scope: e.scope}, seen)
		if !ok {
			return nil, false
		}
		values := make([]any, 0, len(call.args)-1)
		for _, arg := range call.args[1:] {
			value, ok := s.constValue(scopedExpr{expr: arg, scope: e.scope}, seen)
			if !ok {
				return nil, false
			}
			values = append(values, value)
		}
		return fmt.Sprintf(format, values...), true
	}
	name, _ := node["var"].(string)
	info, ok := s.lookup(e.scope, name)
	if !ok || seen[info] || len(info.assigns) == 0 {
		return nil, false
	}
	seen = with(seen, info)
	var value any
	for i, assigned := range info.assigns {
		next, ok := s.constValue(assigned, seen)
		if !ok || (i > 0 && next != value) {
			return nil, false
		}
		value = next
	}
	return value, true
}

// paramNames 是 fn 字面量的形参名。
func paramNames(fn map[string]any) []string {
	var names []string
	for _, param := range asSlice(fn["params"]) {
		pair := asSlice(param)
		if len(pair) != 2 {
			return nil
		}
		name, _ := pair[0].(string)
		names = append(names, name)
	}
	return names
}

// displayName 是消息里的类型名：类型表里的 ID 换成语言名。
func (s *payloadScan) displayName(typeName string) string {
	if descriptor, ok := s.workflow.Program.Types[typeName]; ok && descriptor.LanguageName != "" {
		return descriptor.LanguageName
	}
	return typeName
}

// typeShapes 是类型表里全部类型的 JSON 结构指纹，排序去重；没有 JSON 描述的类型不计入。
func (s *payloadScan) typeShapes() []string {
	var shapes []string
	for id := range s.workflow.Program.Types {
		if wire, err := resolveWireType(id, s.workflow.Program.Types, nil); err == nil {
			shapes = append(shapes, protocol.WireFingerprint(wire))
		}
	}
	sort.Strings(shapes)
	return slices.Compact(shapes)
}

// resolve 把 WLang 类型名转成 wire 结构：类型表里的名字按 registry 的规则找到描述符，内置标量、`array<T>`、
// `map<K,V>` 与 `*T` 按结构展开。
func (s *payloadScan) resolve(typeName string) (protocol.WireType, bool) {
	if scalar, ok := builtinWireScalar(typeName); ok {
		return scalar, true
	}
	if id, ok := s.reg.DescriptorID(typeName); ok {
		wire, err := resolveWireType(id, s.workflow.Program.Types, nil)
		return wire, err == nil
	}
	if elem, ok := strings.CutPrefix(typeName, "*"); ok {
		inner, ok := s.resolve(elem)
		if !ok {
			return protocol.WireType{}, false
		}
		return protocol.WireType{Kind: protocol.WireKindPointer, Elem: &inner, Optional: true}, true
	}
	if body, ok := genericBody(typeName, "array<"); ok {
		inner, ok := s.resolve(body)
		if !ok {
			return protocol.WireType{}, false
		}
		return protocol.WireType{Kind: protocol.WireKindSlice, Elem: &inner}, true
	}
	if body, ok := genericBody(typeName, "map<"); ok {
		keyName, elemName, ok := splitTopLevelComma(body)
		if !ok {
			return protocol.WireType{}, false
		}
		key, keyOK := s.resolve(keyName)
		elem, elemOK := s.resolve(elemName)
		if !keyOK || !elemOK {
			return protocol.WireType{}, false
		}
		return protocol.WireType{Kind: protocol.WireKindMap, Key: &key, Elem: &elem}, true
	}
	return protocol.WireType{}, false
}

// with 返回加入 info 的 seen 副本，用于在递归里截断变量之间的环。
func with(seen map[*varInfo]bool, info *varInfo) map[*varInfo]bool {
	out := make(map[*varInfo]bool, len(seen)+1)
	for key := range seen {
		out[key] = true
	}
	out[info] = true
	return out
}

func genericBody(typeName, prefix string) (string, bool) {
	if !strings.HasPrefix(typeName, prefix) || !strings.HasSuffix(typeName, ">") {
		return "", false
	}
	body := typeName[len(prefix) : len(typeName)-1]
	return body, body != ""
}

func splitTopLevelComma(body string) (string, string, bool) {
	depth := 0
	for i, char := range body {
		switch char {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 && i > 0 && i < len(body)-1 {
				return body[:i], body[i+1:], true
			}
		}
	}
	return "", "", false
}

// declaredType 是 `zero T` 与 `struct [T, {...}]` 表达式的类型名。
func declaredType(expr any) string {
	node := asMap(expr)
	if typeName, ok := node["zero"].(string); ok {
		return typeName
	}
	if parts := asSlice(node["struct"]); len(parts) > 0 {
		typeName, _ := parts[0].(string)
		return typeName
	}
	return ""
}

// asHostCall 识别 `{"Name": [{"pkg": ...}, ctx, args...]}` 形式的宿主函数调用。
func asHostCall(expr any) (hostCall, bool) {
	node := asMap(expr)
	if len(node) != 1 {
		return hostCall{}, false
	}
	for name, child := range node {
		args := asSlice(child)
		if len(args) == 0 {
			return hostCall{}, false
		}
		if _, ok := asMap(args[0])["pkg"]; !ok {
			return hostCall{}, false
		}
		return hostCall{name: name, args: hostArgs(child)}, true
	}
	return hostCall{}, false
}

// hostArgs 去掉宿主函数调用的 pkg 实参，保留 ctx 及之后的实参。
func hostArgs(child any) []any {
	args := asSlice(child)
	if len(args) > 0 {
		if _, ok := asMap(args[0])["pkg"]; ok {
			return args[1:]
		}
	}
	return args
}

func asMap(value any) map[string]any {
	node, _ := value.(map[string]any)
	return node
}

func asSlice(value any) []any {
	items, _ := value.([]any)
	return items
}
