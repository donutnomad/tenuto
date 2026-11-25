package sdkgen

import (
	"cmp"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"

	"github.com/donutnomad/wlang/go2wlang"
	wlangtypes "github.com/donutnomad/wlang/types"
	"github.com/donutnomad/wlang/wflang"

	programpkg "github.com/donutnomad/tenuto/program"
)

type Options struct {
	WorkflowType string
	Name         string
	FuncName     string
	Filename     string
	Dir          string
}

// Result 是一次翻译的产物：登记用的 WorkflowConfig 与格式化后的 WLang 程序 JSON。
type Result struct {
	Workflow    *programpkg.WorkflowConfig
	ProgramJSON []byte
}

func GenerateFromSource(src []byte, opts Options) (Result, error) {
	if opts.FuncName == "" {
		return Result{}, fmt.Errorf("func name is required")
	}
	sources, err := collectWorkflowSources(src, opts.Filename, opts.Dir, opts.FuncName, nil)
	if err != nil {
		return Result{}, err
	}
	return generateWithExtraTypes(sources, opts, nil)
}

// workflowSources 是翻译的输入：workflow 函数与它调用的同包函数（funcs，workflow 函数在首位），
// 以及它们所在文件的内容（files，按文件路径）。
type workflowSources struct {
	files map[string][]byte
	funcs []helperFunc
}

// collectWorkflowSources 找出 workflow 函数调用的同包函数，并读入它们所在的文件。
func collectWorkflowSources(src []byte, filename, dir, funcName string, activityImpls map[string]bool) (workflowSources, error) {
	helpers, err := workflowHelpers(src, filename, dir, funcName, activityImpls)
	if err != nil {
		return workflowSources{}, err
	}
	filename = filepath.Clean(filename)
	sources := workflowSources{
		files: map[string][]byte{filename: src},
		funcs: []helperFunc{{file: filename, key: funcName}},
	}
	for _, helper := range helpers {
		helper.file = filepath.Clean(helper.file)
		if _, ok := sources.files[helper.file]; !ok {
			content, err := os.ReadFile(helper.file)
			if err != nil {
				return workflowSources{}, err
			}
			sources.files[helper.file] = content
		}
		sources.funcs = append(sources.funcs, helper)
	}
	return sources, nil
}

// hostPackages 是 Worker 绑定的非标准库包：Workflow 代码只能调用这些包的函数与方法，
// 以及 wlang 绑定的标准库函数。time 包的方法指 workflow.Now 返回的 time.Time 值的方法。
var hostPackages = []string{workflowSDKPath, "time"}

// generateWithExtraTypes 把 activity 契约的类型描述符并入产物 Types 后再算内容版本。注解
// 生成路径需要它：activity 的 Input/Output 类型不一定出现在 workflow 入口签名里，必须显式
// 补齐才能做契约对照。
func generateWithExtraTypes(sources workflowSources, opts Options, extraTypes map[string]programpkg.WLangTypeDescriptor) (Result, error) {
	if opts.WorkflowType == "" {
		return Result{}, fmt.Errorf("workflow type is required")
	}
	if opts.FuncName == "" {
		return Result{}, fmt.Errorf("func name is required")
	}
	for name, content := range sources.files {
		canonical, err := canonicalWorkflowImport(content, name)
		if err != nil {
			return Result{}, err
		}
		sources.files[name] = canonical
	}
	for _, fn := range sources.funcs {
		lowered, err := lowerWorkflowSource(sources.files[fn.file], fn.file, fn.key)
		if err != nil {
			return Result{}, err
		}
		sources.files[fn.file] = lowered
	}
	workflowFile := filepath.Clean(opts.Filename)
	src := sources.files[workflowFile]
	overlay := map[string][]byte{}
	for name, content := range sources.files {
		if name != workflowFile {
			overlay[name] = content
		}
	}
	translated, err := go2wlang.TranslateFileDetailed(src, go2wlang.Options{
		FuncName:         opts.FuncName,
		Filename:         opts.Filename,
		Dir:              opts.Dir,
		EmbedImportMap:   true,
		Overlay:          overlay,
		TranslateHelpers: true,
		HostPackages:     hostPackages,
	})
	if err != nil {
		return Result{}, err
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, opts.Filename, src, 0)
	if err != nil {
		return Result{}, err
	}
	fn := findFuncDecl(file, opts.FuncName)
	if fn == nil {
		return Result{}, fmt.Errorf("function %s not found", opts.FuncName)
	}
	// 没有导入 sdk/workflow 的源码按默认包名扫描。
	wfAlias := cmp.Or(workflowImportAlias(file), "workflow")

	envelope, programJSON, err := foldTranslation(translated)
	if err != nil {
		return Result{}, err
	}

	// update handler 由 engine 的 handler 协程执行（spawnUpdateHandlers 在 UpdateAccepted 所属批
	// spawn）；这里只校验 handler 的源码形态。
	if err := scanUpdateHandler(fset, fn, wfAlias); err != nil {
		return Result{}, err
	}
	if err := checkWorkflowInput(fset, fn, translated); err != nil {
		return Result{}, err
	}

	importMap := envelope.ImportMap
	if len(importMap) == 0 {
		importMap = map[string]string{}
		for _, name := range envelope.Imports {
			importMap[name] = name
		}
	}
	artifact := programpkg.WLangProgram{
		Schema:  programpkg.WLangProgramSchemaV2,
		Lang:    programpkg.WLangLanguageV1,
		Program: envelope.Program,
		Imports: importMap,
		Types:   translated.Types,
		// Entry 描述入口的类型边界。非 typed workflow（标量入参、无入参、
		// 纯 error 返回）这里是零值，artifact 序列化后逐字节不变（omitzero），
		// 版本号也就不会翻转。
		Entry: programpkg.WLangEntryDescriptor{
			InputVar: translated.Entry.InputVar,
			Input:    translated.Entry.Input,
			Output:   translated.Entry.Output,
		},
		HandlerABISchema: programpkg.WLangHandlerABISchemaV1,
	}
	for id, descriptor := range extraTypes {
		if artifact.Types == nil {
			artifact.Types = make(map[string]programpkg.WLangTypeDescriptor, len(extraTypes))
		}
		artifact.Types[id] = descriptor
	}
	if workflowUsesGetInfo(fn, wfAlias) {
		if artifact.Types == nil {
			artifact.Types = make(map[string]programpkg.WLangTypeDescriptor)
		}
		addWorkflowInfoDescriptors(artifact.Types)
	}
	if err := artifact.ValidateArtifact(); err != nil {
		return Result{}, err
	}
	// Activity 引用表来自翻译产物：产物里有什么符号，运行期就解析什么符号。
	workflow := &programpkg.WorkflowConfig{
		Type:       opts.WorkflowType,
		Name:       opts.Name,
		Program:    artifact,
		Activities: programActivities(envelope.Program),
	}
	return Result{Workflow: workflow, ProgramJSON: programJSON}, nil
}

// checkWorkflowInput 要求 Workflow 的入参是 JSON object：struct（或其指针）、map[string]any，
// 或其他包的 struct / map 命名类型。Run 的 Input 在线上是 JSON object，标量、切片与 any 入参
// 在运行时绑不上，生成时报出位置。
func checkWorkflowInput(fset *token.FileSet, fn *ast.FuncDecl, translated *go2wlang.Result) error {
	entry := translated.Entry
	if entry.InputVar == "" || fn.Type.Params == nil || len(fn.Type.Params.List) < 2 {
		return nil
	}
	expr := fn.Type.Params.List[1].Type
	if entry.Input == "" {
		if isStringAnyMap(expr) {
			return nil
		}
	} else {
		// 指针与命名类型逐层展开到实际的数据形状；层数不超过描述符个数。
		descriptor := translated.Types[entry.Input]
		for range len(translated.Types) {
			if descriptor.Kind == wlangtypes.TypeKindPointer {
				descriptor = translated.Types[descriptor.Elem]
			} else if descriptor.Kind == wlangtypes.TypeKindAlias {
				descriptor = translated.Types[descriptor.Underlying]
			}
		}
		if descriptor.Kind == wlangtypes.TypeKindRecord || descriptor.Kind == wlangtypes.TypeKindMap {
			return nil
		}
	}
	return fmt.Errorf("%s: Workflow %s 的输入类型 %s 不是 JSON object；Workflow 的输入必须是 struct（或其指针）或 map[string]any，单个值请包进 struct 的字段",
		fset.Position(expr.Pos()), fn.Name.Name, types.ExprString(expr))
}

// translationEnvelope 是 go2wlang 产物里生成器关心的部分。
type translationEnvelope struct {
	Program   []any
	Imports   []string
	ImportMap map[string]string
}

// foldTranslation 对翻译产物做编译期折叠（见 foldProgram），并返回折叠后的 JSON。
// 折叠后程序不再引用 time 包，import 表里的 time 一并去掉。
func foldTranslation(translated *go2wlang.Result) (translationEnvelope, []byte, error) {
	var raw map[string]any
	if err := json.Unmarshal(translated.JSON, &raw); err != nil {
		return translationEnvelope{}, nil, err
	}
	timeAliases := map[string]bool{}
	envelope := translationEnvelope{ImportMap: map[string]string{}}
	for alias, path := range translated.Imports {
		if path == "time" {
			timeAliases[alias] = true
			continue
		}
		envelope.ImportMap[alias] = path
	}
	if len(translated.Imports) == 0 {
		timeAliases["time"] = true
	}
	program, _ := raw["program"].([]any)
	envelope.Program, _ = foldProgram(program, timeAliases).([]any)
	raw["program"] = envelope.Program
	imports, _ := raw["imports"].([]any)
	kept := make([]any, 0, len(imports))
	for _, item := range imports {
		name, _ := item.(string)
		if timeAliases[name] {
			continue
		}
		kept = append(kept, item)
		envelope.Imports = append(envelope.Imports, name)
	}
	if len(kept) > 0 {
		raw["imports"] = kept
	} else {
		delete(raw, "imports")
	}
	if _, ok := raw["importMap"]; ok {
		if len(envelope.ImportMap) > 0 {
			raw["importMap"] = envelope.ImportMap
		} else {
			delete(raw, "importMap")
		}
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return translationEnvelope{}, nil, err
	}
	formatted, err := wflang.FormatJSON(encoded)
	if err != nil {
		return translationEnvelope{}, nil, err
	}
	return envelope, formatted, nil
}

// workflowImportAlias 返回 file 引用 sdk/workflow 包时使用的名字；文件没有导入该包时返回空串。
func workflowImportAlias(file *ast.File) string {
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil || path != workflowSDKPath {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return "workflow"
	}
	return ""
}

// canonicalWorkflowImport 把以别名导入的 sdk/workflow 改写成默认包名 workflow：Worker 按包名 workflow
// 绑定 SDK，产物里的包引用必须是这个名字。只替换标识符，行号不变，后续诊断仍指向作者的源码行。
// 文件里另有名为 workflow 的标识符时改写会改变语义，按 file:line 拒绝。
func canonicalWorkflowImport(src []byte, filename string) ([]byte, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	alias := workflowImportAlias(file)
	if alias == "" || alias == "workflow" || alias == "_" || alias == "." {
		return src, nil
	}
	var spec *ast.ImportSpec
	for _, candidate := range file.Imports {
		if candidate.Name != nil && candidate.Name.Name == alias {
			spec = candidate
		}
	}
	var conflict *ast.Ident
	idents := []*ast.Ident{spec.Name}
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.Ident:
			if node.Name == "workflow" && conflict == nil {
				conflict = node
			}
		case *ast.SelectorExpr:
			// 指向导入包的标识符没有本地声明（Obj 为 nil）；同名的局部变量不改。
			if root, ok := node.X.(*ast.Ident); ok && root.Name == alias && root.Obj == nil {
				idents = append(idents, root)
			}
		}
		return true
	})
	if conflict != nil {
		return nil, fmt.Errorf("%s: sdk/workflow 以别名 %s 导入，文件里的标识符 workflow 与 Worker 绑定的包名冲突；把这个标识符改名，或按默认名 workflow 导入",
			fset.Position(conflict.Pos()), alias)
	}
	out := append([]byte(nil), src...)
	slices.SortFunc(idents, func(a, b *ast.Ident) int { return int(b.Pos() - a.Pos()) })
	for _, ident := range idents {
		start := fset.Position(ident.Pos()).Offset
		out = append(out[:start:start], append([]byte("workflow"), out[start+len(alias):]...)...)
	}
	return out, nil
}

// workflowCallName 返回 expr 调用的 sdk/workflow 包函数名；expr 是其他表达式时返回空串。
func workflowCallName(expr ast.Expr, wfAlias string) string {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return ""
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	root, ok := sel.X.(*ast.Ident)
	if !ok || root.Name != wfAlias {
		return ""
	}
	return sel.Sel.Name
}

func workflowUsesGetInfo(fn *ast.FuncDecl, wfAlias string) bool {
	used := false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && workflowCallName(call, wfAlias) == "GetInfo" {
			used = true
		}
		return !used
	})
	return used
}

func addWorkflowInfoDescriptors(descriptors map[string]programpkg.WLangTypeDescriptor) {
	const (
		infoID   = "github.com/donutnomad/tenuto/sdk/workflow.Info"
		reasonID = "github.com/donutnomad/tenuto/sdk/workflow.ContinueAsNewReason"
		sliceID  = "[]" + reasonID
	)
	items := []wlangtypes.TypeDescriptor{
		{ID: reasonID, LanguageName: "workflow.ContinueAsNewReason", Kind: wlangtypes.TypeKindAlias, Underlying: wlangtypes.TString},
		{ID: sliceID, LanguageName: "[]workflow.ContinueAsNewReason", Kind: wlangtypes.TypeKindSlice, Elem: reasonID},
		{ID: infoID, LanguageName: "workflow.Info", Kind: wlangtypes.TypeKindRecord, Fields: []wlangtypes.FieldDescriptor{
			{GoName: "CurrentHistoryLength", JSONName: "CurrentHistoryLength", TypeID: wlangtypes.TInt64},
			{GoName: "CurrentHistorySize", JSONName: "CurrentHistorySize", TypeID: wlangtypes.TInt64},
			{GoName: "ContinueAsNewReasons", JSONName: "ContinueAsNewReasons", TypeID: sliceID},
		}},
	}
	for _, descriptor := range items {
		descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
		descriptors[descriptor.ID] = descriptor
	}
}

// scanUpdateHandler validates the persisted closure ABI before a program can
// enter the definition store. The engine repeats the same checks when loading
// an artifact because historical programs can bypass sdkgen.
func scanUpdateHandler(fset *token.FileSet, fn *ast.FuncDecl, wfAlias string) error {
	var validationErr error
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if validationErr != nil {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var err error
		switch workflowCallName(call, wfAlias) {
		case "SetUpdateHandler":
			err = validateUpdateHandlerCall(call, wfAlias)
		case "SetQueryHandler":
			err = validateQueryHandlerCall(call, wfAlias)
		}
		if err != nil {
			validationErr = fmt.Errorf("%s: %w", fset.Position(call.Pos()), err)
			return false
		}
		return true
	})
	return validationErr
}

// validateQueryHandlerCall 校验 query handler 的形状：内联的
// func(ctx workflow.Context, input map[string]any) map[string]any，query 名非空。
func validateQueryHandlerCall(call *ast.CallExpr, wfAlias string) error {
	const shape = "workflow.SetQueryHandler handler must be func(ctx workflow.Context, input map[string]any) map[string]any"
	if len(call.Args) != 3 {
		return fmt.Errorf("workflow.SetQueryHandler expects context, name, and handler")
	}
	if name, ok := call.Args[1].(*ast.BasicLit); ok && name.Value == `""` {
		return fmt.Errorf("workflow.SetQueryHandler query name is empty")
	}
	handler, ok := call.Args[2].(*ast.FuncLit)
	if !ok {
		return fmt.Errorf("workflow.SetQueryHandler handler must be an inline function")
	}
	params := flattenFieldTypes(handler.Type.Params)
	returns := flattenFieldTypes(handler.Type.Results)
	if len(params) != 2 || !isWorkflowContextType(params[0], wfAlias) || !isStringAnyMap(params[1]) ||
		len(returns) != 1 || !isStringAnyMap(returns[0]) {
		return fmt.Errorf(shape)
	}
	return nil
}

func validateUpdateHandlerCall(call *ast.CallExpr, wfAlias string) error {
	if len(call.Args) != 3 {
		return fmt.Errorf("workflow.SetUpdateHandler expects context, name, and handler")
	}
	handler, ok := call.Args[2].(*ast.FuncLit)
	if !ok {
		return fmt.Errorf("workflow.SetUpdateHandler handler must be an inline function")
	}
	params := flattenFieldTypes(handler.Type.Params)
	if len(params) == 0 || !isWorkflowContextType(params[0], wfAlias) {
		return fmt.Errorf("workflow.SetUpdateHandler handler first parameter must be workflow.Context")
	}
	for _, param := range params[1:] {
		if _, ok := param.(*ast.Ellipsis); ok {
			return fmt.Errorf("workflow.SetUpdateHandler handler business parameters must have fixed arity")
		}
	}
	returns := flattenFieldTypes(handler.Type.Results)
	switch len(returns) {
	case 1:
		if !isErrorType(returns[0]) {
			return fmt.Errorf("workflow.SetUpdateHandler handler must return error or (result, error)")
		}
	case 2:
		if !isErrorType(returns[1]) {
			return fmt.Errorf("workflow.SetUpdateHandler handler last return value must be error")
		}
	default:
		return fmt.Errorf("workflow.SetUpdateHandler handler must return error or (result, error)")
	}
	return nil
}

func flattenFieldTypes(fields *ast.FieldList) []ast.Expr {
	if fields == nil {
		return nil
	}
	var out []ast.Expr
	for _, field := range fields.List {
		count := len(field.Names)
		if count == 0 {
			count = 1
		}
		for range count {
			out = append(out, field.Type)
		}
	}
	return out
}

// isWorkflowContextType 报告 expr 是否为 sdk/workflow 的 Context；wfAlias 是本文件导入该包的名字。
func isWorkflowContextType(expr ast.Expr, wfAlias string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Context" {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == wfAlias
}

func isErrorType(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "error"
}

// programActivities 从翻译产物得到 Activity 引用表：每个 ExecuteActivity 的函数符号一条。
//
// 符号的缺省路由只来自 @Activity 注解（由 GeneratePackage 填写）；调用点的 workflow.ActivityOptions
// 只作用于那一次调用，运行期随调用传入。
func programActivities(program []any) map[string]programpkg.ActivityRef {
	refs := map[string]programpkg.ActivityRef{}
	walkProgram(program, func(node map[string]any) {
		args, ok := node["ExecuteActivity"].([]any)
		if !ok || len(args) < 3 {
			return
		}
		marker, _ := args[2].(map[string]any)
		symbol, _ := marker["symbol"].(string)
		if symbol == "" {
			return
		}
		refs[symbol] = programpkg.ActivityRef{Symbol: symbol, Type: symbol}
	})
	return refs
}

// walkProgram 按确定的顺序（数组按下标、对象按 key 排序）访问 WLang 程序里的每个对象节点。
func walkProgram(node any, visit func(map[string]any)) {
	switch value := node.(type) {
	case map[string]any:
		visit(value)
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			walkProgram(value[key], visit)
		}
	case []any:
		for _, item := range value {
			walkProgram(item, visit)
		}
	}
}

// exprString 复原简单表达式的源码写法，用于错误信息。
func exprString(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	case *ast.BasicLit:
		return x.Value
	}
	return fmt.Sprintf("%T", expr)
}
