package sdkgen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	gotypes "go/types"
	"sort"
	"strconv"
	"strings"
)

// workflowSDKPath 是 workflow 编排 SDK 的 import path。
const workflowSDKPath = "github.com/donutnomad/tenuto/sdk/workflow"

// rewriteResult 是一次工作流函数体重写的结果。
type rewriteResult struct {
	Source []byte
	// Used 是按出现顺序去重后的被调用契约；只有这些符号进入 Program.Activities。
	Used []Contract
}

// rewriteWorkflowCalls 把 workflow 函数体里对生成桥 CallXXX 的调用改写成
// `workflow.ExecuteActivity(ctx, marker, in, opts...).Get(ctx, &out)`。
//
// 只改内存中的源码副本；支持的语句形态固定为
//
//	out, err := pkg.CallX(ctx, in [, opts])
//	out, err = pkg.CallX(ctx, in [, opts])
//	return pkg.CallX(ctx, in [, opts])
//
// 其他形态（嵌在任意表达式、多返回值重组、丢弃返回值）报 file:line 明确错误。
func rewriteWorkflowCalls(src []byte, filename, funcName string, ownImportPath string, byImportPath map[string][]Contract) (rewriteResult, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return rewriteResult{}, err
	}
	fn := findFuncDecl(file, funcName)
	if fn == nil {
		return rewriteResult{}, fmt.Errorf("%s: 找不到函数 %s", filename, funcName)
	}
	rewriter := &rewriter{
		src:       src,
		fset:      fset,
		file:      file,
		filename:  filename,
		defs:      shortVarDefs(fset, file),
		used:      map[string]Contract{},
		rewritten: map[*ast.CallExpr]bool{},
	}
	rewriter.pathAlias = map[string]string{}
	rewriter.local = indexContracts(byImportPath[ownImportPath])
	rewriter.localImpls = indexActivityImplementations(byImportPath[ownImportPath])
	rewriter.localMarkers = indexMarkers(byImportPath[ownImportPath])
	rewriter.imported = map[string]map[string]Contract{}
	rewriter.importedImpls = map[string]map[string]Contract{}
	rewriter.importedMarkers = map[string]map[string]Contract{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		contracts := byImportPath[path]
		if len(contracts) == 0 {
			continue
		}
		alias := importAlias(spec, contracts)
		rewriter.pathAlias[path] = alias
		rewriter.imported[alias] = indexContracts(contracts)
		rewriter.importedMarkers[alias] = indexMarkers(contracts)
		if impls := indexActivityImplementations(contracts); len(impls) > 0 {
			rewriter.importedImpls[alias] = impls
		}
	}
	rewriter.wfAlias, err = rewriter.workflowAlias()
	if err != nil {
		return rewriteResult{}, err
	}
	rewriter.names = collectNames(fn)
	rewriter.walkStmts(fn.Body.List)
	if rewriter.err != nil {
		return rewriteResult{}, rewriter.err
	}
	// 兜底：任何没被上面三种形态覆盖的桥调用都必须报错，不能猜。
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if rewriter.err != nil {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if !ok || rewriter.rewritten[call] {
			return true
		}
		// 直接写的 workflow.ExecuteActivity(ctx, marker, ...)：marker 带 @Activity 注解时
		// 记为已使用的契约，Generate 据此写出与 CallX 桥相同的 activity ref。
		if rewriter.isWorkflowMarkerCall(call, "ExecuteActivity") && len(call.Args) >= 2 {
			rewriter.rewriteDirectExecute(call)
			return true
		}
		if _, _, ok := rewriter.matchBridge(call); ok {
			rewriter.err = fmt.Errorf("%s: %s 只能用作 `out, err := ...`、`out, err = ...` 或 `return ...` 的完整右值", rewriter.position(call.Pos()), rewriter.callText(call))
			return false
		}
		// 直调 @Activity 实现函数无法调度：Go 能编译时（例如传 context.Background()）
		// 也必须明确报错，不能把它当普通函数内联进 WLang 而静默丢掉 activity 派发。
		if contract, _, ok := rewriter.matchImplementation(call); ok {
			rewriter.err = fmt.Errorf("%s: %s 直接调用了 @Activity 实现 %s；activity 只能通过生成的桥 %s 调度", rewriter.position(call.Pos()), rewriter.callText(call), contract.Type, contract.CallName)
			return false
		}
		return true
	})
	if rewriter.err != nil {
		return rewriteResult{}, rewriter.err
	}
	edits := rewriter.edits
	if rewriter.wfAlias == "" && len(edits) > 0 {
		importEdit, err := rewriter.injectWorkflowImport()
		if err != nil {
			return rewriteResult{}, err
		}
		edits = append(edits, importEdit)
	}
	sort.Slice(edits, func(i, j int) bool { return edits[i].start > edits[j].start })
	out := append([]byte(nil), src...)
	for _, edit := range edits {
		out = append(out[:edit.start], append([]byte(edit.text), out[edit.end:]...)...)
	}
	return rewriteResult{Source: out, Used: rewriter.usedOrder}, nil
}

// findFuncDecl 按 funcKey 查找函数：函数名，方法写成 "Type.Method"。
func findFuncDecl(file *ast.File, key string) *ast.FuncDecl {
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && funcKey(fn) == key {
			return fn
		}
	}
	return nil
}

// importAlias 取 import 别名：显式别名优先，否则用契约包的真实包名。
func importAlias(spec *ast.ImportSpec, contracts []Contract) string {
	if spec.Name != nil {
		return spec.Name.Name
	}
	if len(contracts) > 0 && contracts[0].PkgName != "" {
		return contracts[0].PkgName
	}
	return ""
}

func indexContracts(contracts []Contract) map[string]Contract {
	out := make(map[string]Contract, len(contracts))
	for _, contract := range contracts {
		out[contract.CallName] = contract
	}
	return out
}

// indexActivityImplementations 索引本地 @Activity 实现函数名（MarkerName），
// 用于识别“直调实现函数”这种无法调度的写法。remote 契约的 marker 是生成函数，
// 用户直调不属于 activity 调用，不索引。
func indexActivityImplementations(contracts []Contract) map[string]Contract {
	out := make(map[string]Contract, len(contracts))
	for _, contract := range contracts {
		if contract.Remote || contract.MarkerName == "" {
			continue
		}
		out[contract.MarkerName] = contract
	}
	return out
}

// indexMarkers 按 marker 名索引契约：本地实现的函数名，或 remote 契约的标记函数名。
func indexMarkers(contracts []Contract) map[string]Contract {
	out := make(map[string]Contract, len(contracts))
	for _, contract := range contracts {
		if contract.MarkerName != "" {
			out[contract.MarkerName] = contract
		}
	}
	return out
}

type sourceEdit struct {
	start int
	end   int
	text  string
}

type rewriter struct {
	src      []byte
	fset     *token.FileSet
	file     *ast.File
	filename string
	wfAlias  string
	// local 是同包契约（不带限定符）。
	local map[string]Contract
	// imported 是 import 别名 → 该包契约。
	imported map[string]map[string]Contract
	// localImpls/importedImpls 是本地 @Activity 实现函数名 → 契约，用于拒绝直调。
	localImpls    map[string]Contract
	importedImpls map[string]map[string]Contract
	// localMarkers/importedMarkers 是 marker 名 → 契约，用于识别直接写的 ExecuteActivity。
	localMarkers    map[string]Contract
	importedMarkers map[string]map[string]Contract
	// pathAlias 是 import path → 本文件使用的别名，跨包限定类型时用。
	pathAlias map[string]string
	// used 按注解位置去重记录被调用契约。
	used      map[string]Contract
	usedOrder []Contract
	// names 是函数体里已出现的标识符，用于给临时变量取名。
	names map[string]bool
	// defs 是文件里由 `:=` 新声明的标识符；决定重写后该用 := 还是 =。
	defs      map[*ast.Ident]gotypes.Object
	rewritten map[*ast.CallExpr]bool
	edits     []sourceEdit
	err       error
	// inElseIf 标记当前语句是上一级 if 的 else-if；该位置不能插入临时变量声明
	//（`else tmp := ...; if ...` 不是合法 Go），因此直接报明确错误。
	inElseIf bool
}

// shortVarDefs 返回文件里由 `:=` 新声明的标识符集合（go/types 的 Defs）。
//
// 为什么需要它：`out, err := Call(...)` 重写成 `var out T; <call>` 后，赋值运算符不能
// 再无条件用 `:=`——Go 要求 `:=` 左侧至少有一个新变量。第二个 `out, err :=`（err 已在
// 作用域里）重写后就是 `no new variables on left side of :=`，整包类型检查失败（下游
// 翻译会因此看不到同包其它文件的包级函数，marker 退化成变量）。
//
// 单文件类型检查就够：局部变量是否由本语句新建只取决于本文件的词法作用域。跨文件包级
// 符号解析失败不影响 Defs 记录，因此这里忽略所有类型错误。
func shortVarDefs(fset *token.FileSet, file *ast.File) map[*ast.Ident]gotypes.Object {
	info := &gotypes.Info{Defs: map[*ast.Ident]gotypes.Object{}}
	conf := gotypes.Config{Error: func(error) {}}
	_, _ = conf.Check(file.Name.Name, fset, []*ast.File{file}, info)
	return info.Defs
}

// walkStmts 递归处理语句列表；容器语句（if/for/switch）的 Init 在 Go 语法里不允许
// 出现 var 声明，所以这类位置通过 prefix 把临时变量声明提到容器语句之前。
func (w *rewriter) walkStmts(stmts []ast.Stmt) {
	for _, stmt := range stmts {
		w.walkScoped(stmt)
	}
}

// walkScoped 处理一条语句；它的 if/for/switch 初始化里提升出的 var 声明连同语句本身包进一个块，
// 提升的变量与 Go 源码里 init 变量的作用域相同：不与同一块里的同名变量冲突，也不遮蔽语句之后的代码。
func (w *rewriter) walkScoped(stmt ast.Stmt) {
	var hoisted []string
	w.walkStmt(stmt, func(decl string) { hoisted = append(hoisted, decl) }, false)
	if len(hoisted) == 0 || w.err != nil {
		return
	}
	w.insertBefore(stmt, "{ "+strings.Join(hoisted, "; ")+"; ")
	w.insertAt(stmt.End(), " }")
}

// walkStmt 处理一条语句，并递归进入它包含的语句列表与闭包体。
func (w *rewriter) walkStmt(stmt ast.Stmt, prefix func(string), inInit bool) {
	if w.err != nil || stmt == nil {
		return
	}
	w.walkFuncLits(stmt)
	switch s := stmt.(type) {
	case *ast.AssignStmt:
		if len(s.Rhs) == 1 {
			if call, ok := s.Rhs[0].(*ast.CallExpr); ok {
				if contract, qualifier, matched := w.matchBridge(call); matched {
					w.rewritten[call] = true
					w.rewriteAssign(s, call, contract, qualifier, prefix, inInit)
					return
				}
			}
		}
	case *ast.ReturnStmt:
		if len(s.Results) == 1 {
			if call, ok := s.Results[0].(*ast.CallExpr); ok {
				if contract, qualifier, matched := w.matchBridge(call); matched {
					w.rewritten[call] = true
					w.rewriteReturn(s, call, contract, qualifier)
					return
				}
			}
		}
	case *ast.ExprStmt:
		if call, ok := s.X.(*ast.CallExpr); ok {
			if _, _, matched := w.matchBridge(call); matched {
				w.err = fmt.Errorf("%s: %s 的返回值被丢弃；必须写成 `out, err := ...` 或 `return ...`", w.position(call.Pos()), w.callText(call))
				return
			}
		}
	case *ast.IfStmt:
		// else-if 标记只作用于本 if 的条件；它的 body 里不再是 else-if 位置。
		elseIf := w.inElseIf
		w.inElseIf = false
		w.hoistApplicationErrorGuard(s, elseIf)
		w.walkStmt(s.Init, prefix, true)
		w.walkStmts(s.Body.List)
		if s.Else != nil {
			_, elseIf := s.Else.(*ast.IfStmt)
			previous := w.inElseIf
			w.inElseIf = elseIf
			w.walkScoped(s.Else)
			w.inElseIf = previous
		}
	case *ast.ForStmt:
		w.walkStmt(s.Init, prefix, true)
		w.walkStmt(s.Post, prefix, true)
		w.rejectApplicationErrorInCondition(s.Cond, "for")
		w.walkStmts(s.Body.List)
	case *ast.RangeStmt:
		w.walkStmts(s.Body.List)
	case *ast.SwitchStmt:
		w.walkStmt(s.Init, prefix, true)
		w.rejectApplicationErrorInCondition(s.Tag, "switch")
		w.walkCaseBodies(s.Body)
	case *ast.TypeSwitchStmt:
		w.walkStmt(s.Init, prefix, true)
		w.walkFuncLits(s.Assign)
		w.walkCaseBodies(s.Body)
	case *ast.BlockStmt:
		w.walkStmts(s.List)
	case *ast.LabeledStmt:
		w.walkStmt(s.Stmt, prefix, false)
	}
}

// walkCaseBodies 遍历 switch/type-switch 的 case 语句体。
func (w *rewriter) walkCaseBodies(body *ast.BlockStmt) {
	if body == nil {
		return
	}
	for _, raw := range body.List {
		switch clause := raw.(type) {
		case *ast.CaseClause:
			w.walkStmts(clause.Body)
		case *ast.CommClause:
			w.walkStmts(clause.Body)
		}
	}
}

// walkFuncLits 递归处理语句里内联闭包（如 workflow.Go(ctx, func(...){...})）的语句体，
// 但不深入子语句（if/for/switch 的 Init、Post、语句体，标签语句的目标）——那些由 walkStmt 逐层处理，
// 这里再进入会把同一个闭包重写两次。
func (w *rewriter) walkFuncLits(stmt ast.Stmt) {
	ast.Inspect(stmt, func(node ast.Node) bool {
		if node == stmt {
			return true
		}
		if _, ok := node.(ast.Stmt); ok {
			return false
		}
		literal, ok := node.(*ast.FuncLit)
		if !ok {
			return true
		}
		w.walkStmts(literal.Body.List)
		return false
	})
}

// rewriteAssign 处理 `out, err := Call` / `out, err = Call`。
func (w *rewriter) rewriteAssign(stmt *ast.AssignStmt, call *ast.CallExpr, contract Contract, qualifier string, prefix func(string), inInit bool) {
	if len(stmt.Lhs) != 2 {
		w.err = fmt.Errorf("%s: %s 必须赋值给两个变量 (out, err)，实际 %d 个", w.position(stmt.Pos()), w.callText(call), len(stmt.Lhs))
		return
	}
	outName := w.nameOf(stmt.Lhs[0])
	if w.err != nil {
		return
	}
	errName := w.nameOf(stmt.Lhs[1])
	if w.err != nil {
		return
	}
	if outName == "_" && errName == "_" {
		w.err = fmt.Errorf("%s: %s 的输出与错误都被丢弃；必须接收 err", w.position(stmt.Pos()), w.callText(call))
		return
	}
	outputType, err := w.qualifyType(contract.OutputExpr, contract, qualifier)
	if err != nil {
		w.err = err
		return
	}
	define := stmt.Tok == token.DEFINE
	// `var` 只在本条 := 真正新建变量时补：变量已在作用域里时再声明会重复声明。
	declaredOut := define && w.shortVarNew(stmt.Lhs[0])
	declaredErr := define && w.shortVarNew(stmt.Lhs[1])
	var parts []string
	receiver := outName
	switch {
	case outName == "_":
		receiver = w.freeName("bridgeOut")
		if inInit {
			prefix("var " + receiver + " " + outputType)
		} else {
			parts = append(parts, "var "+receiver+" "+outputType)
		}
	case declaredOut:
		if inInit {
			prefix("var " + outName + " " + outputType)
		} else {
			parts = append(parts, "var "+outName+" "+outputType)
		}
	}
	assignOp := "="
	if define && errName != "_" && declaredErr {
		assignOp = ":="
	}
	parts = append(parts, errName+" "+assignOp+" "+w.executeActivity(call, contract, qualifier, "&"+receiver))
	w.recordUse(contract, qualifier)
	w.replace(stmt, strings.Join(parts, "; "))
}

// shortVarNew 报告 id 是否由当前这条 `:=` 语句新建（go/types 的 Defs 结果）。
func (w *rewriter) shortVarNew(id ast.Expr) bool {
	ident, ok := id.(*ast.Ident)
	if !ok || ident == nil {
		return false
	}
	return w.defs[ident] != nil
}

// rewriteReturn 处理 `return Call`：临时变量接收输出后原样返回。
func (w *rewriter) rewriteReturn(stmt *ast.ReturnStmt, call *ast.CallExpr, contract Contract, qualifier string) {
	outputType, err := w.qualifyType(contract.OutputExpr, contract, qualifier)
	if err != nil {
		w.err = err
		return
	}
	outName := w.freeName("bridgeOut")
	errName := w.freeName("bridgeErr")
	parts := []string{
		"var " + outName + " " + outputType,
		errName + " := " + w.executeActivity(call, contract, qualifier, "&"+outName),
		"return " + outName + ", " + errName,
	}
	w.recordUse(contract, qualifier)
	w.replace(stmt, strings.Join(parts, "; "))
}

// executeActivity 生成 `workflow.ExecuteActivity(ctx, marker, in, opts...).Get(ctx, out)`。
func (w *rewriter) executeActivity(call *ast.CallExpr, contract Contract, qualifier, outArg string) string {
	args := call.Args
	ctxExpr := w.source(args[0])
	if len(args) < 2 {
		w.err = fmt.Errorf("%s: %s 至少需要 (ctx, in) 两个参数", w.position(call.Pos()), w.callText(call))
		return ""
	}
	inExpr := w.source(args[1])
	if !w.checkActivityTimeouts(call, args[2:]) {
		return ""
	}
	var opts strings.Builder
	for i, arg := range args[2:] {
		opts.WriteString(", ")
		opts.WriteString(w.source(arg))
		if call.Ellipsis.IsValid() && i == len(args[2:])-1 {
			opts.WriteString("...")
		}
	}
	return fmt.Sprintf("%s.ExecuteActivity(%s, %s, %s%s).Get(%s, %s)",
		w.wfAlias, ctxExpr, contract.markerExpr(qualifier), inExpr, opts.String(), ctxExpr, outArg)
}

// isPredeclaredTypeName 判断预声明类型名（不能加包限定）。
func isPredeclaredTypeName(name string) bool {
	switch name {
	case "bool", "string", "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "uintptr",
		"byte", "rune", "float32", "float64", "complex64", "complex128",
		"any", "error":
		return true
	}
	return false
}

// qualifyType 把契约包里的类型表达式渲染到 workflow 包里：
// 裸标识符补上契约包别名，已带包限定的类型要求 workflow 文件已 import 同一路径。
func (w *rewriter) qualifyType(expr ast.Expr, contract Contract, qualifier string) (string, error) {
	render := func(node ast.Expr) (string, error) { return w.qualifyType(node, contract, qualifier) }
	switch x := expr.(type) {
	case *ast.Ident:
		if qualifier == "" || isPredeclaredTypeName(x.Name) {
			return x.Name, nil
		}
		return qualifier + "." + x.Name, nil
	case *ast.StarExpr:
		elem, err := render(x.X)
		return "*" + elem, err
	case *ast.ArrayType:
		elem, err := render(x.Elt)
		if err != nil {
			return "", err
		}
		if x.Len != nil {
			length, err := strconv.Atoi(sourceOf(w.fset, w.src, x.Len))
			if err != nil {
				return "", fmt.Errorf("%s: activity %s 的类型 %s 使用了非常量长度", contract.Pos, contract.Type, sourceOf(w.fset, w.src, expr))
			}
			return fmt.Sprintf("[%d]%s", length, elem), nil
		}
		return "[]" + elem, nil
	case *ast.MapType:
		key, err := render(x.Key)
		if err != nil {
			return "", err
		}
		value, err := render(x.Value)
		return "map[" + key + "]" + value, err
	case *ast.SelectorExpr:
		prefix, ok := x.X.(*ast.Ident)
		if !ok {
			return "", fmt.Errorf("%s: activity %s 的类型 %s 形态不支持，请在契约包里使用裸类型名", contract.Pos, contract.Type, sourceOf(w.fset, w.src, expr))
		}
		path, ok := contract.Imports[prefix.Name]
		if !ok {
			return "", fmt.Errorf("%s: activity %s 的类型 %s 引用了未知包 %s", contract.Pos, contract.Type, sourceOf(w.fset, w.src, expr), prefix.Name)
		}
		alias, ok := w.pathAlias[path]
		if !ok {
			return "", fmt.Errorf("%s: activity %s 的类型 %s 来自 %s，workflow 文件必须 import 该包", contract.Pos, contract.Type, sourceOf(w.fset, w.src, expr), path)
		}
		return alias + "." + x.Sel.Name, nil
	}
	return "", fmt.Errorf("%s: activity %s 的类型 %s 形态不支持", contract.Pos, contract.Type, sourceOf(w.fset, w.src, expr))
}

// matchBridge 判断调用是不是生成的桥：同包不加限定符，跨包按 import 别名。
func (w *rewriter) matchBridge(call *ast.CallExpr) (Contract, string, bool) {
	switch target := call.Fun.(type) {
	case *ast.Ident:
		contract, ok := w.local[target.Name]
		return contract, "", ok
	case *ast.SelectorExpr:
		root, ok := target.X.(*ast.Ident)
		if !ok {
			return Contract{}, "", false
		}
		contract, ok := w.imported[root.Name][target.Sel.Name]
		return contract, root.Name, ok
	}
	return Contract{}, "", false
}

// matchImplementation 判断调用是否是本地 @Activity 实现函数（而非生成的桥）。
func (w *rewriter) matchImplementation(call *ast.CallExpr) (Contract, string, bool) {
	switch target := call.Fun.(type) {
	case *ast.Ident:
		contract, ok := w.localImpls[target.Name]
		return contract, "", ok
	case *ast.SelectorExpr:
		root, ok := target.X.(*ast.Ident)
		if !ok {
			return Contract{}, "", false
		}
		contract, ok := w.importedImpls[root.Name][target.Sel.Name]
		return contract, root.Name, ok
	}
	return Contract{}, "", false
}

// matchMarker 判断表达式是不是某个契约的 marker：同包裸标识符，跨包带 import 别名。
func (w *rewriter) matchMarker(expr ast.Expr) (Contract, string, bool) {
	switch target := expr.(type) {
	case *ast.Ident:
		contract, ok := w.localMarkers[target.Name]
		return contract, "", ok
	case *ast.SelectorExpr:
		root, ok := target.X.(*ast.Ident)
		if !ok {
			return Contract{}, "", false
		}
		contract, ok := w.importedMarkers[root.Name][target.Sel.Name]
		return contract, root.Name, ok
	}
	return Contract{}, "", false
}

// rewriteDirectExecute 处理作者直接写的 workflow.ExecuteActivity(ctx, marker, in, ...)：marker 带 @Activity 注解时
// 记为已使用的契约。输入原样传递：struct 值以 record 交给 Worker，按类型表编码成 JSON。
func (w *rewriter) rewriteDirectExecute(call *ast.CallExpr) {
	if len(call.Args) >= 3 && !w.checkActivityTimeouts(call, call.Args[3:]) {
		return
	}
	if contract, qualifier, annotated := w.matchMarker(call.Args[1]); annotated {
		w.recordUse(contract, qualifier)
	}
}

// checkActivityTimeouts 要求 Activity 调用点设置 StartToCloseTimeout 或 ScheduleToCloseTimeout：
// attempt 开始后它的存活只由超时判定。opts 是调用点的 ActivityOptions 实参；其中有变量或展开时
// 选项的值在生成期不可知，由 Server 在调度时校验。返回 false 时 w.err 已写入。
func (w *rewriter) checkActivityTimeouts(call *ast.CallExpr, opts []ast.Expr) bool {
	for _, opt := range opts {
		literal, ok := opt.(*ast.CompositeLit)
		if !ok || !isActivityOptionsType(literal.Type) {
			return true
		}
		for _, element := range literal.Elts {
			field, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := field.Key.(*ast.Ident)
			if !ok || (key.Name != "StartToCloseTimeout" && key.Name != "ScheduleToCloseTimeout") {
				continue
			}
			if value, ok := field.Value.(*ast.BasicLit); ok && value.Value == "0" {
				continue
			}
			return true
		}
	}
	w.err = fmt.Errorf("%s: %s 没有设置 StartToCloseTimeout 或 ScheduleToCloseTimeout；传入 %s.ActivityOptions{StartToCloseTimeout: 30 * time.Second}",
		w.position(call.Pos()), w.callText(call), w.workflowPackageName())
	return false
}

func isActivityOptionsType(expr ast.Expr) bool {
	switch typ := expr.(type) {
	case *ast.Ident:
		return typ.Name == "ActivityOptions"
	case *ast.SelectorExpr:
		return typ.Sel.Name == "ActivityOptions"
	}
	return false
}

// workflowPackageName 是错误提示里 workflow 包的写法：源码导入了它时用导入名。
func (w *rewriter) workflowPackageName() string {
	if w.wfAlias != "" {
		return w.wfAlias
	}
	return "workflow"
}

func (w *rewriter) recordUse(contract Contract, qualifier string) {
	if _, exists := w.used[contract.Pos]; exists {
		return
	}
	w.used[contract.Pos] = contract
	w.usedOrder = append(w.usedOrder, contract)
}

// nameOf 取赋值左侧的标识符名；不是标识符时报错。
func (w *rewriter) nameOf(expr ast.Expr) string {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		w.err = fmt.Errorf("%s: %s 的赋值左侧必须是标识符", w.position(expr.Pos()), exprString(expr))
		return "_"
	}
	return ident.Name
}

// freeName 取一个函数体内未被占用的临时变量名。
func (w *rewriter) freeName(prefix string) string {
	for index := 1; ; index++ {
		candidate := fmt.Sprintf("%s%d", prefix, index)
		if !w.names[candidate] {
			w.names[candidate] = true
			return candidate
		}
	}
}

// insertBefore 在语句之前插入文本（用于把 var 声明提到 if/for 的初始化之前）。
func (w *rewriter) insertBefore(node ast.Node, text string) {
	w.insertAt(node.Pos(), text)
}

// insertAt 在源码位置 pos 插入文本。
func (w *rewriter) insertAt(pos token.Pos, text string) {
	file := w.fset.File(pos)
	if file == nil {
		w.err = fmt.Errorf("%s: 无法定位插入位置", w.position(pos))
		return
	}
	offset := file.Offset(pos)
	w.edits = append(w.edits, sourceEdit{start: offset, end: offset, text: text})
}

func (w *rewriter) replace(node ast.Node, text string) {
	file := w.fset.File(node.Pos())
	if file == nil {
		w.err = fmt.Errorf("%s: 无法定位重写区间", w.position(node.Pos()))
		return
	}
	w.edits = append(w.edits, sourceEdit{start: file.Offset(node.Pos()), end: file.Offset(node.End()), text: text})
}

// position 输出 file:line（不带列号，便于直接对照源码）。
func (w *rewriter) position(pos token.Pos) string {
	position := w.fset.Position(pos)
	if position.Filename == "" {
		return w.filename
	}
	return fmt.Sprintf("%s:%d", position.Filename, position.Line)
}

func (w *rewriter) source(node ast.Node) string {
	file := w.fset.File(node.Pos())
	if file == nil {
		return ""
	}
	start, end := file.Offset(node.Pos()), file.Offset(node.End())
	if start < 0 || end > len(w.src) || start >= end {
		return ""
	}
	return string(w.src[start:end])
}

// callText 复原调用原文，用于错误信息里的实际形态。
func (w *rewriter) callText(call *ast.CallExpr) string {
	if text := w.source(call); text != "" {
		return text
	}
	return exprString(call.Fun)
}

// workflowAlias 取文件里 sdk/workflow 的导入别名；未导入时返回空串。
func (w *rewriter) workflowAlias() (string, error) {
	alias := workflowImportAlias(w.file)
	if alias == "_" || alias == "." {
		return "", fmt.Errorf("%s: workflow 包以 %q 别名导入，无法用于生成的桥调用重写", w.filename, alias)
	}
	return alias, nil
}

// injectWorkflowImport 给没有导入 workflow 包的文件补一条别名 import。
func (w *rewriter) injectWorkflowImport() (sourceEdit, error) {
	w.wfAlias = "workflow"
	spec := "workflow " + strconv.Quote(workflowSDKPath)
	for _, declaration := range w.file.Decls {
		gen, ok := declaration.(*ast.GenDecl)
		if !ok || gen.Tok != token.IMPORT {
			continue
		}
		file := w.fset.File(gen.Pos())
		if file == nil {
			return sourceEdit{}, fmt.Errorf("%s: 无法定位 import 块", w.filename)
		}
		if gen.Lparen.IsValid() {
			offset := file.Offset(gen.Lparen) + 1
			return sourceEdit{start: offset, end: offset, text: "\n\t" + spec}, nil
		}
		offset := file.Offset(gen.End())
		return sourceEdit{start: offset, end: offset, text: "\nimport " + spec}, nil
	}
	packageFile := w.fset.File(w.file.Name.End())
	if packageFile == nil {
		return sourceEdit{}, fmt.Errorf("%s: 无法定位 package 子句", w.filename)
	}
	offset := packageFile.Offset(w.file.Name.End())
	for offset < len(w.src) && w.src[offset] != '\n' {
		offset++
	}
	return sourceEdit{start: offset, end: offset, text: "\n\nimport " + spec + "\n"}, nil
}

// collectNames 收集函数定义里出现的所有标识符，供临时变量避让。
func collectNames(fn *ast.FuncDecl) map[string]bool {
	names := map[string]bool{}
	ast.Inspect(fn, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok {
			names[ident.Name] = true
		}
		return true
	})
	return names
}

// hoistApplicationErrorGuard 把 `if workflow.IsApplicationError(err, "Type")` 的判定下沉成
// 临时变量声明，守卫改读该变量。
//
// 目的：wlang 的 if 守卫只接受有限表达式形态（比较/布尔组合），函数调用不能直接当守卫。
// 作者仍写自然的 `if workflow.IsApplicationError(...)` 或 `!workflow.IsApplicationError(...)`；
// 这里只在既有 AST 重写层做最小适配，不扩展通用函数表达式框架。
//
// 求值位置与次数不变：临时变量声明插在 if 语句之前，守卫只读变量；else 分支不受影响。
// 组合条件（&&、||、混入其它表达式）不在支持范围内，直接报明确错误，不猜语义。
func (w *rewriter) hoistApplicationErrorGuard(stmt *ast.IfStmt, elseIf bool) {
	if w.err != nil || stmt == nil || stmt.Cond == nil {
		return
	}
	call, negated := w.applicationErrorGuardCall(stmt.Cond)
	if call == nil {
		if mixed := w.findApplicationErrorCall(stmt.Cond); mixed != nil {
			w.err = fmt.Errorf("%s: workflow.IsApplicationError 只支持完整 if 条件（可用 ! 取反）；"+
				"与其它条件组合时请先赋值给变量，再在 if 中使用该变量", w.position(mixed.Pos()))
		}
		return
	}
	if elseIf {
		w.err = fmt.Errorf("%s: workflow.IsApplicationError 守卫暂不支持 else-if 形态；"+
			"请写成 `else { if workflow.IsApplicationError(...) { ... } }` 或先给判定赋值变量", w.position(call.Pos()))
		return
	}
	if stmt.Init != nil {
		w.err = fmt.Errorf("%s: workflow.IsApplicationError 守卫不能与 if 初始化语句同时使用"+
			"（初始化声明的变量在条件里可见，提前到 if 之前会改变作用域）；请把初始化语句移到 if 之前", w.position(call.Pos()))
		return
	}
	name := w.freeName("applicationError")
	w.insertBefore(stmt, fmt.Sprintf("%s := %s; ", name, w.callText(call)))
	if negated {
		w.replace(stmt.Cond, "!"+name)
		return
	}
	w.replace(stmt.Cond, name)
}

// rejectApplicationErrorInCondition 给 for/switch 条件里的 IsApplicationError 诊断。
//
// 这些位置没有「提前声明临时变量」的等价插入点：for 条件每轮求值、switch 标签在选择时
// 求值，提升到语句之前会改变求值时机/次数。因此保持有限变换（只处理 if 条件），要求作者
// 先把判定赋值给变量，再用变量作条件。
func (w *rewriter) rejectApplicationErrorInCondition(cond ast.Expr, keyword string) {
	if w.err != nil || cond == nil {
		return
	}
	call := w.findApplicationErrorCall(cond)
	if call == nil {
		return
	}
	w.err = fmt.Errorf("%s: workflow.IsApplicationError 只支持 if 条件（可用 ! 取反）或先赋值给变量；"+
		"不能直接写在 %s 条件里，请先把判定赋值给变量", w.position(call.Pos()), keyword)
}

// applicationErrorGuardCall 识别 `workflow.IsApplicationError(...)` 与其 `!` 取反形态。
func (w *rewriter) applicationErrorGuardCall(cond ast.Expr) (*ast.CallExpr, bool) {
	negated := false
	if unary, ok := cond.(*ast.UnaryExpr); ok && unary.Op == token.NOT {
		negated = true
		cond = unary.X
	}
	call, ok := cond.(*ast.CallExpr)
	if !ok || !w.isWorkflowMarkerCall(call, "IsApplicationError") {
		return nil, false
	}
	return call, negated
}

// findApplicationErrorCall 在条件表达式里找 workflow.IsApplicationError 调用（用于给组合条件报错）。
func (w *rewriter) findApplicationErrorCall(cond ast.Expr) *ast.CallExpr {
	var found *ast.CallExpr
	ast.Inspect(cond, func(node ast.Node) bool {
		if found != nil {
			return false
		}
		call, ok := node.(*ast.CallExpr)
		if ok && w.isWorkflowMarkerCall(call, "IsApplicationError") {
			found = call
			return false
		}
		return true
	})
	return found
}

// isWorkflowMarkerCall 判断调用是否为 workflow 包（按本文件导入别名）的指定 marker。
func (w *rewriter) isWorkflowMarkerCall(call *ast.CallExpr, name string) bool {
	if call == nil || w.wfAlias == "" {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	if !ok {
		return false
	}
	return ident.Name == w.wfAlias
}
