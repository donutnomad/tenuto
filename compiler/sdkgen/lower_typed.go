package sdkgen

import (
	"fmt"
	"go/ast"
	"go/token"
	gotypes "go/types"
	"sort"
	"strings"
)

// lowerTyped 在 lowerWorkflowSource 之后、翻译之前，按 Go 类型信息改写 WLang 运算结果与 Go 不同的写法：
//
//   - 一侧静态类型是 any、另一侧不是 nil 的 == / !=：改成 workflow.Equal(a, b)，按 Go 的接口比较规则比较
//     动态类型与值；
//   - slice、map 与 nil 的 == / !=：改成 workflow.IsNil(x)，判断是否为 nil 值（空 slice 与空 map 不是 nil）；
//   - 两侧都是指向 struct 的指针（都不是 nil 字面量）的 == / !=：改成 workflow.SamePointer(a, b)，按地址比较；
//     WLang 的 == 比较指向的内容。其余指针（*[]T、*map、*基础类型）的载体是指向的值本身，没有地址可比，保持按
//     内容比较（go-subset 列为差异）；
//   - tag 或 case 表达式是 any 的 switch：tag 先求值一次存入临时变量，每个 case 用 workflow.Equal 比较；
//   - any 与 nil 的 == / !=：改成 workflow.Equal(x, nil)，按动态类型判断（装着 nil 指针的 any 不等于 nil）；
//   - 类型是 slice 或 map 的 nil（赋值、return、实参、复合字面量的元素与字段）：改成该类型的零值，之后能 append、range。
//
// 改写是 Go 到 Go 的等价变换，写在原来的行上，后续诊断的行号不变。一次类型检查算出全部改写；
// 嵌套的改写（外层改写的操作数里还有改写）在拼外层文本时先应用内层。
func lowerTyped(sources workflowSources, workflowFile, dir string) error {
	fset, files, info, pkgPath, err := loadPackage(sources.files, workflowFile, dir)
	if err != nil {
		return err
	}
	rewriters := map[string]*typedRewriter{}
	for _, fn := range sources.funcs {
		rewriter := rewriters[fn.file]
		if rewriter == nil {
			file := fileNamed(fset, files, fn.file)
			if file == nil {
				return fmt.Errorf("%s: 类型检查没有包含这个文件", fn.file)
			}
			rewriter = &typedRewriter{fset: fset, info: info, file: file, src: sources.files[fn.file], filename: fn.file, pkgPath: pkgPath, imports: importTable(file)}
			rewriters[fn.file] = rewriter
		}
		decl := findFuncDecl(rewriter.file, fn.key)
		if decl == nil || decl.Body == nil {
			continue
		}
		rewriter.names = collectNames(decl)
		rewriter.decl = decl
		rewriter.rewrite(decl.Body)
		if rewriter.err != nil {
			return rewriter.err
		}
	}
	for name, rewriter := range rewriters {
		if len(rewriter.edits) == 0 {
			continue
		}
		if rewriter.needsWorkflowImport && workflowImportAlias(rewriter.file) == "" {
			edit, err := workflowImportEdit(fset, rewriter.file, rewriter.src, name)
			if err != nil {
				return err
			}
			rewriter.edits = append(rewriter.edits, edit)
		}
		sources.files[name] = applyEdits(rewriter.src, rewriter.edits)
	}
	return nil
}

func fileNamed(fset *token.FileSet, files []*ast.File, name string) *ast.File {
	for _, file := range files {
		if samePath(fset.Position(file.Pos()).Filename, name) {
			return file
		}
	}
	return nil
}

type typedRewriter struct {
	fset     *token.FileSet
	info     *gotypes.Info
	file     *ast.File
	src      []byte
	filename string
	pkgPath  string
	// imports 是本文件的 import 别名 → path，用于在改写里写出其他包的类型名。
	imports map[string]string
	// names 是当前函数里已出现的标识符，用于给临时变量取名。
	names map[string]bool
	// decl 是正在改写的函数，return nil 按它的结果类型改写。
	decl *ast.FuncDecl
	// edits 互不重叠：登记外层改写时，落在它范围内的内层改写已并入它的文本。
	edits               []sourceEdit
	needsWorkflowImport bool
	err                 error
}

// rewrite 后序遍历函数体：子节点的改写先于父节点登记。
func (r *typedRewriter) rewrite(body *ast.BlockStmt) {
	var stack []ast.Node
	ast.Inspect(body, func(node ast.Node) bool {
		if r.err != nil {
			return false
		}
		if node != nil {
			// 转换检查按先序：外层转换先报（string(rune(x)) 报 string）。
			if call, ok := node.(*ast.CallExpr); ok {
				r.checkConversion(call)
				r.checkChannelReceive(call)
			}
			stack = append(stack, node)
			return true
		}
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		switch x := current.(type) {
		case *ast.BinaryExpr:
			r.rewriteComparison(x)
		case *ast.SwitchStmt:
			r.rewriteSwitch(x, parent)
		case *ast.Ident:
			r.rewriteTypedNil(x, stack)
		}
		return true
	})
}

func (r *typedRewriter) rewriteComparison(x *ast.BinaryExpr) {
	if x.Op != token.EQL && x.Op != token.NEQ {
		return
	}
	negate := ""
	if x.Op == token.NEQ {
		negate = "!"
	}
	xNil, yNil := r.isNil(x.X), r.isNil(x.Y)
	switch {
	case xNil && yNil:
	case xNil || yNil:
		operand := x.X
		if xNil {
			operand = x.Y
		}
		switch operandType := r.typeOf(operand); {
		case isSliceOrMap(operandType):
			r.replace(x.Pos(), x.End(), fmt.Sprintf("%sworkflow.IsNil(%s)", negate, r.text(operand.Pos(), operand.End())))
		case isEmptyInterface(operandType):
			// any 与 nil 比较按动态类型：装着 nil 指针、nil slice 的 any 不等于 nil。
			r.replace(x.Pos(), x.End(), fmt.Sprintf("%sworkflow.Equal(%s, nil)", negate, r.text(operand.Pos(), operand.End())))
		default:
			return
		}
	case isEmptyInterface(r.typeOf(x.X)) || isEmptyInterface(r.typeOf(x.Y)):
		r.replace(x.Pos(), x.End(), fmt.Sprintf("%sworkflow.Equal(%s, %s)", negate,
			r.text(x.X.Pos(), x.X.End()), r.text(x.Y.Pos(), x.Y.End())))
	case isStructPointer(r.typeOf(x.X)) && isStructPointer(r.typeOf(x.Y)):
		r.replace(x.Pos(), x.End(), fmt.Sprintf("%sworkflow.SamePointer(%s, %s)", negate,
			r.text(x.X.Pos(), x.X.End()), r.text(x.Y.Pos(), x.Y.End())))
	default:
		return
	}
	r.needsWorkflowImport = true
}

// rewriteSwitch 把 `switch init; tag { case a, b: ... }` 改成
// `switch zzTag := (tag); { case workflow.Equal(zzTag, a) || workflow.Equal(zzTag, b): ... }`；
// 有 init 时外面套一层 `if true { init; switch zzTag := (tag); { ... } }`：init 声明的变量只在这一层可见。
// go2wlang 把裸块 `{ ... }` 展平到外层作用域，init 里的同名变量会覆盖外层变量；它对带 init 的 switch
// 也用 `if true` 包裹。
func (r *typedRewriter) rewriteSwitch(s *ast.SwitchStmt, parent ast.Node) {
	if s.Tag == nil {
		return
	}
	interfaceCompare := isEmptyInterface(r.typeOf(s.Tag))
	hasCases := false
	for _, raw := range s.Body.List {
		for _, expr := range raw.(*ast.CaseClause).List {
			hasCases = true
			interfaceCompare = interfaceCompare || isEmptyInterface(r.typeOf(expr))
		}
	}
	if !interfaceCompare || !hasCases {
		return
	}
	if _, labeled := parent.(*ast.LabeledStmt); labeled && s.Init != nil {
		r.fail(s, "带标签的 switch 在 tag 是 any 时不能带初始化语句；把 %s 移到 switch 之前", r.text(s.Init.Pos(), s.Init.End()))
		return
	}
	tag := r.freeName("zzTag")
	for _, raw := range s.Body.List {
		clause := raw.(*ast.CaseClause)
		if len(clause.List) == 0 {
			continue
		}
		first, last := clause.List[0], clause.List[len(clause.List)-1]
		conditions := make([]string, 0, len(clause.List))
		for _, expr := range clause.List {
			conditions = append(conditions, fmt.Sprintf("workflow.Equal(%s, %s)", tag, r.text(expr.Pos(), expr.End())))
		}
		r.replace(first.Pos(), last.End(), strings.Join(conditions, " || "))
	}
	header := fmt.Sprintf("switch %s := (%s); ", tag, r.text(s.Tag.Pos(), s.Tag.End()))
	if s.Init != nil {
		header = "if true { " + r.text(s.Init.Pos(), s.Init.End()) + "; " + header
		r.insert(s.End(), " }")
	}
	r.replace(s.Switch, s.Body.Lbrace, header)
	r.needsWorkflowImport = true
}

// checkChannelReceive 拒绝 Receive 到接口类型的目标（`var v any; ch.Receive(ctx, &v)`）：Channel 在内存里传递
// 值的载体，接口目标得不到值原来的 WLang 类型。目标写成发送方的具体类型（string、结构体、map 等）。
func (r *typedRewriter) checkChannelReceive(call *ast.CallExpr) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Receive" || len(call.Args) != 2 {
		return
	}
	named, ok := r.info.TypeOf(sel.X).(*gotypes.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != workflowSDKPath || named.Obj().Name() != "Channel" {
		return
	}
	target, ok := unparen(call.Args[1]).(*ast.UnaryExpr)
	if !ok || target.Op != token.AND {
		return
	}
	if targetType := r.info.TypeOf(target.X); targetType != nil && gotypes.IsInterface(targetType) {
		r.fail(call.Args[1], "Channel 的 Receive 目标不能是接口类型 %s；workflow 里 Channel 传递的值不带 Go 类型，目标写成发送方的具体类型（string、结构体、map 等）", targetType)
	}
}

// checkConversion 拒绝改变值表示、转换目标不是 int 与 int64 的基础类型转换（float64(n)、string(65)、int32(x)）：
// WLang 的数值运算要求两侧类型一致，没有对应的转换。表示不变的转换照常翻译成原值：具名基础类型与其底层类型
// 互转（string(status)、Status("paid")、float64(amount)）。目标是 int、int64 的转换由 go2wlang 判断。
func (r *typedRewriter) checkConversion(call *ast.CallExpr) {
	tv, ok := r.info.Types[call.Fun]
	if !ok || !tv.IsType() || len(call.Args) != 1 {
		return
	}
	target, ok := tv.Type.Underlying().(*gotypes.Basic)
	if !ok || target.Kind() == gotypes.Int || target.Kind() == gotypes.Int64 {
		return
	}
	argType := r.info.TypeOf(call.Args[0])
	if argType == nil {
		return
	}
	source, ok := argType.Underlying().(*gotypes.Basic)
	if !ok {
		return
	}
	if source.Info()&gotypes.IsUntyped != 0 {
		source = gotypes.Default(source).(*gotypes.Basic)
	}
	if sameBasicRepresentation(source.Kind(), target.Kind()) {
		return
	}
	r.fail(call, "workflow 里不支持类型转换 %s(...)；WLang 的数值运算要求两侧类型一致，int 与 int64、具名基础类型与其底层类型可以互转，其余转换放进 Activity", r.text(call.Fun.Pos(), call.Fun.End()))
}

func sameBasicRepresentation(a, b gotypes.BasicKind) bool {
	integer := func(kind gotypes.BasicKind) bool { return kind == gotypes.Int || kind == gotypes.Int64 }
	return a == b || integer(a) && integer(b)
}

// rewriteTypedNil 把类型是 slice 或 map 的 nil 改成该类型的零值 `func() T { var zz T; return zz }()`。WLang 把
// nil 译成类型为 null 的值：赋给变量后变量的类型就成了 null，append 与 range 报 E_TYPE。零值带元素类型名与规范的
// nil 载体，与 `var xs []T` 相同，len 为 0，workflow.IsNil 为 true。和 nil 的比较由 rewriteComparison 处理，不改。
func (r *typedRewriter) rewriteTypedNil(x *ast.Ident, stack []ast.Node) {
	if !r.isNil(x) || len(stack) == 0 {
		return
	}
	target := r.nilContextType(x, stack)
	if target == nil || !isSliceOrMap(target) {
		return
	}
	text, ok := r.typeText(target)
	if !ok {
		r.fail(x, "这里的 nil 需要写出类型 %s，但本文件没有导入它所在的包；先导入该包", target)
		return
	}
	zero := r.freeName("zzNil")
	r.replace(x.Pos(), x.End(), fmt.Sprintf("func() %s { var %s %s; return %s }()", text, zero, text, zero))
}

// nilContextType 返回 nil 在所处位置被赋予的类型：赋值与变量声明的左侧、return 对应的结果、实参对应的形参、
// 复合字面量的元素、字段或 map 值。其他位置返回 nil。
func (r *typedRewriter) nilContextType(x ast.Expr, stack []ast.Node) gotypes.Type {
	switch p := stack[len(stack)-1].(type) {
	case *ast.AssignStmt:
		if len(p.Lhs) == len(p.Rhs) {
			for i, rhs := range p.Rhs {
				if rhs == x {
					return r.info.TypeOf(p.Lhs[i])
				}
			}
		}
	case *ast.ValueSpec:
		if len(p.Names) == len(p.Values) {
			for i, value := range p.Values {
				if value == x {
					return r.info.TypeOf(p.Names[i])
				}
			}
		}
	case *ast.ReturnStmt:
		results := r.enclosingResults(stack)
		if results != nil && results.Len() == len(p.Results) {
			for i, result := range p.Results {
				if result == x {
					return results.At(i).Type()
				}
			}
		}
	case *ast.CallExpr:
		signature, ok := r.info.TypeOf(p.Fun).(*gotypes.Signature)
		if !ok {
			return nil
		}
		params := signature.Params()
		for i, arg := range p.Args {
			if arg != x {
				continue
			}
			if signature.Variadic() && i >= params.Len()-1 {
				last := params.At(params.Len() - 1).Type()
				if p.Ellipsis.IsValid() {
					return last
				}
				if slice, ok := last.Underlying().(*gotypes.Slice); ok {
					return slice.Elem()
				}
				return nil
			}
			if i < params.Len() {
				return params.At(i).Type()
			}
		}
	case *ast.CompositeLit:
		return compositeElem(r.info.TypeOf(p), "")
	case *ast.KeyValueExpr:
		if p.Value != x || len(stack) < 2 {
			return nil
		}
		lit, ok := stack[len(stack)-2].(*ast.CompositeLit)
		if !ok {
			return nil
		}
		field := ""
		if key, ok := p.Key.(*ast.Ident); ok {
			field = key.Name
		}
		return compositeElem(r.info.TypeOf(lit), field)
	}
	return nil
}

// enclosingResults 返回 return 所在函数（最内层的函数字面量，否则是正在改写的函数）的结果列表。
func (r *typedRewriter) enclosingResults(stack []ast.Node) *gotypes.Tuple {
	for i := len(stack) - 1; i >= 0; i-- {
		if lit, ok := stack[i].(*ast.FuncLit); ok {
			if signature, ok := r.info.TypeOf(lit).(*gotypes.Signature); ok {
				return signature.Results()
			}
			return nil
		}
	}
	if object := r.info.Defs[r.decl.Name]; object != nil {
		if signature, ok := object.Type().(*gotypes.Signature); ok {
			return signature.Results()
		}
	}
	return nil
}

// compositeElem 返回复合字面量 t 里元素的类型：slice、数组与 map 是元素类型，struct 是名为 field 的字段类型。
func compositeElem(t gotypes.Type, field string) gotypes.Type {
	if t == nil {
		return nil
	}
	switch current := t.Underlying().(type) {
	case *gotypes.Slice:
		return current.Elem()
	case *gotypes.Array:
		return current.Elem()
	case *gotypes.Map:
		return current.Elem()
	case *gotypes.Struct:
		for i := 0; i < current.NumFields(); i++ {
			if current.Field(i).Name() == field {
				return current.Field(i).Type()
			}
		}
	case *gotypes.Pointer:
		return compositeElem(current.Elem(), field)
	}
	return nil
}

// typeOf 返回 expr 的类型。类型检查没有得到它的类型时（包有类型错误、导入解析失败，或包加载失败后只检查了
// 单个文件）生成失败：改写与否取决于类型，按缺省处理会静默生成与 Go 语义不同的程序。
func (r *typedRewriter) typeOf(expr ast.Expr) gotypes.Type {
	if tv, ok := r.info.Types[expr]; ok && tv.Type != nil && !containsInvalid(tv.Type) {
		return tv.Type
	}
	r.fail(expr, "类型检查没有得到 %s 的类型，生成器无法判断这里的比较、switch 或 map 读取要不要按 Go 语义改写；修正包里的编译错误（含无法解析的导入）后重新生成", r.text(expr.Pos(), expr.End()))
	return gotypes.Typ[gotypes.Invalid]
}

// containsInvalid 报告 t 本身或它的元素、键类型是否是无效类型。
func containsInvalid(t gotypes.Type) bool {
	switch x := t.(type) {
	case *gotypes.Basic:
		return x.Kind() == gotypes.Invalid
	case *gotypes.Map:
		return containsInvalid(x.Key()) || containsInvalid(x.Elem())
	case *gotypes.Slice:
		return containsInvalid(x.Elem())
	case *gotypes.Pointer:
		return containsInvalid(x.Elem())
	}
	return false
}

func (r *typedRewriter) isNil(expr ast.Expr) bool {
	tv, ok := r.info.Types[expr]
	return ok && tv.IsNil()
}

func isEmptyInterface(t gotypes.Type) bool {
	iface, ok := t.Underlying().(*gotypes.Interface)
	return ok && iface.Empty()
}

// isStructPointer 报告 t 是否是指向 struct 的指针：解释器里它的载体是共享的 *types.Record，可以按地址比较。
func isStructPointer(t gotypes.Type) bool {
	pointer, ok := t.Underlying().(*gotypes.Pointer)
	if !ok {
		return false
	}
	_, isStruct := pointer.Elem().Underlying().(*gotypes.Struct)
	return isStruct
}

func isSliceOrMap(t gotypes.Type) bool {
	switch t.Underlying().(type) {
	case *gotypes.Slice, *gotypes.Map:
		return true
	}
	return false
}

// typeText 是 t 在本文件里的写法：本包类型不带包名，其他包的类型用本文件的 import 别名。
func (r *typedRewriter) typeText(t gotypes.Type) (string, bool) {
	aliases := map[string]string{}
	for alias, path := range r.imports {
		aliases[path] = alias
	}
	missing := false
	text := gotypes.TypeString(t, func(pkg *gotypes.Package) string {
		if pkg.Path() == r.pkgPath {
			return ""
		}
		alias, ok := aliases[pkg.Path()]
		if !ok {
			missing = true
		}
		return alias
	})
	return text, !missing
}

func (r *typedRewriter) fail(node ast.Node, format string, args ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("%s: %s", r.fset.Position(node.Pos()), fmt.Sprintf(format, args...))
	}
}

func (r *typedRewriter) freeName(prefix string) string {
	for index := 1; ; index++ {
		candidate := fmt.Sprintf("%s%d", prefix, index)
		if !r.names[candidate] {
			r.names[candidate] = true
			return candidate
		}
	}
}

func (r *typedRewriter) offset(pos token.Pos) int {
	return r.fset.File(pos).Offset(pos)
}

// text 是 [from, to) 的源码，已登记的内层改写应用在内。
func (r *typedRewriter) text(from, to token.Pos) string {
	start, end := r.offset(from), r.offset(to)
	inner := r.editsWithin(start, end)
	var out strings.Builder
	cursor := start
	for _, edit := range inner {
		out.Write(r.src[cursor:edit.start])
		out.WriteString(edit.text)
		cursor = edit.end
	}
	out.Write(r.src[cursor:end])
	return out.String()
}

func (r *typedRewriter) editsWithin(start, end int) []sourceEdit {
	var inner []sourceEdit
	for _, edit := range r.edits {
		if start <= edit.start && edit.end <= end && !(edit.start == edit.end && (edit.start == start || edit.start == end)) {
			inner = append(inner, edit)
		}
	}
	sort.Slice(inner, func(i, j int) bool { return inner[i].start < inner[j].start })
	return inner
}

// replace 用 text 替换 [from, to)；范围内已登记的改写已由调用方经 text 并入，这里移除。
func (r *typedRewriter) replace(from, to token.Pos, text string) {
	start, end := r.offset(from), r.offset(to)
	kept := r.edits[:0]
	for _, edit := range r.edits {
		if start <= edit.start && edit.end <= end && !(edit.start == edit.end && (edit.start == start || edit.start == end)) {
			continue
		}
		kept = append(kept, edit)
	}
	r.edits = append(kept, sourceEdit{start: start, end: end, text: text})
}

func (r *typedRewriter) insert(at token.Pos, text string) {
	offset := r.offset(at)
	r.edits = append(r.edits, sourceEdit{start: offset, end: offset, text: text})
}
