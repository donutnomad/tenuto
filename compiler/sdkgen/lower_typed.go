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
//   - tag 或 case 表达式是 any 的 switch：tag 先求值一次存入临时变量，每个 case 用 workflow.Equal 比较；
//   - 元素类型是 struct 或基础类型的 map 读取 m[k] 与 v, ok := m[k]：改成立即调用的闭包，key 不存在时得到元素类型的零值。
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
		case *ast.IndexExpr:
			r.rewriteMapRead(x, parent)
		case *ast.SwitchStmt:
			r.rewriteSwitch(x, parent)
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
		if !isSliceOrMap(r.typeOf(operand)) {
			return
		}
		r.replace(x.Pos(), x.End(), fmt.Sprintf("%sworkflow.IsNil(%s)", negate, r.text(operand.Pos(), operand.End())))
	case isEmptyInterface(r.typeOf(x.X)) || isEmptyInterface(r.typeOf(x.Y)):
		r.replace(x.Pos(), x.End(), fmt.Sprintf("%sworkflow.Equal(%s, %s)", negate,
			r.text(x.X.Pos(), x.X.End()), r.text(x.Y.Pos(), x.Y.End())))
	default:
		return
	}
	r.needsWorkflowImport = true
}

// rewriteSwitch 把 `switch init; tag { case a, b: ... }` 改成
// `switch zzTag := (tag); { case workflow.Equal(zzTag, a) || workflow.Equal(zzTag, b): ... }`；
// 有 init 时外面再套一层块：`{ init; switch zzTag := (tag); { ... } }`。
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
		header = "{ " + r.text(s.Init.Pos(), s.Init.End()) + "; " + header
		r.insert(s.End(), " }")
	}
	r.replace(s.Switch, s.Body.Lbrace, header)
	r.needsWorkflowImport = true
}

// rewriteMapRead 把元素类型是 struct 或基础类型的 map 读取改成立即调用的闭包，key 不存在时得到元素类型的零值。
// WLang 的 map 读取在 key 不存在时取 map 载体的元素零值：元素是 struct 时是 nil；结构体字段与具名 map 类型的
// nil map 载体是 map[string]any，元素零值也是 nil。元素是指针、slice、map、接口时 nil 就是 Go 的零值，不改。
// 赋值左侧（m[k] = v、m[k] += v）不改。
func (r *typedRewriter) rewriteMapRead(x *ast.IndexExpr, parent ast.Node) {
	mapType, ok := r.typeOf(x.X).Underlying().(*gotypes.Map)
	if !ok {
		return
	}
	switch elem := mapType.Elem().Underlying().(type) {
	case *gotypes.Struct:
	case *gotypes.Basic:
		if elem.Kind() == gotypes.UntypedNil || elem.Kind() == gotypes.Invalid {
			return
		}
	default:
		return
	}
	commaOK := false
	switch p := parent.(type) {
	case *ast.AssignStmt:
		for _, lhs := range p.Lhs {
			if lhs == x {
				return
			}
		}
		commaOK = len(p.Lhs) == 2 && len(p.Rhs) == 1
	case *ast.ValueSpec:
		commaOK = len(p.Names) == 2 && len(p.Values) == 1
	case *ast.IncDecStmt:
		return
	}
	elem, ok := r.typeText(mapType.Elem())
	if !ok {
		r.fail(x, "读取 %s 需要写出元素类型 %s，但本文件没有导入它所在的包；先导入该包", r.text(x.Pos(), x.End()), mapType.Elem())
		return
	}
	value, present, zero := r.freeName("zzValue"), r.freeName("zzOK"), r.freeName("zzZero")
	read := r.text(x.Pos(), x.End())
	if commaOK {
		r.replace(x.Pos(), x.End(), fmt.Sprintf("func() (%s, bool) { if %s, %s := %s; %s { return %s, true }; var %s %s; return %s, false }()",
			elem, value, present, read, present, value, zero, elem, zero))
		return
	}
	r.replace(x.Pos(), x.End(), fmt.Sprintf("func() %s { if %s, %s := %s; %s { return %s }; var %s %s; return %s }()",
		elem, value, present, read, present, value, zero, elem, zero))
}

func (r *typedRewriter) typeOf(expr ast.Expr) gotypes.Type {
	if tv, ok := r.info.Types[expr]; ok && tv.Type != nil {
		return tv.Type
	}
	return gotypes.Typ[gotypes.Invalid]
}

func (r *typedRewriter) isNil(expr ast.Expr) bool {
	tv, ok := r.info.Types[expr]
	return ok && tv.IsNil()
}

func isEmptyInterface(t gotypes.Type) bool {
	iface, ok := t.Underlying().(*gotypes.Interface)
	return ok && iface.Empty()
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
