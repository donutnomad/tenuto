package sdkgen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

// lowerWorkflowSource 在翻译之前处理 workflow 函数（或它调用的同包函数，funcName 按 funcKey 写）的源码：
//
//  1. 拒绝在 workflow 里没有确定性语义的写法（原生并发、墙钟、随机数、IO），报 file:line 与替代写法；
//  2. 把 go2wlang 不接受、但有等价简单写法的 Go 语句改写成等价形态（只改内存里的源码副本）。
//
// 改写都是 Go 到 Go 的等价变换，产物仍能通过类型检查；每轮只应用互不重叠的改写，
// 重新解析后继续，直到没有可改写的位置。
func lowerWorkflowSource(src []byte, filename, funcName string) ([]byte, error) {
	for round := 0; round < 32; round++ {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		fn := findFuncDecl(file, funcName)
		if fn == nil || fn.Body == nil {
			return src, nil
		}
		pass := &lowerPass{src: src, fset: fset, file: file, filename: filename, imports: importTable(file), names: collectNames(fn)}
		if round == 0 {
			// 诊断只在原始源码上做：行号与作者看到的一致。
			pass.reject(fn)
			if pass.err != nil {
				return nil, pass.err
			}
		}
		pass.lowerBlock(fn.Body)
		pass.lowerExprs(fn)
		if pass.err != nil {
			return nil, pass.err
		}
		if pass.needsWorkflowImport && workflowImportAlias(file) == "" {
			edit, err := workflowImportEdit(fset, file, src, filename)
			if err != nil {
				return nil, err
			}
			pass.edits = append(pass.edits, edit)
		}
		if len(pass.edits) == 0 {
			return src, nil
		}
		src = applyEdits(src, pass.edits)
	}
	return nil, fmt.Errorf("%s: workflow 函数 %s 的改写没有收敛", filename, funcName)
}

// applyEdits 应用互不重叠的改写；与已接受改写重叠的留到下一轮。
func applyEdits(src []byte, edits []sourceEdit) []byte {
	// 同一位置上零宽插入排在替换之前；其余按起点排序，外层（更长）的改写优先。
	sort.SliceStable(edits, func(i, j int) bool {
		if edits[i].start != edits[j].start {
			return edits[i].start < edits[j].start
		}
		iInsert, jInsert := edits[i].start == edits[i].end, edits[j].start == edits[j].end
		if iInsert != jInsert {
			return iInsert
		}
		return edits[i].end > edits[j].end
	})
	var accepted []sourceEdit
	lastEnd := -1
	for _, edit := range edits {
		if edit.start < lastEnd {
			continue
		}
		accepted = append(accepted, edit)
		lastEnd = edit.end
	}
	out := append([]byte(nil), src...)
	for index := len(accepted) - 1; index >= 0; index-- {
		edit := accepted[index]
		out = append(out[:edit.start], append([]byte(edit.text), out[edit.end:]...)...)
	}
	return out
}

type lowerPass struct {
	src      []byte
	fset     *token.FileSet
	file     *ast.File
	filename string
	// imports 是本文件的 import 别名 → path。
	imports map[string]string
	// names 是函数里已出现的标识符，用于给临时变量取名。
	names map[string]bool
	edits []sourceEdit
	err   error
	// needsWorkflowImport 表示改写引入了 workflow 包的调用，文件没有导入它时补一条 import。
	needsWorkflowImport bool
}

// importTable 收集 import 别名 → path；没有显式别名时取路径末段（跳过 v2 这类版本段）。
func importTable(file *ast.File) map[string]string {
	table := map[string]string{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		segments := strings.Split(path, "/")
		alias := segments[len(segments)-1]
		if len(segments) > 1 && isVersionSegment(alias) {
			alias = segments[len(segments)-2]
		}
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		table[alias] = path
	}
	return table
}

// packageOf 返回 ident 指向的 import path；ident 是局部变量或不是包名时返回空串。
func (p *lowerPass) packageOf(expr ast.Expr) string {
	ident, ok := expr.(*ast.Ident)
	if !ok || ident.Obj != nil {
		return ""
	}
	return p.imports[ident.Name]
}

func (p *lowerPass) fail(node ast.Node, format string, args ...any) {
	if p.err != nil {
		return
	}
	position := p.fset.Position(node.Pos())
	p.err = fmt.Errorf("%s:%d: %s", position.Filename, position.Line, fmt.Sprintf(format, args...))
}

func (p *lowerPass) source(node ast.Node) string {
	file := p.fset.File(node.Pos())
	if file == nil {
		return ""
	}
	return string(p.src[file.Offset(node.Pos()):file.Offset(node.End())])
}

func (p *lowerPass) replace(node ast.Node, text string) {
	file := p.fset.File(node.Pos())
	p.edits = append(p.edits, sourceEdit{start: file.Offset(node.Pos()), end: file.Offset(node.End()), text: text})
}

func (p *lowerPass) freeName(prefix string) string {
	for index := 1; ; index++ {
		candidate := fmt.Sprintf("%s%d", prefix, index)
		if !p.names[candidate] {
			p.names[candidate] = true
			return candidate
		}
	}
}

// timeConstants 是 workflow 代码里可用的 time 包常量；生成期折叠成字面量。
var timeConstants = map[string]any{
	"Nanosecond":  int64(1),
	"Microsecond": int64(1000),
	"Millisecond": int64(1000000),
	"Second":      int64(1000000000),
	"Minute":      int64(60000000000),
	"Hour":        int64(3600000000000),

	"Layout":      "01/02 03:04:05PM '06 -0700",
	"ANSIC":       "Mon Jan _2 15:04:05 2006",
	"UnixDate":    "Mon Jan _2 15:04:05 MST 2006",
	"RubyDate":    "Mon Jan 02 15:04:05 -0700 2006",
	"RFC822":      "02 Jan 06 15:04 MST",
	"RFC822Z":     "02 Jan 06 15:04 -0700",
	"RFC850":      "Monday, 02-Jan-06 15:04:05 MST",
	"RFC1123":     "Mon, 02 Jan 2006 15:04:05 MST",
	"RFC1123Z":    "Mon, 02 Jan 2006 15:04:05 -0700",
	"RFC3339":     "2006-01-02T15:04:05Z07:00",
	"RFC3339Nano": "2006-01-02T15:04:05.999999999Z07:00",
	"Kitchen":     "3:04PM",
	"Stamp":       "Jan _2 15:04:05",
	"StampMilli":  "Jan _2 15:04:05.000",
	"StampMicro":  "Jan _2 15:04:05.000000",
	"StampNano":   "Jan _2 15:04:05.000000000",
	"DateTime":    "2006-01-02 15:04:05",
	"DateOnly":    "2006-01-02",
	"TimeOnly":    "15:04:05",
}

// wallClockReplacements 是墙钟函数 → workflow 包里的确定性替代。
var wallClockReplacements = map[string]string{
	"Now":       "workflow.Now(ctx)",
	"Since":     "workflow.Now(ctx).Sub(t)",
	"Until":     "t.Sub(workflow.Now(ctx))",
	"Sleep":     "workflow.Sleep(ctx, d)",
	"After":     "workflow.Sleep(ctx, d)",
	"AfterFunc": "workflow.Go 加 workflow.Sleep(ctx, d)",
	"Tick":      "循环里的 workflow.Sleep(ctx, d)",
	"NewTimer":  "workflow.Sleep(ctx, d)",
	"NewTicker": "循环里的 workflow.Sleep(ctx, d)",
}

// unsupportedConversions 是没有 WLang 对应写法的数值/字符串类型转换。
// int 与 int64 在 WLang 里是同一个类型，转换直接省略（见 foldProgram）。
var unsupportedConversions = map[string]bool{
	"int8": true, "int16": true, "int32": true, "uint": true, "uint8": true, "uint16": true,
	"uint32": true, "uint64": true, "uintptr": true, "byte": true, "rune": true,
	"float32": true, "float64": true, "string": true, "complex64": true, "complex128": true,
}

// reject 拒绝在 workflow 里没有确定性语义、或运行期必然失败的写法。
func (p *lowerPass) reject(fn *ast.FuncDecl) {
	// callArgs 是直接作为调用实参的表达式：&x 只在这里有意义（输出参数）。
	callArgs := map[ast.Expr]bool{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if p.err != nil {
			return false
		}
		switch x := node.(type) {
		case *ast.GoStmt:
			p.fail(x, "workflow 里不能用 go 语句；用 workflow.Go(ctx, func(ctx workflow.Context) { ... }) 起协程，用 workflow.NewWaitGroup(ctx) 等待")
		case *ast.SelectStmt:
			p.fail(x, "workflow 里不能用 select 语句；用 workflow.NewSelector(ctx) 的 AddReceive / AddFuture / AddDefault 加 Select(ctx)")
		case *ast.ChanType:
			p.fail(x, "workflow 里不能用原生 chan；用 workflow.NewChannel(ctx) 或 workflow.NewBufferedChannel(ctx, n)")
		case *ast.SendStmt:
			p.fail(x, "workflow 里不能用 ch <- v；用 workflow.Channel 的 Send(ctx, v)")
		case *ast.UnaryExpr:
			if x.Op == token.ARROW {
				p.fail(x, "workflow 里不能用 <-ch；用 workflow.Channel 的 Receive(ctx, &v)")
			}
			if x.Op == token.AND && !callArgs[x] && isOutOperand(x.X) {
				p.fail(x, "workflow 里 %s 只能直接作为调用实参（输出参数，如 Get(ctx, &out)）：Workflow 程序没有指针别名，`p := &a` 之后经 p 的读写不会作用到 a；直接使用值 a", p.source(x))
			}
		case *ast.SelectorExpr:
			p.rejectPackageUse(x)
		case *ast.CallExpr:
			p.rejectCall(x)
			for _, arg := range x.Args {
				callArgs[arg] = true
			}
		}
		return true
	})
}

// isOutOperand 报告 &x 的 x 是否为 go2wlang 当作输出参数翻译的形态（标识符或字段选择）；
// 其他取址表达式由 go2wlang 报诊断。
func isOutOperand(expr ast.Expr) bool {
	switch unparen(expr).(type) {
	case *ast.Ident, *ast.SelectorExpr:
		return true
	}
	return false
}

func (p *lowerPass) rejectPackageUse(sel *ast.SelectorExpr) {
	path := p.packageOf(sel.X)
	name := p.source(sel)
	switch {
	case path == "":
	case path == "time":
		if replacement, ok := wallClockReplacements[sel.Sel.Name]; ok {
			p.fail(sel, "workflow 里不能用 %s（墙钟时间在 replay 时不同）；用 %s", name, replacement)
			return
		}
		if _, ok := timeConstants[sel.Sel.Name]; ok || sel.Sel.Name == "Duration" || sel.Sel.Name == "Time" {
			return
		}
		p.fail(sel, "workflow 里不能用 %s；time 包可用的是常量（time.Second 等、布局常量）、time.Duration 运算与 time.Time 类型，当前时间用 workflow.Now(ctx)，等待用 workflow.Sleep(ctx, d)", name)
	case path == "sync" || path == "sync/atomic":
		if sel.Sel.Name == "WaitGroup" {
			p.fail(sel, "workflow 里不能用 %s；用 workflow.NewWaitGroup(ctx)", name)
			return
		}
		p.fail(sel, "workflow 里不能用 %s；workflow 协程是协作式调度，共享变量不需要锁，等待一组协程用 workflow.NewWaitGroup(ctx)，等待条件用 workflow.Await(ctx, cond)", name)
	case path == "math/rand" || path == "math/rand/v2" || path == "crypto/rand":
		p.fail(sel, "workflow 里不能用 %s（随机数在 replay 时不同）；用 workflow.RandomInt(ctx, max)、workflow.RandomUUID(ctx) 或 workflow.SideEffect", name)
	case path == "github.com/google/uuid":
		p.fail(sel, "workflow 里不能用 %s（生成的 UUID 在 replay 时不同）；用 workflow.RandomUUID(ctx)", name)
	case path == "os" || path == "net" || strings.HasPrefix(path, "net/") || strings.HasPrefix(path, "os/"):
		p.fail(sel, "workflow 里不能用 %s（IO 与环境读取在 replay 时不同）；把它放进 Activity，或用 workflow.SideEffect 记录一次结果", name)
	case path == "errors":
		p.fail(sel, "workflow 里不能用 %s；构造错误用 fmt.Errorf 或 workflow.NewApplicationError，判断错误用 workflow.IsApplicationError、workflow.IsCanceledError", name)
	}
}

func (p *lowerPass) rejectCall(call *ast.CallExpr) {
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return
	}
	if ident.Obj == nil && unsupportedConversions[ident.Name] && len(call.Args) == 1 {
		p.fail(call, "workflow 里不支持类型转换 %s(...)；WLang 的数值运算要求两侧类型一致，int 与 int64 可以互转，其余转换放进 Activity", ident.Name)
	}
}

// lowerBlock 处理语句级改写：只有直接位于语句列表里的语句可以展开成多条语句。
func (p *lowerPass) lowerBlock(block *ast.BlockStmt) {
	if block == nil {
		return
	}
	p.lowerStmtList(block.List)
}

func (p *lowerPass) lowerStmtList(stmts []ast.Stmt) {
	for _, stmt := range stmts {
		p.lowerListStmt(stmt)
	}
}

// lowerListStmt 处理一条位于语句列表里的语句，并递归进入它包含的语句列表与闭包体。
func (p *lowerPass) lowerListStmt(stmt ast.Stmt) {
	if p.err != nil || stmt == nil {
		return
	}
	switch s := stmt.(type) {
	case *ast.AssignStmt:
		if p.lowerMakeAssign(s) || p.lowerBlankAssign(s) {
			return
		}
		p.hoistAssertedFields(s)
	case *ast.ExprStmt, *ast.ReturnStmt, *ast.DeclStmt:
		p.hoistAssertedFields(s)
	case *ast.IncDecStmt:
		operator := "+="
		if s.Tok == token.DEC {
			operator = "-="
		}
		p.replace(s, p.source(s.X)+" "+operator+" 1")
		return
	case *ast.BlockStmt:
		p.lowerStmtList(s.List)
	case *ast.IfStmt:
		p.lowerStmtList(s.Body.List)
		switch branch := s.Else.(type) {
		case *ast.BlockStmt:
			p.lowerStmtList(branch.List)
		case *ast.IfStmt:
			p.lowerListStmt(branch)
		}
	case *ast.ForStmt:
		p.lowerLoopBody(s.Body)
	case *ast.RangeStmt:
		p.lowerLoopBody(s.Body)
	case *ast.SwitchStmt:
		p.lowerCaseBodies(s.Body)
	case *ast.TypeSwitchStmt:
		p.lowerCaseBodies(s.Body)
	case *ast.LabeledStmt:
		p.lowerListStmt(s.Stmt)
	}
	p.lowerFuncLits(stmt)
}

// lowerLoopBody 在每个循环体开头插入一条语句，再处理循环体。执行步数按执行的语句计（见解释器的
// workflowMaxBatchSteps）；循环体在 Go 里非空也可能翻译成空的 WLang 语句列表（`_ = i` 整句省略），
// 不插入时这样的循环每轮不执行任何语句，不受步数上限约束，一个 Workflow Task 会永远执行下去。
func (p *lowerPass) lowerLoopBody(body *ast.BlockStmt) {
	if hasLoopStep(body) {
		p.lowerStmtList(body.List[1:])
		return
	}
	name := p.freeName(loopStepPrefix)
	offset := p.fset.File(body.Lbrace).Offset(body.Lbrace) + 1
	p.edits = append(p.edits, sourceEdit{start: offset, end: offset, text: fmt.Sprintf(" %s := 0; _ = %s; ", name, name)})
	p.lowerStmtList(body.List)
}

// loopStepPrefix 是 lowerLoopBody 插入的计数变量的名字前缀。
const loopStepPrefix = "zzLoopStep"

// hasLoopStep 报告循环体是否已经以 lowerLoopBody 插入的语句开头（上一轮改写插入的）。
func hasLoopStep(body *ast.BlockStmt) bool {
	if len(body.List) == 0 {
		return false
	}
	assign, ok := body.List[0].(*ast.AssignStmt)
	if !ok || assign.Tok != token.DEFINE || len(assign.Lhs) != 1 {
		return false
	}
	ident, ok := assign.Lhs[0].(*ast.Ident)
	return ok && strings.HasPrefix(ident.Name, loopStepPrefix)
}

func (p *lowerPass) lowerCaseBodies(body *ast.BlockStmt) {
	if body == nil {
		return
	}
	for _, raw := range body.List {
		if clause, ok := raw.(*ast.CaseClause); ok {
			p.lowerStmtList(clause.Body)
		}
	}
}

// lowerFuncLits 进入语句里内联闭包的语句体（不深入嵌套语句，它们由 lowerListStmt 逐层处理）。
func (p *lowerPass) lowerFuncLits(stmt ast.Stmt) {
	ast.Inspect(stmt, func(node ast.Node) bool {
		if node == stmt {
			return true
		}
		switch x := node.(type) {
		case *ast.BlockStmt, *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt,
			*ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt, *ast.LabeledStmt:
			return false
		case *ast.FuncLit:
			p.lowerStmtList(x.Body.List)
			return false
		}
		return true
	})
}

// lowerMakeAssign 把 `x := make([]T, n)` 展开成空切片加循环追加零值。n 在清空 x 之前求值一次，
// `x = make([]T, len(x))` 因此得到与原切片等长的结果。带容量的 `x := make([]T, n, c)` 在此之后调用
// workflow.SetCap(&x, c)，容量与 Go 相同，`x[:k]`（k 不超过 c）按容量判断越界。
func (p *lowerPass) lowerMakeAssign(stmt *ast.AssignStmt) bool {
	if len(stmt.Lhs) != 1 || len(stmt.Rhs) != 1 || (stmt.Tok != token.DEFINE && stmt.Tok != token.ASSIGN) {
		return false
	}
	target, ok := stmt.Lhs[0].(*ast.Ident)
	if !ok || target.Name == "_" {
		return false
	}
	call, array, ok := sliceMake(stmt.Rhs[0])
	if !ok {
		return false
	}
	sized, withCap := sizedSliceMake(call), len(call.Args) == 3
	if !sized && !withCap {
		return false
	}
	typeText := p.source(array)
	var parts []string
	var size, capacity string
	if sized {
		size = p.freeName("zzMakeLen")
		parts = append(parts, fmt.Sprintf("%s := (%s)", size, p.source(call.Args[1])))
	}
	if withCap {
		capacity = p.freeName("zzMakeCap")
		parts = append(parts, fmt.Sprintf("%s := (%s)", capacity, p.source(call.Args[2])))
	}
	parts = append(parts, fmt.Sprintf("%s %s %s{}", target.Name, stmt.Tok, typeText))
	if sized {
		index := p.freeName("zzMake")
		parts = append(parts, fmt.Sprintf("for %s := 0; %s < %s; %s++ { %s = append(%s, %s) }",
			index, index, size, index, target.Name, target.Name, p.zeroLiteral(array.Elt)))
	}
	if withCap {
		parts = append(parts, fmt.Sprintf("workflow.SetCap(&%s, %s)", target.Name, capacity))
		p.needsWorkflowImport = true
	}
	p.replace(stmt, strings.Join(parts, "; "))
	return true
}

// lowerBlankAssign 把 `_ = expr`（expr 不是调用）改写成先求值再丢弃：wlang 的翻译把赋给 `_` 的非调用表达式
// 整句省略，Go 却会求值它，越界与 nil 指针取字段在 Go 里 panic。改写后同样报运行时错误。标识符、字面量与
// 包成员引用（`_ = strings.ToUpper`）保持原样；含类型断言的表达式由 hoistAssertedFields 处理，也保持原样。
func (p *lowerPass) lowerBlankAssign(stmt *ast.AssignStmt) bool {
	if len(stmt.Lhs) != 1 || len(stmt.Rhs) != 1 || stmt.Tok != token.ASSIGN {
		return false
	}
	if target, ok := stmt.Lhs[0].(*ast.Ident); !ok || target.Name != "_" {
		return false
	}
	rhs := ast.Unparen(stmt.Rhs[0])
	switch x := rhs.(type) {
	case *ast.CallExpr, *ast.Ident, *ast.BasicLit, *ast.FuncLit:
		return false
	case *ast.SelectorExpr:
		if p.packageOf(x.X) != "" {
			return false
		}
	}
	assertion := false
	ast.Inspect(rhs, func(node ast.Node) bool {
		if _, ok := node.(*ast.TypeAssertExpr); ok {
			assertion = true
		}
		return !assertion
	})
	if assertion {
		return false
	}
	value := p.freeName("zzDiscard")
	p.replace(stmt, fmt.Sprintf("%s := (%s); _ = %s", value, p.source(stmt.Rhs[0]), value))
	return true
}

// sliceMake 识别 `make([]T, ...)`。
func sliceMake(expr ast.Expr) (*ast.CallExpr, *ast.ArrayType, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) < 2 {
		return nil, nil, false
	}
	if ident, ok := call.Fun.(*ast.Ident); !ok || ident.Name != "make" || ident.Obj != nil {
		return nil, nil, false
	}
	array, ok := call.Args[0].(*ast.ArrayType)
	if !ok || array.Len != nil {
		return nil, nil, false
	}
	return call, array, true
}

// sizedSliceMake 报告 slice 的 make 是否带非字面量 0 的长度。
func sizedSliceMake(call *ast.CallExpr) bool {
	literal, ok := call.Args[1].(*ast.BasicLit)
	return !ok || literal.Value != "0"
}

// zeroLiteral 是元素类型零值的 Go 写法：内建标量用 *new(T)，引用类型与 any 用 nil，其余按 struct 写 T{}。
func (p *lowerPass) zeroLiteral(elem ast.Expr) string {
	switch x := elem.(type) {
	case *ast.Ident:
		if x.Name == "any" {
			return "nil"
		}
		if isPredeclaredTypeName(x.Name) && x.Obj == nil {
			return "*new(" + x.Name + ")"
		}
	case *ast.StarExpr, *ast.ArrayType, *ast.MapType, *ast.FuncType, *ast.InterfaceType:
		return "nil"
	}
	return p.source(elem) + "{}"
}

// hoistAssertedFields 把简单语句里的 `x.(T).Field` 改成先赋值给临时变量再取字段。
//
// 只处理求值顺序不会被改变的位置：断言的操作数不含函数调用，且不在闭包、&& / || 的右侧里。
func (p *lowerPass) hoistAssertedFields(stmt ast.Stmt) {
	var prefix []string
	var visit func(node ast.Node, guarded bool)
	visit = func(node ast.Node, guarded bool) {
		ast.Inspect(node, func(child ast.Node) bool {
			if p.err != nil || child == nil {
				return false
			}
			switch x := child.(type) {
			case *ast.FuncLit:
				return false
			case *ast.BinaryExpr:
				if x.Op == token.LAND || x.Op == token.LOR {
					visit(x.X, guarded)
					visit(x.Y, true)
					return false
				}
			case *ast.SelectorExpr:
				assertion, ok := unparen(x.X).(*ast.TypeAssertExpr)
				if !ok || assertion.Type == nil {
					return true
				}
				if guarded || containsCall(assertion.X) {
					p.fail(x, "%s 需要先把类型断言赋值给变量：`v := %s`，再写 v.%s", p.source(x), p.source(assertion), x.Sel.Name)
					return false
				}
				name := p.freeName("zzAssert")
				prefix = append(prefix, name+" := "+p.source(assertion))
				p.replace(x.X, name)
				return false
			}
			return true
		})
	}
	visit(stmt, false)
	if len(prefix) > 0 {
		file := p.fset.File(stmt.Pos())
		offset := file.Offset(stmt.Pos())
		p.edits = append(p.edits, sourceEdit{start: offset, end: offset, text: strings.Join(prefix, "; ") + "; "})
	}
}

func unparen(expr ast.Expr) ast.Expr {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.X
	}
}

func containsCall(expr ast.Expr) bool {
	found := false
	ast.Inspect(expr, func(node ast.Node) bool {
		if _, ok := node.(*ast.CallExpr); ok {
			found = true
		}
		return !found
	})
	return found
}

// lowerExprs 处理表达式级改写与只能在特定位置出现的写法的诊断。
func (p *lowerPass) lowerExprs(fn *ast.FuncDecl) {
	listed := listStatements(fn.Body)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if p.err != nil {
			return false
		}
		switch x := node.(type) {
		case *ast.SelectorExpr:
			if p.packageOf(x.X) == workflowSDKPath && x.Sel.Name == "DefaultVersion" {
				p.replace(x, "(-1)")
				return false
			}
			if assertion, ok := unparen(x.X).(*ast.TypeAssertExpr); ok && assertion.Type != nil && !p.hasEditAt(x.X) {
				p.fail(x, "%s 需要先把类型断言赋值给变量：`v := %s`，再写 v.%s", p.source(x), p.source(assertion), x.Sel.Name)
			}
		case *ast.CallExpr:
			p.lowerMake(x, listed)
		}
		return true
	})
}

// listStatements 返回直接位于语句列表里的语句集合。
func listStatements(body *ast.BlockStmt) map[ast.Stmt]bool {
	listed := map[ast.Stmt]bool{}
	ast.Inspect(body, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.BlockStmt:
			for _, stmt := range x.List {
				listed[stmt] = true
			}
		case *ast.CaseClause:
			for _, stmt := range x.Body {
				listed[stmt] = true
			}
		case *ast.CommClause:
			for _, stmt := range x.Body {
				listed[stmt] = true
			}
		}
		return true
	})
	return listed
}

func (p *lowerPass) hasEditAt(node ast.Node) bool {
	file := p.fset.File(node.Pos())
	start, end := file.Offset(node.Pos()), file.Offset(node.End())
	for _, edit := range p.edits {
		if edit.start == start && edit.end == end {
			return true
		}
	}
	return false
}

// lowerMake 把 `make(map[K]V)`、`make([]T, 0)` 改成空字面量；`make([]T, n)` 与带容量的 `make([]T, n, c)`
// 只接受独立赋值语句。
func (p *lowerPass) lowerMake(call *ast.CallExpr, listed map[ast.Stmt]bool) {
	ident, ok := call.Fun.(*ast.Ident)
	if !ok || ident.Name != "make" || ident.Obj != nil || len(call.Args) == 0 {
		return
	}
	switch target := call.Args[0].(type) {
	case *ast.MapType:
		p.replace(call, p.source(target)+"{}")
	case *ast.ArrayType:
		if p.coveredByEdit(call) {
			return
		}
		if len(call.Args) == 2 && !sizedSliceMake(call) {
			p.replace(call, p.source(target)+"{}")
			return
		}
		p.fail(call, "%s 只能写成独立的赋值语句：`x := %s`（生成器把它展开成追加零值，带容量时再设置容量）", p.source(call), p.source(call))
	}
}

// coveredByEdit 判断节点是否落在某个已登记的改写区间内。
func (p *lowerPass) coveredByEdit(node ast.Node) bool {
	file := p.fset.File(node.Pos())
	start, end := file.Offset(node.Pos()), file.Offset(node.End())
	for _, edit := range p.edits {
		if edit.start <= start && end <= edit.end {
			return true
		}
	}
	return false
}

// foldProgram 在翻译产物上完成编译期求值，使程序不带 time 包符号：
//
//   - `time.Second` 等常量换成 int64 / string 字面量；
//   - `time.Duration(x)` 与 `int(x)` 换成 x（Duration、int、int64 在 WLang 里都是 int64）；
//   - 类型名 time.Duration 换成 int64。
//
// timeAliases 是产物 import 表里指向 time 包的别名。
func foldProgram(node any, timeAliases map[string]bool) any {
	switch value := node.(type) {
	case []any:
		for index, item := range value {
			value[index] = foldProgram(item, timeAliases)
		}
		return value
	case map[string]any:
		if symbol, ok := value["symbol"].(string); ok && len(value) == 1 {
			if alias, name, found := strings.Cut(symbol, "."); found && timeAliases[alias] {
				switch constant := timeConstants[name].(type) {
				case int64:
					return map[string]any{"literal": map[string]any{"type": "int64", "value": strconv.FormatInt(constant, 10)}}
				case string:
					return map[string]any{"literal": map[string]any{"type": "string", "value": constant}}
				}
			}
		}
		if len(value) == 1 {
			for _, conversion := range []string{"Duration", "int"} {
				args, ok := value[conversion].([]any)
				if !ok || len(args) != 2 {
					continue
				}
				pkg, _ := args[0].(map[string]any)
				alias, isPkg := pkg["pkg"].(string)
				if !isPkg || len(pkg) != 1 || (conversion == "Duration" && !timeAliases[alias]) {
					continue
				}
				return foldProgram(args[1], timeAliases)
			}
		}
		if _, isLiteral := value["literal"]; isLiteral && len(value) == 1 {
			return value
		}
		for key, item := range value {
			value[key] = foldProgram(item, timeAliases)
		}
		return value
	case string:
		if alias, name, found := strings.Cut(value, "."); found && name == "Duration" && timeAliases[alias] {
			return "int64"
		}
		return value
	}
	return node
}
