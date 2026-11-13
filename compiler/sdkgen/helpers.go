package sdkgen

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	gotypes "go/types"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"golang.org/x/tools/go/packages"
)

// helperFunc 是 workflow 函数直接或间接调用的同包函数或方法。go2wlang 把它们的函数体
// 翻译进同一个程序（Options.TranslateHelpers）；生成器先对它们做与 workflow 函数相同的
// 重写与诊断。
type helperFunc struct {
	file string // 声明所在文件（绝对路径）
	key  string // 函数名，方法写成 "Type.Method"
}

// workflowHelpers 对 workflow 所在包做类型检查，按调用关系从 funcName 出发找出可达的
// 同包函数与值接收者方法，顺序与发现顺序一致。生成文件里的函数（桥 CallX、remote marker）
// 与 @Activity 实现（activityImpls）不是 helper：前者由重写替换，后者由重写报错。
//
// 同时拒绝把同包函数当成值使用：程序里只有被调用的 helper，函数值在运行期没有对应符号。
// 唯一的例外是 workflow.ExecuteActivity 的 Activity 参数，它是 Activity 的 marker。
//
// dir 为空时只检查 filename 这一个文件（GenerateFromSource）。
func workflowHelpers(src []byte, filename, dir, funcName string, activityImpls map[string]bool) ([]helperFunc, error) {
	fset, files, info, pkgPath, err := loadForHelpers(src, filename, dir)
	if err != nil {
		return nil, err
	}
	decls := map[gotypes.Object]*ast.FuncDecl{}
	generated := map[*ast.FuncDecl]bool{}
	var entry *ast.FuncDecl
	for _, file := range files {
		isGenerated := ast.IsGenerated(file) || strings.HasPrefix(filepath.Base(fset.Position(file.Pos()).Filename), "zz_")
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if obj := info.Defs[fn.Name]; obj != nil {
				decls[obj] = fn
			}
			generated[fn] = isGenerated
			if fn.Recv == nil && fn.Name.Name == funcName && samePath(fset.Position(fn.Pos()).Filename, filename) {
				entry = fn
			}
		}
	}
	if entry == nil {
		return nil, fmt.Errorf("%s: 找不到函数 %s", filename, funcName)
	}
	seen := map[*ast.FuncDecl]bool{entry: true}
	var helpers []helperFunc
	queue := []*ast.FuncDecl{entry}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		allowed := map[ast.Node]bool{}
		var walkErr error
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if walkErr != nil {
				return false
			}
			switch x := node.(type) {
			case *ast.CallExpr:
				callee := unwrapTypeArgs(x.Fun)
				allowed[callee] = true
				if isWorkflowFunc(info, callee, "ExecuteActivity") && len(x.Args) >= 2 {
					allowed[x.Args[1]] = true
				}
				decl := calledDecl(info, decls, callee, pkgPath)
				if decl == nil || seen[decl] || generated[decl] || (decl.Recv == nil && activityImpls[decl.Name.Name]) {
					return true
				}
				seen[decl] = true
				helpers = append(helpers, helperFunc{file: fset.Position(decl.Pos()).Filename, key: funcKey(decl)})
				queue = append(queue, decl)
			case *ast.Ident:
				if fnObj, ok := info.Uses[x].(*gotypes.Func); ok && !allowed[x] && fnObj.Pkg() != nil && fnObj.Pkg().Path() == pkgPath && fnObj.Type().(*gotypes.Signature).Recv() == nil {
					walkErr = fmt.Errorf("%s: 本包函数 %s 不能作为值使用：Workflow 程序只包含被调用的同包函数；需要函数值时写闭包 func(...) { ... %s(...) ... }", fset.Position(x.Pos()), x.Name, x.Name)
				}
			case *ast.SelectorExpr:
				selection := info.Selections[x]
				if selection != nil && selection.Kind() == gotypes.MethodVal && !allowed[x] && selection.Obj().Pkg() != nil && selection.Obj().Pkg().Path() == pkgPath {
					walkErr = fmt.Errorf("%s: 方法 %s 不能作为值使用：Workflow 程序只包含被调用的同包方法；需要函数值时写闭包", fset.Position(x.Pos()), x.Sel.Name)
				}
				if selection != nil && walkErr == nil {
					walkErr = rejectUnboundTimeMethod(fset, x, selection)
				}
			}
			return walkErr == nil
		})
		if walkErr != nil {
			return nil, walkErr
		}
	}
	return helpers, nil
}

// loadForHelpers 加载 helper 分析需要的语法树与类型信息。包里其他地方的类型错误不影响
// 调用关系的解析，因此忽略。
func loadForHelpers(src []byte, filename, dir string) (*token.FileSet, []*ast.File, *gotypes.Info, string, error) {
	fset := token.NewFileSet()
	if dir != "" {
		absolute, err := filepath.Abs(filename)
		if err != nil {
			return nil, nil, nil, "", err
		}
		loaded, err := packages.Load(&packages.Config{
			Dir:     dir,
			Fset:    fset,
			Overlay: map[string][]byte{absolute: src},
			Mode:    packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		}, ".")
		if err == nil && len(loaded) == 1 && loaded[0].TypesInfo != nil {
			return fset, loaded[0].Syntax, loaded[0].TypesInfo, loaded[0].PkgPath, nil
		}
	}
	file, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		return nil, nil, nil, "", err
	}
	info := &gotypes.Info{
		Defs:       map[*ast.Ident]gotypes.Object{},
		Uses:       map[*ast.Ident]gotypes.Object{},
		Selections: map[*ast.SelectorExpr]*gotypes.Selection{},
	}
	config := gotypes.Config{Importer: singleFileImporter{std: importer.Default()}, Error: func(error) {}}
	_, _ = config.Check(file.Name.Name, fset, []*ast.File{file}, info)
	return fset, []*ast.File{file}, info, file.Name.Name, nil
}

// singleFileImporter 是单文件检查（GenerateFromSource）用的 importer：标准库取编译器的导出数据；
// sdk/workflow 从源码检查，让 workflow.Now 等调用的结果有类型，time 方法的诊断才能生效。
// 源码 importer 缓存已检查的包，进程内共用一个，由 sdkSourceMu 串行。
type singleFileImporter struct {
	std gotypes.Importer
}

var (
	sdkSourceMu       sync.Mutex
	sdkSourceImporter gotypes.Importer
)

func (i singleFileImporter) Import(path string) (*gotypes.Package, error) {
	if path != workflowSDKPath {
		return i.std.Import(path)
	}
	sdkSourceMu.Lock()
	defer sdkSourceMu.Unlock()
	if sdkSourceImporter == nil {
		sdkSourceImporter = importer.ForCompiler(token.NewFileSet(), "source", nil)
	}
	return sdkSourceImporter.Import(path)
}

// calledDecl 返回调用目标在本包里的声明：普通函数或方法。
func calledDecl(info *gotypes.Info, decls map[gotypes.Object]*ast.FuncDecl, callee ast.Expr, pkgPath string) *ast.FuncDecl {
	var obj gotypes.Object
	switch x := callee.(type) {
	case *ast.Ident:
		obj = info.Uses[x]
	case *ast.SelectorExpr:
		if selection := info.Selections[x]; selection != nil && selection.Kind() == gotypes.MethodVal {
			obj = selection.Obj()
		}
	}
	fn, ok := obj.(*gotypes.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != pkgPath {
		return nil
	}
	return decls[fn]
}

// isWorkflowFunc 判断 callee 是否为 sdk/workflow 的 name 函数。
func isWorkflowFunc(info *gotypes.Info, callee ast.Expr, name string) bool {
	selector, ok := callee.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	fn, ok := info.Uses[selector.Sel].(*gotypes.Func)
	if ok && fn.Pkg() != nil {
		return fn.Pkg().Path() == workflowSDKPath
	}
	// 类型信息缺失（单文件检查找不到 SDK）时按包名判断。
	root, ok := selector.X.(*ast.Ident)
	return ok && root.Name == "workflow"
}

// unwrapTypeArgs 去掉泛型实例化 F[T](...) 的类型实参，返回被调用的函数表达式。
func unwrapTypeArgs(expr ast.Expr) ast.Expr {
	switch x := expr.(type) {
	case *ast.IndexExpr:
		return x.X
	case *ast.IndexListExpr:
		return x.X
	case *ast.ParenExpr:
		return unwrapTypeArgs(x.X)
	}
	return expr
}

// funcKey 是 findFuncDecl 使用的名字：函数名，方法为 "Type.Method"。
func funcKey(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return receiverTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func receiverTypeName(expr ast.Expr) string {
	switch x := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(x.X)
	case *ast.IndexExpr:
		return receiverTypeName(x.X)
	case *ast.IndexListExpr:
		return receiverTypeName(x.X)
	case *ast.Ident:
		return x.Name
	}
	return ""
}

func samePath(a, b string) bool {
	absA, errA := filepath.Abs(a)
	absB, errB := filepath.Abs(b)
	return errA == nil && errB == nil && filepath.Clean(absA) == filepath.Clean(absB)
}

// boundTimeMethods 是 Worker 给 workflow 代码里的 time.Time 值绑定的方法（interpreter 的 workflowTime）。
var boundTimeMethods = []string{"Add", "After", "Before", "Equal", "Format", "String", "Sub", "Unix", "UnixMilli"}

// rejectUnboundTimeMethod 拒绝 Worker 没有绑定的 time.Time 方法与全部 time.Duration 方法：
// 这些调用能通过 Go 编译，运行期却找不到方法。time.Duration 在 WLang 里是 int64 纳秒，没有方法。
func rejectUnboundTimeMethod(fset *token.FileSet, sel *ast.SelectorExpr, selection *gotypes.Selection) error {
	if selection.Kind() != gotypes.MethodVal && selection.Kind() != gotypes.MethodExpr {
		return nil
	}
	named, ok := gotypes.Unalias(derefType(selection.Recv())).(*gotypes.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != "time" {
		return nil
	}
	method := sel.Sel.Name
	switch named.Obj().Name() {
	case "Time":
		if slices.Contains(boundTimeMethods, method) {
			return nil
		}
		return fmt.Errorf("%s: time.Time 的方法 %s 在 Workflow 里不可用；可用的方法是 %s", fset.Position(sel.Sel.Pos()), method, strings.Join(boundTimeMethods, "、"))
	case "Duration":
		return fmt.Errorf("%s: time.Duration 的方法 %s 在 Workflow 里不可用：time.Duration 在 Workflow 里是 int64 纳秒，没有方法；用整数运算，例如 d / time.Millisecond", fset.Position(sel.Sel.Pos()), method)
	}
	return nil
}

func derefType(t gotypes.Type) gotypes.Type {
	if pointer, ok := t.(*gotypes.Pointer); ok {
		return pointer.Elem()
	}
	return t
}
