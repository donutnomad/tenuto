package protocol_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageBoundaries 固定依赖矩阵之外的两条更严格的边界：
// 领域包之间相互独立（定义、执行、派发各管一个聚合），队列适配器不接触执行域。
// 各层之间的方向由 TestTargetDependencyMatrix 约束。
func TestPackageBoundaries(t *testing.T) {
	checks := []struct {
		dir       string
		forbidden []string
	}{
		{dir: "server/biz/domain/definition", forbidden: modulePackages("server/biz/domain/execution", "server/biz/domain/dispatch")},
		{dir: "server/biz/domain/execution", forbidden: modulePackages("server/biz/domain/dispatch")},
		{dir: "server/biz/domain/dispatch", forbidden: modulePackages("server/biz/domain/execution")},
		{dir: "server/infra/taskqueue/taskq", forbidden: modulePackages("server/biz/domain/execution")},
	}
	for _, check := range checks {
		for _, imp := range internalImports(t, check.dir) {
			for _, forbidden := range check.forbidden {
				if packagePathForbidden(modulePath+imp.target, forbidden) {
					t.Errorf("%s imports %s across package boundary", imp.file, imp.target)
				}
			}
		}
	}
}

func TestEngineRunWritesUseExecutionPackage(t *testing.T) {
	mutations := map[string]bool{
		"Append": true, "AppendIdempotent": true,
		"AppendWorkflowTaskScheduled": true, "AppendWorkflowTaskClosed": true,
		"AppendWorkflowTaskCompleted": true, "AppendWorkflowTaskCompletedWithEvents": true,
		"Create": true, "CreateRun": true, "Update": true, "UpdateRun": true,
		"UpsertActivityState": true, "UpsertTimerState": true, "UpsertChildWorkflowState": true,
		"SetCurrentRunCAS": true, "SetResetRunID": true,
	}
	root := filepath.Join("..", "server", "biz", "service", "engine")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// testdata 不属于构建，测试也会在里面建临时目录。
		if entry.IsDir() && entry.Name() == "testdata" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !mutations[selector.Sel.Name] {
				return true
			}
			if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == "tx" {
				t.Errorf("%s writes Run storage directly; use server/biz/domain/execution", fileSet.Position(call.Pos()))
			}
			if receiver, ok := selector.X.(*ast.SelectorExpr); ok && receiver.Sel.Name == "mgr" {
				t.Errorf("%s writes Run storage directly; use server/biz/domain/execution", fileSet.Position(call.Pos()))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func modulePackages(names ...string) []string {
	packages := make([]string, len(names))
	for i, name := range names {
		packages[i] = "github.com/donutnomad/tenuto/" + name
	}
	return packages
}

func packagePathForbidden(importPath, pattern string) bool {
	if strings.HasSuffix(pattern, "/...") {
		prefix := strings.TrimSuffix(pattern, "/...")
		return importPath == prefix || strings.HasPrefix(importPath, prefix+"/")
	}
	return importPath == pattern
}

// TestEngineWritePathsReadRunInTransaction 固定多副本下的一致性规则：写事务依据的 Run 是在这个事务里、持有
// 该 Run 的 execution state 行锁时用 ExecutionTx.GetRunForUpdate 读到的。事务之外从存储读到的 Run 可能已被
// 另一个副本改过，不能带进写事务。
//
// 检查 server/biz/service/engine 的非测试代码：
//   - 在事务外从存储读取 Run（mgr.Get、mgr.List、resolveRun、execution.ResolveRun）只出现在 runReaders 列出的函数里；
//   - runReaders 里的函数都不开事务（不调用 runInTx / RunInTx）；
//   - 开事务的函数不接收 *execution.Run 参数，也不调用 runReaders 里返回 *execution.Run 的函数。
//
// runReaders 是读路径与只取不可变标识的解析函数：读模型与查询的应答、寻址解析（Run ID、namespace、Workflow ID、
// History branch 这些创建后不变的字段）、事务外的预检与派发参数。表里的每一项都必须仍然存在。
func TestEngineWritePathsReadRunInTransaction(t *testing.T) {
	runReaders := map[string]bool{
		// 寻址：只取 Run 创建后不变的字段（ID、namespace、Workflow ID 与类型、History branch、目标 Worker、父关联）。
		"resolveRun": true, "resolveRunRef": true, "runRefOf": true, "runBranchInfo": true,
		"lockOpenWorkflowTask": true, "TaskRun": true,
		"runExists": true, "runNamespace": true, "continuedWorkflowType": true,
		"resolveActivityCommand": true, "GetWorkflowConfig": true, "activityTaskFromState": true,
		// 事务外的预检与查询：结果只决定是否继续，写入的事务按锁下读到的 Run 或 execution state 重新判定。
		"validateInteraction": true, "lookupUpdateFromHistory": true, "scheduleWorkflowTask": true,
		// 读路径：等待 Run 结束的应答；重发的启动请求应答它开启的 Run 链的 current Run。
		"closedRun": true, "runStartedByRequest": true,
		// 准备：按 Run 构造派生 Run、准备派发或选择要触发的定时器；提交的事务在锁下重读 Run。
		"pollWorkflowTaskOnce": true, "prepareWorkflowFailure": true, "prepareRetryRun": true,
		"prepareContinuedRun": true, "prepareReset": true,
		// 扫描：列出要补偿或触发的 Run，每个 Run 的写入各自在事务内读取。
		"forEachRunningRunPage": true, "reconcileRun": true, "recoverChildWorkflow": true,
		"FireOverdueTimers": true, "Reconcile": true,
	}
	runReads := func(call *ast.CallExpr) bool {
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		switch selector.Sel.Name {
		case "resolveRun":
			return true
		case "ResolveRun":
			pkg, ok := selector.X.(*ast.Ident)
			return ok && pkg.Name == "execution"
		case "Get", "List":
			receiver, ok := selector.X.(*ast.SelectorExpr)
			return ok && receiver.Sel.Name == "mgr"
		}
		return false
	}
	opensTx := func(call *ast.CallExpr) bool {
		selector, ok := call.Fun.(*ast.SelectorExpr)
		return ok && (selector.Sel.Name == "runInTx" || selector.Sel.Name == "RunInTx")
	}
	type funcInfo struct {
		pos         token.Position
		readsRun    bool
		opensTx     bool
		returnsRun  bool
		runParam    bool
		calledNames map[string]bool
	}
	isRunType := func(expr ast.Expr) bool {
		star, ok := expr.(*ast.StarExpr)
		if !ok {
			return false
		}
		selector, ok := star.X.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		return ok && pkg.Name == "execution" && selector.Sel.Name == "Run"
	}
	funcs := map[string]*funcInfo{}
	root := filepath.Join("..", "server", "biz", "service", "engine")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// testdata 不属于构建，测试也会在里面建临时目录。
		if entry.IsDir() && entry.Name() == "testdata" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileSet := token.NewFileSet()
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			info := &funcInfo{pos: fileSet.Position(fn.Pos()), calledNames: map[string]bool{}}
			for _, field := range fn.Type.Params.List {
				info.runParam = info.runParam || isRunType(field.Type)
			}
			if fn.Type.Results != nil {
				for _, field := range fn.Type.Results.List {
					info.returnsRun = info.returnsRun || isRunType(field.Type)
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				info.readsRun = info.readsRun || runReads(call)
				info.opensTx = info.opensTx || opensTx(call)
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					info.calledNames[fun.Sel.Name] = true
				case *ast.Ident:
					info.calledNames[fun.Name] = true
				}
				return true
			})
			// 同名的函数（不同接收者上的方法）合并判定，任何一个违反规则都报出。
			if existing, ok := funcs[fn.Name.Name]; ok {
				info.readsRun = info.readsRun || existing.readsRun
				info.opensTx = info.opensTx || existing.opensTx
				info.returnsRun = info.returnsRun || existing.returnsRun
				info.runParam = info.runParam || existing.runParam
				for called := range existing.calledNames {
					info.calledNames[called] = true
				}
			}
			funcs[fn.Name.Name] = info
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range runReaders {
		if info, ok := funcs[name]; !ok || !info.readsRun {
			t.Errorf("runReaders lists %s, which no longer reads a Run outside a transaction", name)
		}
	}
	for name, info := range funcs {
		if info.readsRun && !runReaders[name] {
			t.Errorf("%s: %s reads a Run outside a transaction; read it with ExecutionTx.GetRunForUpdate inside the transaction, or add a read-only function to runReaders", info.pos, name)
		}
		if runReaders[name] && info.opensTx {
			t.Errorf("%s: %s is in runReaders and opens a transaction", info.pos, name)
		}
		if !info.opensTx {
			continue
		}
		if info.runParam {
			t.Errorf("%s: %s opens a transaction with a *execution.Run parameter read outside it", info.pos, name)
		}
		for called := range info.calledNames {
			if reader, ok := funcs[called]; ok && runReaders[called] && reader.returnsRun {
				t.Errorf("%s: %s opens a transaction and calls %s, which returns a Run read outside a transaction", info.pos, name, called)
			}
		}
	}
}
