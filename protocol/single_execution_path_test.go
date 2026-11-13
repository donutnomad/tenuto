package protocol_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/program"
)

// Workflow 只有一条执行路径：登记的配置携带一段 WLang 程序，Worker 的 WLang 解释器执行它。
// 下面的断言固定这条路径的结构，防止第二种执行模型（步骤节点树、逐节点求值的 handler、错误分支）回来。

// removedPackages 是已删除的第二条执行路径所在的包：步骤节点 handler 与把步骤配置编译成 WLang 的编译器。
var removedPackages = []string{path.Join("internal", "executor", "logic"), "compiler"}

func TestRemovedExecutionPathPackagesStayRemoved(t *testing.T) {
	for _, dir := range removedPackages {
		entries, err := os.ReadDir(filepath.Join("..", filepath.FromSlash(dir)))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
				t.Errorf("%s/%s: package %s was removed with the step-node execution path", dir, entry.Name(), dir)
			}
		}
	}
	err := filepath.WalkDir("..", func(file string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && (entry.Name() == "node_modules" || entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".")) && file != ".." {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(file, ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range parsed.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			for _, removed := range removedPackages {
				if importPath == modulePath+removed {
					t.Errorf("%s imports removed package %s", file, importPath)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// WorkflowConfig 直接携带程序：没有节点列表，也没有错误分支。
func TestWorkflowConfigCarriesOneProgram(t *testing.T) {
	config := reflect.TypeOf(program.WorkflowConfig{})
	field, ok := config.FieldByName("Program")
	if !ok || field.Type != reflect.TypeOf(program.WLangProgram{}) {
		t.Fatalf("WorkflowConfig.Program must be a program.WLangProgram, got %v", field.Type)
	}
	for i := range config.NumField() {
		field := config.Field(i)
		if field.Type.Kind() == reflect.Slice || field.Name == "OnError" {
			t.Errorf("WorkflowConfig.%s: a config holds one program, not a node list or an error branch", field.Name)
		}
	}
}

// Worker 的 main 协程只执行 WLang 程序：executeWorkflow 只调用 executeWLang，没有按节点类型分派的求值器。
func TestWorkerMainBodyRunsOnlyTheWLangProgram(t *testing.T) {
	dir := filepath.Join("..", "internal", "executor", "interpreter")
	packages, err := parser.ParseDir(token.NewFileSet(), dir, func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body *ast.FuncDecl
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv == nil {
					continue
				}
				if fn.Name.Name == "executeWorkflow" {
					body = fn
				}
				if strings.HasPrefix(fn.Name.Name, "execute") && fn.Name.Name != "executeWorkflow" &&
					fn.Name.Name != "executeWLang" && fn.Name.Name != "executeActivity" {
					t.Errorf("%s: executor method %s looks like a second evaluator", dir, fn.Name.Name)
				}
			}
		}
	}
	if body == nil {
		t.Fatal("WorkflowExecutor.executeWorkflow not found")
	}
	var calls []string
	ast.Inspect(body.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(sel.Sel.Name, "execute") {
			calls = append(calls, sel.Sel.Name)
		}
		return true
	})
	if len(calls) != 1 || calls[0] != "executeWLang" {
		t.Fatalf("executeWorkflow calls %v, want only executeWLang", calls)
	}
}
