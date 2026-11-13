package protocol_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// userFacingSDKPackages 是用户 import 的全部 SDK 包。sdk/registration 留在这里，因为生成器写出的
// zz_<pkg>_artifacts_gen.go 返回 registration.Definition。
var userFacingSDKPackages = []string{
	"sdk/activity",
	"sdk/apperror",
	"sdk/client",
	"sdk/registration",
	"sdk/worker",
	"sdk/workflow",
}

// sdk/ 下除 sdk/internal 之外、含非测试 Go 文件的目录，恰好是 userFacingSDKPackages。
func TestUserFacingSDKPackagesAreExactlyTheListedOnes(t *testing.T) {
	var found []string
	err := filepath.WalkDir(filepath.Join("..", "sdk"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, ".."+string(filepath.Separator)))
		if entry.IsDir() {
			if rel == "sdk/internal" || entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			dir := filepath.ToSlash(filepath.Dir(rel))
			if !slices.Contains(found, dir) {
				found = append(found, dir)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(found)
	if !slices.Equal(found, userFacingSDKPackages) {
		t.Fatalf("user-facing sdk packages = %v, want %v", found, userFacingSDKPackages)
	}
}

// SDK 不链接服务端：sdk 的非测试代码不 import server/ 下的任何包。
func TestSDKDoesNotImportServer(t *testing.T) {
	for _, imp := range internalImports(t, "sdk") {
		if hasDirPrefix(imp.target, []string{"server"}) {
			t.Errorf("%s imports %s: sdk must not depend on the server", imp.file, imp.target)
		}
	}
}

// 服务端（含测试）不使用 SDK 的实现细节：要连接 Server 的代码经 sdk/client 与 sdk/worker。
func TestServerDoesNotImportSDKInternals(t *testing.T) {
	for _, imp := range moduleImports(t, "server", true) {
		if hasDirPrefix(imp.target, []string{"sdk/internal"}) {
			t.Errorf("%s imports %s: server code uses the public sdk/client and sdk/worker", imp.file, imp.target)
		}
	}
}

// sdk/client 与 sdk/worker 的依赖是单向的：Worker 经 Client 的连接工作，Client 不依赖 Worker。
func TestSDKClientDoesNotDependOnWorker(t *testing.T) {
	for _, imp := range internalImports(t, "sdk/client") {
		if hasDirPrefix(imp.target, []string{"sdk/worker", "internal/worker", "internal/executor"}) {
			t.Errorf("%s imports %s: sdk/client must not depend on the worker", imp.file, imp.target)
		}
	}
}

// 示例的应用代码只用公开 SDK：不 import server/、模块的 internal/ 与 sdk/internal/。
func TestExamplesUseOnlyThePublicSDK(t *testing.T) {
	for _, imp := range internalImports(t, "docs/examples") {
		if hasDirPrefix(imp.target, []string{"server", "internal", "sdk/internal"}) {
			t.Errorf("%s imports %s: example application code uses only the public SDK", imp.file, imp.target)
		}
	}
}

// Worker 库在 sdk/worker，原来的顶层 worker/ 与服务端目录里的 HTTP 客户端都已删除。
func TestRemovedSDKLocationsStayRemoved(t *testing.T) {
	for _, dir := range []string{"worker", "server/http/runtimeclient"} {
		if _, err := os.Stat(filepath.Join("..", filepath.FromSlash(dir))); !os.IsNotExist(err) {
			t.Errorf("%s must not exist: the SDK lives under sdk/", dir)
		}
	}
}

// moduleImports 返回 dir 下 Go 文件对本模块其他包的 import；withTests 为 true 时包含测试文件。
func moduleImports(t *testing.T, dir string, withTests bool) []internalImport {
	t.Helper()
	root := filepath.Join("..", filepath.FromSlash(dir))
	var out []internalImport
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "testdata" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || (!withTests && strings.HasSuffix(path, "_test.go")) {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(strings.TrimPrefix(path, ".."+string(filepath.Separator)))
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if target, ok := strings.CutPrefix(importPath, modulePath); ok {
				out = append(out, internalImport{file: rel, pkg: filepath.ToSlash(filepath.Dir(rel)), target: target})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}
