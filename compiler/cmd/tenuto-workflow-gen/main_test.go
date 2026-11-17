package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/compiler/artifact"
	"github.com/donutnomad/tenuto/compiler/sdkgen"
	"github.com/donutnomad/tenuto/program"
)

// fixtureDir 返回 sdkgen 测试 fixture 的绝对路径。
func fixtureDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "sdkgen", "testdata", "orders"))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// repoRoot 返回仓库根（go.mod 所在目录）。
func repoRoot(t *testing.T) string {
	t.Helper()
	current := fixtureDir(t)
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			t.Fatal("找不到 go.mod")
		}
		current = parent
	}
}

// copyFixture 把 fixture 复制到仓库内的临时目录（模块解析与 packages.Load 需要）。
func copyFixture(t *testing.T) string {
	t.Helper()
	return copyFixtureFrom(t, "orders")
}

// copyFixtureFrom 把 sdkgen testdata 下的指定 fixture 复制到仓库内临时目录，
// 并把 import path 改写到副本路径（go/packages 需要可解析的包）。
func copyFixtureFrom(t *testing.T, fixture string) string {
	t.Helper()
	root := repoRoot(t)
	source := filepath.Join(root, "compiler", "sdkgen", "testdata", fixture)
	sourcePath := "github.com/donutnomad/tenuto/compiler/sdkgen/testdata/" + fixture
	tempRoot := filepath.Join(root, "compiler", "sdkgen", "testdata")
	target, err := os.MkdirTemp(tempRoot, "tmp-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(target) })
	relative, err := filepath.Rel(root, target)
	if err != nil {
		t.Fatal(err)
	}
	targetPath := "github.com/donutnomad/tenuto/" + filepath.ToSlash(relative)
	if err := copyTree(source, target, sourcePath, targetPath); err != nil {
		t.Fatal(err)
	}
	return target
}

func copyTree(source, target, sourcePath, targetPath string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		from := filepath.Join(source, entry.Name())
		to := filepath.Join(target, entry.Name())
		if entry.IsDir() {
			if err := os.MkdirAll(to, 0o755); err != nil {
				return err
			}
			if err := copyTree(from, to, sourcePath, targetPath); err != nil {
				return err
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(from)
		if err != nil {
			return err
		}
		rewritten := strings.ReplaceAll(string(data), sourcePath, targetPath)
		if err := os.WriteFile(to, []byte(rewritten), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// generate 跑一次 generate 子命令，返回 stdout 与错误。
func generate(t *testing.T, options generateOptions, dir string) (string, error) {
	t.Helper()
	var stdout bytes.Buffer
	options.stdout = &stdout
	_, err := generatePackage(context.Background(), dir, options)
	return stdout.String(), err
}

// TestGenerateRejectsDirectActivityCall 固定 CLI 失败边界：
// 直调 @Activity 实现必须在生成阶段报出桥名；失败时不写任何 JSON 产物，
// 已写出的桥文件必须是完整文件（不是半写）。
func TestGenerateRejectsDirectActivityCall(t *testing.T) {
	dir := copyFixtureFrom(t, "directcall")
	_, err := generate(t, generateOptions{}, dir)
	if err == nil || !strings.Contains(err.Error(), "CallCharge") {
		t.Fatalf("generate error = %v, want direct-call diagnostic naming CallCharge", err)
	}
	for _, name := range []string{"workflow.json", "activities.json"} {
		if _, statErr := os.Stat(filepath.Join(dir, name)); statErr == nil {
			t.Fatalf("%s must not be written when generation fails", name)
		}
	}
	bridges, globErr := filepath.Glob(filepath.Join(dir, "activities", "zz_*_contract_gen.go"))
	if globErr != nil || len(bridges) != 1 {
		t.Fatalf("expected one complete bridge, got %v (%v)", bridges, globErr)
	}
	data, readErr := os.ReadFile(bridges[0])
	if readErr != nil {
		t.Fatal(readErr)
	}
	content := string(data)
	for _, want := range []string{"DO NOT EDIT", "func CallCharge"} {
		if !strings.Contains(content, want) {
			t.Fatalf("bridge is incomplete, missing %q", want)
		}
	}
}

// TestGenerateContractPackageAndWatch 覆盖纯契约包首次生成、watch 更新及写入失败。
func TestGenerateContractPackageAndWatch(t *testing.T) {
	// Happy Path: 首次生成与 watch 使用相同入口，纯契约包只写桥。
	dir := filepath.Join(copyFixture(t), "payments")
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run([]string{"generate", "-url", "http://127.0.0.1:1", dir}, &stdout, &stderr); code != 0 {
		t.Fatalf("contract generate = %d: %s", code, stderr.String())
	}
	result, err := generatePackage(context.Background(), dir, generateOptions{stdout: &stdout})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Bridges) != 1 || len(result.WatchFiles) == 0 {
		t.Fatalf("missing contract watch metadata: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(dir, "workflow.json")); !os.IsNotExist(err) {
		t.Fatalf("contract package must only generate bridges: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var output syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- watchAllDirs(ctx, []string{dir}, []sdkgen.PackageResult{result}, generateOptions{stdout: &output}, &output, 20*time.Millisecond)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("watch: %v", err)
		}
	})
	waitForOutput(t, &output, "watching", "contract watch did not start")
	source := filepath.Join(dir, "payments.go")
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, bytes.ReplaceAll(data, []byte("SettleRemote"), []byte("ConfirmRemote")), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, &output, "generated ", "contract bridge was not regenerated")
	bridge, err := os.ReadFile(result.Bridges[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(bridge, []byte("func ConfirmRemoteActivity")) || output.contains("generation failed") {
		t.Fatalf("contract watch failed: %s\n%s", bridge, output.String())
	}
}

// TestGenerateContractWriteFailure 覆盖契约桥写入失败时 CLI 返回错误。
func TestGenerateContractWriteFailure(t *testing.T) {
	// Error Case: 目标文件路径被目录占用时，生成命令返回失败。
	dir := filepath.Join(copyFixture(t), "payments")
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(plan.Bridges[0].Path, 0o755); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"generate", dir}, &stdout, &stderr); code != 1 {
		t.Fatalf("write failure exit = %d, stderr=%s", code, stderr.String())
	}
}

// TestGenerateWorkflowPackageIgnoresToolPackage 覆盖排序靠前的 ignore 工具文件。
func TestGenerateWorkflowPackageIgnoresToolPackage(t *testing.T) {
	// Edge Case: artifacts 包名来自 Workflow 文件，工具文件声明 package main。
	dir := copyFixture(t)
	tool := "//go:build ignore\n\npackage main\n\nfunc main() {}\n"
	if err := os.WriteFile(filepath.Join(dir, "aaa_tool.go"), []byte(tool), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	result, err := generatePackage(context.Background(), dir, generateOptions{stdout: &stdout})
	if err != nil {
		t.Fatal(err)
	}
	if result.PackageName != "orders" || !bytes.Contains(result.Embedded.Content, []byte("package orders")) {
		t.Fatalf("wrong artifacts package: %s\n%s", result.PackageName, result.Embedded.Content)
	}
}

// TestGenerateIsIdempotent 锁住「内容相同不重写」：第二次运行只打印 up-to-date。
func TestGenerateIsIdempotent(t *testing.T) {
	dir := copyFixture(t)
	first, err := generate(t, generateOptions{}, dir)
	if err != nil {
		t.Fatalf("首次生成失败: %v\n%s", err, first)
	}
	if !strings.Contains(first, "generated ") || !strings.Contains(first, "output: "+dir) {
		t.Fatalf("首次生成输出缺少写出路径:\n%s", first)
	}
	second, err := generate(t, generateOptions{}, dir)
	if err != nil {
		t.Fatalf("二次生成失败: %v\n%s", err, second)
	}
	if strings.Contains(second, "generated ") {
		t.Fatalf("二次生成不应重写文件:\n%s", second)
	}
	if !strings.Contains(second, "up-to-date ") {
		t.Fatalf("二次生成应报告 up-to-date:\n%s", second)
	}
	for _, name := range []string{"workflow.json", "activities.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("缺少产物 %s: %v", name, err)
		}
	}
	// WLang 程序只放在 workflow.json 的 program 字段里，不单独写文件。
	if _, err := os.Stat(filepath.Join(dir, "wlang.json")); !os.IsNotExist(err) {
		t.Fatalf("不应写出 wlang.json: %v", err)
	}
	loaded, err := artifact.LoadDir(dir)
	if err != nil {
		t.Fatalf("产物加载失败: %v", err)
	}
	if loaded.Workflow.ID != "orders-demo" || len(loaded.Workflow.Program.Program) == 0 || len(loaded.Activities) != 6 {
		t.Fatalf("产物内容异常: id=%s program=%d activities=%d",
			loaded.Workflow.ID, len(loaded.Workflow.Program.Program), len(loaded.Activities))
	}
}

// TestGenerateFailureKeepsPreviousArtifacts 锁住「生成失败保留上次成功产物」。
func TestGenerateFailureKeepsPreviousArtifacts(t *testing.T) {
	dir := copyFixture(t)
	if stdout, err := generate(t, generateOptions{}, dir); err != nil {
		t.Fatalf("首次生成失败: %v\n%s", err, stdout)
	}
	before, err := os.ReadFile(filepath.Join(dir, "workflow.json"))
	if err != nil {
		t.Fatal(err)
	}
	// 破坏注解：未知 policy。
	source := filepath.Join(dir, "workflow.go")
	content, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(string(content), "policy=auto_upgrade", "policy=latest", 1)
	if err := os.WriteFile(source, []byte(broken), 0o644); err != nil {
		t.Fatal(err)
	}
	stdout, err := generate(t, generateOptions{}, dir)
	if err == nil {
		t.Fatalf("破坏注解后应失败\n%s", stdout)
	}
	if !strings.Contains(err.Error(), "policy=") {
		t.Fatalf("错误信息应指出 policy: %v", err)
	}
	after, readErr := os.ReadFile(filepath.Join(dir, "workflow.json"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("生成失败不应改动上次成功的产物")
	}
	// 修复后继续同步。
	if err := os.WriteFile(source, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if stdout, err := generate(t, generateOptions{}, dir); err != nil {
		t.Fatalf("修复后应重新生成: %v\n%s", err, stdout)
	}
}

// TestPublishAndRegisterClients 覆盖两个 HTTP 入口：默认只登记，-publish 才激活。
func TestPublishAndRegisterClients(t *testing.T) {
	dir := copyFixture(t)
	if stdout, err := generate(t, generateOptions{}, dir); err != nil {
		t.Fatalf("生成失败: %v\n%s", err, stdout)
	}
	var calls []struct {
		path string
		body map[string]any
	}
	var mutex sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("解析请求失败: %v", err)
		}
		mutex.Lock()
		calls = append(calls, struct {
			path string
			body map[string]any
		}{request.URL.Path, body})
		mutex.Unlock()
		if request.URL.Path == artifact.RegisterPath {
			writer.WriteHeader(http.StatusAccepted)
			_, _ = writer.Write([]byte(`{"registered":"orders-demo"}`))
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"version":"abc"}`))
	}))
	defer server.Close()

	t.Run("默认只调用 artifacts", func(t *testing.T) {
		mutex.Lock()
		calls = nil
		mutex.Unlock()
		stdout, err := generate(t, generateOptions{url: server.URL, namespace: "orders-ns"}, dir)
		if err != nil {
			t.Fatalf("登记失败: %v\n%s", err, stdout)
		}
		mutex.Lock()
		defer mutex.Unlock()
		if len(calls) != 1 || calls[0].path != artifact.RegisterPath {
			t.Fatalf("应只调用 %s，实际 %+v", artifact.RegisterPath, calls)
		}
		if calls[0].body["namespace"] != "orders-ns" {
			t.Fatalf("请求缺少 namespace: %+v", calls[0].body)
		}
		// 服务端只接收 namespace 与 workflow。
		if _, ok := calls[0].body["workflow"]; !ok || len(calls[0].body) != 2 {
			t.Fatalf("登记请求应只有 namespace 与 workflow: %+v", calls[0].body)
		}
		// 打印的内容版本就是服务端为该 workflow.json 计算的版本。
		loaded, err := artifact.LoadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		want, err := program.WorkflowConfigVersion(loaded.Workflow)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(stdout, "content version "+want) {
			t.Fatalf("输出缺少服务端内容版本 %s:\n%s", want, stdout)
		}
	})

	t.Run("publish 调用 register", func(t *testing.T) {
		mutex.Lock()
		calls = nil
		mutex.Unlock()
		stdout, err := generate(t, generateOptions{url: server.URL, namespace: "orders-ns", publish: true}, dir)
		if err != nil {
			t.Fatalf("发布失败: %v\n%s", err, stdout)
		}
		mutex.Lock()
		defer mutex.Unlock()
		if len(calls) != 1 || calls[0].path != artifact.PublishPath {
			t.Fatalf("应只调用 %s，实际 %+v", artifact.PublishPath, calls)
		}
		if _, ok := calls[0].body["workflow"]; !ok {
			t.Fatalf("发布请求缺少 workflow: %+v", calls[0].body)
		}
	})

	t.Run("非 2xx 打印响应体并失败", func(t *testing.T) {
		failing := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusBadRequest)
			_, _ = writer.Write([]byte("workflow id conflict"))
		}))
		defer failing.Close()
		_, err := generate(t, generateOptions{url: failing.URL, namespace: "orders-ns"}, dir)
		if err == nil {
			t.Fatal("非 2xx 应失败")
		}
		if !strings.Contains(err.Error(), "workflow id conflict") || !strings.Contains(err.Error(), "400") {
			t.Fatalf("错误应包含状态码与响应体: %v", err)
		}
	})
}

// TestWatchRegeneratesAfterSaveAndRecovers 覆盖 watch 主路径：保存触发重新生成，
// 失败保留上次成功产物并打印诊断，修复后继续同步。
func TestWatchRegeneratesAfterSaveAndRecovers(t *testing.T) {
	dir := copyFixture(t)
	if stdout, err := generate(t, generateOptions{}, dir); err != nil {
		t.Fatalf("首次生成失败: %v\n%s", err, stdout)
	}
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		t.Fatal(err)
	}
	dirs := watchDirsOf(sdkgen.PackageResult{Dir: dir, WatchFiles: plan.WatchFiles, Bridges: plan.Bridges})
	if len(dirs) < 3 {
		t.Fatalf("应监听入口、同包与契约包，实际 %v", dirs)
	}

	before, err := os.ReadFile(filepath.Join(dir, "workflow.json"))
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "workflow.go")
	content, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var watchOutput syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = watchDirsWithInterval(ctx, func() []string { return dirs }, func() error {
			_, err := generatePackage(ctx, dir, generateOptions{stdout: &watchOutput})
			return err
		}, &watchOutput, 20*time.Millisecond)
	}()
	waitForOutput(t, &watchOutput, "watching", "watch 没有启动")

	// 1) 破坏注解 → 失败诊断，产物保持不变。
	if err := os.WriteFile(source, []byte(strings.Replace(string(content), "policy=auto_upgrade", "policy=latest", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, &watchOutput, "policy=", "等不到注解失败诊断")
	after, err := os.ReadFile(filepath.Join(dir, "workflow.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("watch 失败时不应改动上次成功产物")
	}

	// 2) 修复并改名 → 重新生成成功，workflow.json 反映新名字。
	fixed := strings.Replace(string(content), `name="Orders Demo"`, `name="Orders Demo v2"`, 1)
	if fixed == string(content) {
		t.Fatal("fixture 里找不到 name=\"Orders Demo\"")
	}
	if err := os.WriteFile(source, []byte(fixed), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, &watchOutput, "output: "+dir, "等不到修复后的重新生成")
	recovered, err := os.ReadFile(filepath.Join(dir, "workflow.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(recovered), "Orders Demo v2") {
		t.Fatalf("修复后产物没有更新:\n%s", recovered)
	}
	cancel()
	<-done
}

// TestWatchRegeneratesAfterSaveDuringGeneration: 生成进行中保存的修改在生成结束后再触发一次生成，
// 不会被当成新的基线吞掉。
func TestWatchRegeneratesAfterSaveDuringGeneration(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "workflow.go")
	if err := os.WriteFile(source, []byte("package p\n// v1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runs := make(chan int, 4)
	count := 0
	var output syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = watchDirsWithInterval(ctx, func() []string { return []string{dir} }, func() error {
			count++
			if count == 1 {
				// 第一次生成期间再保存一次。
				if err := os.WriteFile(source, []byte("package p\n// v3\n"), 0o644); err != nil {
					return err
				}
			}
			runs <- count
			return nil
		}, &output, 10*time.Millisecond)
	}()
	waitForOutput(t, &output, "watching", "watch 没有启动")
	if err := os.WriteFile(source, []byte("package p\n// v2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for want := 1; want <= 2; want++ {
		select {
		case got := <-runs:
			if got != want {
				t.Fatalf("第 %d 次生成编号为 %d", want, got)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("等不到第 %d 次生成：生成期间的保存被吞掉", want)
		}
	}
	cancel()
	<-done
}

// waitForOutput 轮询 watch 输出，直到出现指定片段。
// watchGenerationTimeout 是等待 watch 完成一次生成的上限。生成要加载并类型检查 SDK 依赖，
// CI 上与其他测试包并行时单次可达 10 s 以上；成功时立即返回，不增加正常耗时。
const watchGenerationTimeout = 90 * time.Second

func waitForOutput(t *testing.T, output *syncBuffer, fragment, message string) {
	t.Helper()
	deadline := time.Now().Add(watchGenerationTimeout)
	for time.Now().Before(deadline) {
		if output.contains(fragment) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("超时: %s\nwatch 输出:\n%s", message, output.String())
}

// syncBuffer 是并发安全的输出缓冲。
type syncBuffer struct {
	mutex sync.Mutex
	data  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.data.Write(p)
}

func (b *syncBuffer) String() string {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.data.String()
}

func (b *syncBuffer) contains(fragment string) bool {
	return strings.Contains(b.String(), fragment)
}

// TestWatchAllDirsRegeneratesEveryDirectory 固定多目录 + -watch 语义：
// 首轮必须生成全部目录，任一目录保存后重新生成全部目录（不是只监听第一个）。
func TestWatchAllDirsRegeneratesEveryDirectory(t *testing.T) {
	first := copyFixture(t)
	second := copyFixture(t)
	dirs := []string{first, second}
	results := make([]sdkgen.PackageResult, 0, len(dirs))
	for _, dir := range dirs {
		stdout, err := generate(t, generateOptions{}, dir)
		if err != nil {
			t.Fatalf("首次生成失败: %v\n%s", err, stdout)
		}
		plan, err := sdkgen.Scan(dir)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, sdkgen.PackageResult{Dir: dir, WatchFiles: plan.WatchFiles, Bridges: plan.Bridges})
	}
	for _, dir := range dirs {
		if _, err := os.Stat(filepath.Join(dir, "workflow.json")); err != nil {
			t.Fatalf("首轮没有生成 %s: %v", dir, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var watchOutput syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = watchAllDirs(ctx, dirs, results, generateOptions{stdout: &watchOutput}, &watchOutput, 20*time.Millisecond)
	}()
	waitForOutput(t, &watchOutput, "watching", "多目录 watch 没有启动")

	source := filepath.Join(second, "workflow.go")
	content, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(source, []byte(strings.Replace(string(content), "Same Workflow", "Same Workflow v2", 1)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, &watchOutput, "output: "+second, "第二个目录没有重新生成")
	if !strings.Contains(watchOutput.String(), "output: "+first) {
		t.Fatalf("第一个目录没有随多目录 watch 一起重新生成:\n%s", watchOutput.String())
	}
}

// TestWatchAllDirsWatchesDirectoryThatFailedFirstGeneration 固定首轮失败目录的监听：
// 修好该目录自己的源码就要触发重新生成，不依赖其它目录的改动。
func TestWatchAllDirsWatchesDirectoryThatFailedFirstGeneration(t *testing.T) {
	healthy := copyFixture(t)
	broken := copyFixture(t)
	brokenSource := filepath.Join(broken, "workflow.go")
	original, err := os.ReadFile(brokenSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(brokenSource, []byte(strings.Replace(string(original), "policy=auto_upgrade", "policy=latest", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := generate(t, generateOptions{}, broken); err == nil {
		t.Fatal("broken fixture generated successfully")
	}
	if _, err := generate(t, generateOptions{}, healthy); err != nil {
		t.Fatalf("首次生成失败: %v", err)
	}
	plan, err := sdkgen.Scan(healthy)
	if err != nil {
		t.Fatal(err)
	}
	results := []sdkgen.PackageResult{{Dir: healthy, WatchFiles: plan.WatchFiles, Bridges: plan.Bridges}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var watchOutput syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = watchAllDirs(ctx, []string{healthy, broken}, results, generateOptions{stdout: &watchOutput}, &watchOutput, 20*time.Millisecond)
	}()
	waitForOutput(t, &watchOutput, "watching", "多目录 watch 没有启动")

	if err := os.WriteFile(brokenSource, original, 0o644); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, &watchOutput, "output: "+broken, "首轮失败的目录修好后没有重新生成")

	// 修好后的生成结果带来了新的依赖目录：改动其中的契约源码也要触发重新生成。
	plan, err = sdkgen.Scan(broken)
	if err != nil {
		t.Fatal(err)
	}
	dependency := ""
	for _, file := range plan.WatchFiles {
		if filepath.Dir(file) != broken {
			dependency = file
			break
		}
	}
	if dependency == "" {
		t.Fatalf("fixture 没有包目录之外的依赖: %v", plan.WatchFiles)
	}
	data, err := os.ReadFile(dependency)
	if err != nil {
		t.Fatal(err)
	}
	before := strings.Count(watchOutput.String(), "output: "+broken)
	time.Sleep(60 * time.Millisecond)
	if err := os.WriteFile(dependency, append(data, []byte("\n// edited\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(watchGenerationTimeout)
	for strings.Count(watchOutput.String(), "output: "+broken) <= before {
		if time.Now().After(deadline) {
			t.Fatalf("修好后新增的依赖目录没有被监听\nwatch 输出:\n%s", watchOutput.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestWatchAllDirsContinuesAfterOneDirectoryFails 固定失败隔离：
// 一个目录生成失败不能截断其它目录，否则它们的修改会被 watch 哈希吞掉。
func TestWatchAllDirsContinuesAfterOneDirectoryFails(t *testing.T) {
	first := copyFixture(t)
	second := copyFixture(t)
	dirs := []string{first, second}
	results := make([]sdkgen.PackageResult, 0, len(dirs))
	for _, dir := range dirs {
		if _, err := generate(t, generateOptions{}, dir); err != nil {
			t.Fatalf("首次生成失败: %v", err)
		}
		plan, err := sdkgen.Scan(dir)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, sdkgen.PackageResult{Dir: dir, WatchFiles: plan.WatchFiles, Bridges: plan.Bridges})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var watchOutput syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = watchAllDirs(ctx, dirs, results, generateOptions{stdout: &watchOutput}, &watchOutput, 20*time.Millisecond)
	}()
	waitForOutput(t, &watchOutput, "watching", "多目录 watch 没有启动")

	firstSource := filepath.Join(first, "workflow.go")
	content, err := os.ReadFile(firstSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(firstSource, []byte(strings.Replace(string(content), "policy=auto_upgrade", "policy=latest", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	secondSource := filepath.Join(second, "workflow.go")
	content, err = os.ReadFile(secondSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(secondSource, []byte(strings.Replace(string(content), "Same Workflow", "Same Workflow After Failure", 1)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(watchGenerationTimeout)
	for time.Now().Before(deadline) {
		output := watchOutput.String()
		if strings.Contains(output, "generation failed") && strings.Count(output, "output: "+second) >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("second directory was not regenerated after the first failed:\n%s", watchOutput.String())
}
