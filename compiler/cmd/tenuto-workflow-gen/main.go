// Command tenuto-workflow-gen 把带 @Workflow/@Activity 注解的 Go 包生成为可注册的
// workflow.json 等产物，并按需登记或发布到 runtime HTTP API。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/donutnomad/tenuto/compiler/artifact"
	"github.com/donutnomad/tenuto/compiler/sdkgen"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

const usage = `usage: tenuto-workflow-gen generate [flags] <pkgdir>...

从 <pkgdir> 里的 @Workflow/@Activity 注解生成 workflow.json，
并写出契约桥文件 zz_<pkg>_contract_gen.go。

flags:
  -watch          监听依赖源码的内容哈希，保存后重新生成；失败保留上次成功产物
  -url <base>     生成成功后提交到该服务端（默认不联网）
  -namespace <ns> 提交使用的 namespace（默认 default）
  -publish        -url 时调用 POST /runtime/workflow/register（注册并激活 latest）；
                  不带 -publish 只调用 POST /runtime/workflow/artifacts（仅登记）

environment:
  TENUTO_AUTH_TOKEN  -url 的 Server 开启 Bearer 鉴权（WORKFLOW_HTTP_AUTH_TOKEN）时随请求发送的 token
`

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "generate" {
		fmt.Fprint(stderr, usage)
		return 2
	}
	flags := flag.NewFlagSet("generate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	watch := flags.Bool("watch", false, "监听依赖源码内容哈希并重新生成")
	url := flags.String("url", "", "服务端 URL；留空表示不联网")
	namespace := flags.String("namespace", "default", "提交使用的 namespace")
	publish := flags.Bool("publish", false, "调用 register 入口（注册并激活 latest）")
	if err := flags.Parse(args[1:]); err != nil {
		return 2
	}
	dirs := flags.Args()
	if len(dirs) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	options := generateOptions{
		url:       strings.TrimSpace(*url),
		namespace: *namespace,
		publish:   *publish,
		authToken: strings.TrimSpace(os.Getenv("TENUTO_AUTH_TOKEN")),
		stdout:    stdout,
	}
	if options.url != "" && options.namespace == "" {
		fmt.Fprintln(stderr, "namespace 不能为空")
		return 2
	}
	ctx := context.Background()
	exitCode := 0
	results := make([]sdkgen.PackageResult, 0, len(dirs))
	// 先对所有目录生成一次：-watch 不能把后续目录永远挡在第一个目录的监听循环之后。
	for _, dir := range dirs {
		result, err := generatePackage(ctx, dir, options)
		if err != nil {
			fmt.Fprintf(stderr, "generation failed: %v\n", err)
			exitCode = 1
			continue
		}
		results = append(results, result)
	}
	if *watch {
		if err := watchAllDirs(ctx, dirs, results, options, stderr, watchInterval); err != nil {
			fmt.Fprintf(stderr, "watch stopped: %v\n", err)
			return 1
		}
	}
	return exitCode
}

// generateOptions 是 generate 子命令的运行参数。
type generateOptions struct {
	url       string
	namespace string
	publish   bool
	// authToken 来自环境变量 TENUTO_AUTH_TOKEN，不走命令行参数（参数会出现在进程列表里）。
	authToken string
	stdout    io.Writer
}

// generatePackage 完成一次「扫描 → 写桥 → 翻译 → 写 JSON → 提交」。
func generatePackage(ctx context.Context, dir string, options generateOptions) (sdkgen.PackageResult, error) {
	plan, err := sdkgen.Scan(dir)
	if err != nil {
		return sdkgen.PackageResult{}, err
	}
	for _, bridge := range plan.Bridges {
		if err := writeIfChanged(bridge.Path, bridge.Content, options.stdout); err != nil {
			return sdkgen.PackageResult{}, err
		}
	}
	if plan.Spec.File == "" {
		return sdkgen.PackageResult{Dir: plan.Dir, Bridges: plan.Bridges, WatchFiles: plan.WatchFiles}, nil
	}
	result, err := sdkgen.Generate(plan)
	if err != nil {
		return sdkgen.PackageResult{}, err
	}
	if err := writeArtifacts(result, options.stdout); err != nil {
		return sdkgen.PackageResult{}, err
	}
	artifacts, err := artifact.LoadDir(result.Dir)
	if err != nil {
		return sdkgen.PackageResult{}, err
	}
	fmt.Fprintf(options.stdout, "output: %s (content version %s)\n", result.Dir, artifacts.ContentVersion)
	if options.url == "" {
		return result, nil
	}
	client := artifact.Client{BaseURL: options.url, Namespace: options.namespace, AuthToken: options.authToken}
	path, verb := artifact.RegisterPath, "registered"
	if options.publish {
		path, verb = artifact.PublishPath, "published"
	}
	body, err := client.Submit(ctx, path, artifacts)
	if err != nil {
		return sdkgen.PackageResult{}, err
	}
	fmt.Fprintf(options.stdout, "%s: %s\n", verb, strings.TrimSpace(string(body)))
	return result, nil
}

// writeArtifacts 写出 workflow.json 与打包文件；内容相同时不重写。
func writeArtifacts(result sdkgen.PackageResult, stdout io.Writer) error {
	data, err := marshalJSON(result.Workflow)
	if err != nil {
		return fmt.Errorf("%s: %w", artifact.WorkflowFile, err)
	}
	if err := writeIfChanged(filepath.Join(result.Dir, artifact.WorkflowFile), data, stdout); err != nil {
		return err
	}
	// 打包文件把 workflow.json embed 进二进制；作者构建后启动即携带定义。
	if err := writeIfChanged(result.Embedded.Path, result.Embedded.Content, stdout); err != nil {
		return err
	}
	return nil
}
