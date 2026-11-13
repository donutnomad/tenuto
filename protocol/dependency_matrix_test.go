package protocol_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const modulePath = "github.com/donutnomad/tenuto/"

// 分包的允许依赖矩阵。
// 键是模块内目录前缀，值是该目录下的非测试代码可以 import 的模块内目录前缀；一个目录按最长的键匹配。
// server/app 与 server/cmd 是组合根，可以依赖全部包，不设规则。
//
// 执行核心是 server/biz/service/engine 一个包（方案 9.1 的中止条件）：它在各用例之上，
// 组合用例与读模型并实现它们消费的端口，所以 service 可以依赖 usecase 与 view，
// usecase 只依赖领域。view 只依赖领域与 protocol；Workflow 执行器（internal/executor）只在 Worker、
// 组合根与 console 的重放里使用，biz 与 infra 不依赖它。
// runtime HTTP 使用 sdk/apperror 的错误编码，是 server/http 一行里登记的例外。
// runtime HTTP 的线上契约在 protocol/runtimeapi：handler（server/http/runtimehttp）与 SDK 的传输层
// （sdk/internal/runtimeclient）都依赖它，传输层不依赖服务端各层，handler 不依赖 SDK 的传输层。
//
// SDK：用户 import 的包在 sdk/ 下（见 userFacingSDKPackages），实现细节在 sdk/internal 与模块的 internal；
// sdk 不依赖 server。sdk/client 不依赖 sdk/worker 与 Worker 的实现，sdk/worker 经 sdk/client 的连接工作。
var (
	innermost     = []string{"protocol", "program", "internal/jsonvalue", "internal/runtimectx", "telemetry"}
	targetLayers  = layerRules()
	usecasePrefix = "server/biz/usecase/"
)

func layerRules() map[string][]string {
	domain := concat(innermost, []string{"server/biz/domain"})
	sdkDefinitions := []string{"sdk/workflow", "sdk/apperror", "sdk/activity", "sdk/registration"}
	return map[string][]string{
		"protocol":                {"protocol", "internal/jsonvalue"},
		"protocol/runtimeapi":     {"protocol"},
		"program":                 {"protocol", "internal/jsonvalue", "internal/runtimectx", "program"},
		"internal/jsonvalue":      {},
		"internal/runtimectx":     {"protocol"},
		"internal/executor":       concat(innermost, sdkDefinitions, []string{"internal/executor"}),
		"internal/worker":         concat(innermost, sdkDefinitions, []string{"internal/executor", "internal/worker"}),
		"sdk":                     concat(innermost, []string{"sdk"}),
		"sdk/workflow":            {"protocol", "sdk/apperror"},
		"sdk/apperror":            {"protocol"},
		"sdk/activity":            concat(innermost, []string{"sdk/workflow", "sdk/activity"}),
		"sdk/registration":        {},
		"sdk/internal":            concat(innermost, []string{"sdk/internal"}),
		"sdk/client":              concat(innermost, sdkDefinitions, []string{"sdk/client", "sdk/internal/runtimeclient"}),
		"sdk/worker":              concat(innermost, sdkDefinitions, []string{"sdk/worker", "sdk/client", "sdk/internal/runtimeclient", "internal/worker", "internal/executor"}),
		"server/biz/domain":       domain,
		"server/biz/usecase":      domain,
		"server/biz/view":         domain,
		"server/biz/service":      concat(innermost, []string{"server/biz"}),
		"server/infra":            concat(domain, []string{"server/infra"}),
		"server/http":             concat(innermost, []string{"server/biz", "server/http", "sdk/apperror"}),
		"server/http/runtimehttp": concat(innermost, []string{"server/biz", "server/http/runtimehttp", "sdk/apperror"}),
		// sdkgen 读取并生成 SDK 代码，compiler 因此依赖 sdk 的定义包。
		"compiler": concat(innermost, sdkDefinitions, []string{"compiler"}),
	}
}

func concat(lists ...[]string) []string {
	var out []string
	for _, list := range lists {
		out = append(out, list...)
	}
	return out
}

// TestTargetDependencyMatrix 对模块内各层严格执行依赖矩阵：每个文件按最长匹配的层取规则。
func TestTargetDependencyMatrix(t *testing.T) {
	for layer := range targetLayers {
		for _, imp := range internalImports(t, layer) {
			if governingLayer(imp.pkg) != layer {
				continue
			}
			allowed := targetLayers[layer]
			if !hasDirPrefix(imp.target, allowed) {
				t.Errorf("%s imports %s: layer %s may only depend on %v", imp.file, imp.target, layer, allowed)
			}
			// usecase 包之间不直接 import：协作经消费方定义的窄接口，由执行核心或入口注入实现。
			if strings.HasPrefix(imp.pkg, usecasePrefix) && strings.HasPrefix(imp.target, usecasePrefix) &&
				usecaseName(imp.pkg) != usecaseName(imp.target) {
				t.Errorf("%s imports %s: usecase packages do not depend on each other", imp.file, imp.target)
			}
		}
	}
}

// governingLayer 返回目录按最长前缀匹配到的层。
func governingLayer(dir string) string {
	best := ""
	for layer := range targetLayers {
		if hasDirPrefix(dir, []string{layer}) && len(layer) > len(best) {
			best = layer
		}
	}
	return best
}

type internalImport struct {
	file   string // 相对仓库根的文件路径
	pkg    string // 文件所在的模块内目录
	target string // 被 import 的模块内目录
}

// internalImports 返回 dir 下非测试 Go 文件对本模块其他包的 import。dir 不存在时返回空。
func internalImports(t *testing.T, dir string) []internalImport {
	t.Helper()
	if _, err := os.Stat(filepath.Join("..", filepath.FromSlash(dir))); os.IsNotExist(err) {
		return nil
	}
	return moduleImports(t, dir, false)
}

func hasDirPrefix(dir string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if dir == prefix || strings.HasPrefix(dir, prefix+"/") {
			return true
		}
	}
	return false
}

func usecaseName(dir string) string {
	name, _, _ := strings.Cut(strings.TrimPrefix(dir, usecasePrefix), "/")
	return name
}
