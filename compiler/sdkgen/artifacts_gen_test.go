package sdkgen

import (
	"bytes"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestArtifactsFileStable 锁住打包文件的确定性与 embed 指令：
// 同一包两次生成的字节相同，且只 embed 运行必需的 workflow.json。
func TestArtifactsFileStable(t *testing.T) {
	first := embeddedArtifactsFile("/tmp/orders", "orders")
	second := embeddedArtifactsFile("/tmp/orders", "orders")
	if first.Path != filepath.Join("/tmp/orders", "zz_orders_artifacts_gen.go") {
		t.Fatalf("生成文件名 = %q", first.Path)
	}
	if !bytes.Equal(first.Content, second.Content) {
		t.Fatal("两次生成的内容不同")
	}
	source := string(first.Content)
	for _, want := range []string{
		"package orders",
		"//go:embed workflow.json",
		"func WorkflowDefinition() registration.Definition",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("生成内容缺少 %q:\n%s", want, source)
		}
	}
	if strings.Contains(source, "go:embed *.json") {
		t.Fatal("只应打包 workflow.json，不应打包全部 JSON")
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "zz_orders_artifacts_gen.go", first.Content, 0); err != nil {
		t.Fatalf("生成内容不是合法 Go 源码: %v", err)
	}
}
