package sdkgen_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/compiler/sdkgen"
)

// 第 7 项：Activity 的输入与输出在线上是 JSON object，标量、切片、命名的非 struct 类型在生成期报错。
func TestScanRejectsNonObjectActivityIO(t *testing.T) {
	cases := map[string]struct {
		signature string
		want      string
	}{
		"scalar input":       {"func Length(ctx context.Context, in string) (LengthOutput, error)", "输入类型 string"},
		"scalar output":      {"func Length(ctx context.Context, in LengthInput) (int, error)", "输出类型 int"},
		"slice input":        {"func Length(ctx context.Context, in []LengthInput) (LengthOutput, error)", "输入类型 []LengthInput"},
		"named scalar input": {"func Length(ctx context.Context, in Text) (LengthOutput, error)", "输入类型 Text"},
		"pointer output":     {"func Length(ctx context.Context, in LengthInput) (*LengthOutput, error)", "输出类型 *LengthOutput"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir, err := os.MkdirTemp("testdata", "tmp-shape-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			source := `package shape

import "context"

type Text string

type LengthInput struct {
	Value string ` + "`json:\"value\"`" + `
}

type LengthOutput struct {
	Length int ` + "`json:\"length\"`" + `
}

var _ = context.Background

// @Activity(type="text.length",queue="text")
` + tc.signature + ` {
	panic("unused")
}
`
			if err := os.WriteFile(filepath.Join(dir, "activities.go"), []byte(source), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err = sdkgen.Scan(dir)
			if err == nil {
				t.Fatal("Scan accepted a non-object Activity signature")
			}
			for _, want := range []string{tc.want, "struct", "text.length"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not contain %q", err.Error(), want)
				}
			}
		})
	}
}

// struct 与 map[string]any 的输入输出照常接受。
func TestScanAcceptsObjectActivityIO(t *testing.T) {
	dir, err := os.MkdirTemp("testdata", "tmp-shape-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	source := `package shape

import "context"

type LengthInput struct {
	Value string
}

type LengthOutput struct {
	Length int
}

// @Activity(type="text.length",queue="text")
func Length(ctx context.Context, in LengthInput) (LengthOutput, error) {
	return LengthOutput{Length: len(in.Value)}, nil
}

// @Activity(type="text.echo",queue="text")
func Echo(ctx context.Context, in map[string]any) (map[string]any, error) {
	return in, nil
}
`
	if err := os.WriteFile(filepath.Join(dir, "activities.go"), []byte(source), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := sdkgen.Scan(dir); err != nil {
		t.Fatalf("Scan: %v", err)
	}
}
