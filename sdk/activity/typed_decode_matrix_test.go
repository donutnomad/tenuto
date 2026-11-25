package activity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

// 本文件实测 UNKNOWN-03 的“本地 typed Activity 适配层”实际行为：
// RegisterActivity 把 map[string]any 输入用 encoding/json 解码成业务 DTO，
// 再用 encoding/json 把业务 DTO 编码回 map[string]any 输出。
//
// 这里验证的是 SDK JSON adapter，不是契约层，也不是 WLang typed record 解码。
// 三类语义分别由本文件（adapter）、interpreter/typed_record_decode_matrix_test.go
// （WLang）和 server/biz/service/engine 的契约测试（静态指纹）覆盖。
//
// 结论前置：Go encoding/json 对缺字段/改名/多余字段不报错并写零值；
// 只有 JSON 类型与目标 Go 类型不兼容时才返回 UnmarshalTypeError。

type adapterInput struct {
	Amount int64  `json:"amount"`
	Memo   string `json:"memo"`
}

type adapterInputOptional struct {
	Amount int64   `json:"amount"`
	Memo   *string `json:"memo,omitempty"`
	Note   string  `json:"note"`
}

func registerAdapter[I, O any](t *testing.T, fn func(context.Context, I) (O, error)) (*Registry, protocol.ActivityFunc) {
	t.Helper()
	reg := NewRegistry()
	if err := RegisterActivity(reg, "test.activity", fn); err != nil {
		t.Fatalf("RegisterActivity: %v", err)
	}
	adapter, err := reg.Get("test.activity")
	if err != nil {
		t.Fatal(err)
	}
	return reg, adapter
}

func TestTypedActivityInputAdapterMatrix(t *testing.T) {
	var got adapterInput
	_, fn := registerAdapter(t, func(_ context.Context, in adapterInput) (map[string]any, error) {
		got = in
		return map[string]any{"ok": true}, nil
	})

	cases := []struct {
		name string
		in   map[string]any
		want adapterInput
		// wantErrSubstring 非空表示实测 JSON 解码报错。
		wantErrSubstring string
	}{
		{
			name: "complete-payload",
			in:   map[string]any{"amount": 42, "memo": "hi"},
			want: adapterInput{Amount: 42, Memo: "hi"},
		},
		{
			name: "missing-required-field-decodes-zero",
			in:   map[string]any{"memo": "hi"},
			want: adapterInput{Memo: "hi"},
		},
		{
			name: "explicit-null-decodes-zero",
			in:   map[string]any{"amount": nil, "memo": nil},
			want: adapterInput{},
		},
		{
			name: "json-field-rename-decodes-zero",
			// 旧 payload 用 old_memo，新 DTO 的 JSON 名是 memo：未知键被忽略。
			in:   map[string]any{"amount": 7, "old_memo": "hi"},
			want: adapterInput{Amount: 7},
		},
		{
			name:             "int-to-string-is-type-error",
			in:               map[string]any{"amount": 42, "memo": 9},
			wantErrSubstring: "cannot unmarshal number into Go struct field",
		},
		{
			name: "extra-field-payload-is-ignored",
			in:   map[string]any{"amount": 1, "memo": "x", "unknown": true},
			want: adapterInput{Amount: 1, Memo: "x"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got = adapterInput{}
			_, err := fn(context.Background(), tc.in)
			if tc.wantErrSubstring != "" {
				if err == nil {
					t.Fatalf("want decode error containing %q, got nil (decoded %+v)", tc.wantErrSubstring, got)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstring) {
					t.Fatalf("decode error = %v, want substring %q", err, tc.wantErrSubstring)
				}
				if got != (adapterInput{}) {
					t.Fatalf("activity body must not run on decode error, got %+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode error = %v, want success with zero values", err)
			}
			if got != tc.want {
				t.Fatalf("decoded = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// 新增可选字段（*T / omitempty）与新增必填字段在 adapter 层都只是“缺字段 → 零值”，
// 不产生错误；必填性不是 encoding/json 的语义。
func TestTypedActivityInputAddedFields(t *testing.T) {
	var got adapterInputOptional
	_, fn := registerAdapter(t, func(_ context.Context, in adapterInputOptional) (map[string]any, error) {
		got = in
		return nil, nil
	})

	if _, err := fn(context.Background(), map[string]any{"amount": 3}); err != nil {
		t.Fatalf("missing optional and required additions must decode: %v", err)
	}
	if got.Amount != 3 || got.Memo != nil || got.Note != "" {
		t.Fatalf("decoded = %+v, want amount=3 memo=nil note=\"\"", got)
	}

	memo := "set"
	if _, err := fn(context.Background(), map[string]any{"amount": 4, "memo": memo, "note": "n"}); err != nil {
		t.Fatalf("present additions must decode: %v", err)
	}
	if got.Memo == nil || *got.Memo != memo || got.Note != "n" {
		t.Fatalf("decoded additions = %+v", got)
	}
}

type adapterOutput struct {
	Receipt string  `json:"receipt"`
	Count   int64   `json:"count"`
	Memo    *string `json:"memo,omitempty"`
	Note    string  `json:"note"`
}

// 输出侧：struct → map[string]any 的实际键与零值形态。
func TestTypedActivityOutputAdapterMatrix(t *testing.T) {
	_, fn := registerAdapter(t, func(_ context.Context, _ adapterInput) (adapterOutput, error) {
		return adapterOutput{Receipt: "r-1", Count: 2}, nil
	})
	out, err := fn(context.Background(), map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if out["receipt"] != "r-1" {
		t.Fatalf("receipt = %#v", out["receipt"])
	}
	// 数字经 jsonvalue.DecodeMap 归一：整数值落成 int64，超出 int64 才降级。
	if got := out["count"]; got != int64(2) {
		t.Fatalf("count = %#v (%T), want int64(2)", got, got)
	}
	// omitempty 的 nil 指针不出现；没有 omitempty 的零值字符串保留。
	if _, ok := out["memo"]; ok {
		t.Fatalf("omitempty nil pointer must be omitted, got %#v", out["memo"])
	}
	if note, ok := out["note"]; !ok || note != "" {
		t.Fatalf("non-omitempty zero string = %#v (present=%v), want \"\"", note, ok)
	}
}

func TestTypedActivityBodyErrorPropagates(t *testing.T) {
	sentinel := errors.New("business failure")
	_, fn := registerAdapter(t, func(_ context.Context, _ adapterInput) (adapterOutput, error) {
		return adapterOutput{}, sentinel
	})
	if _, err := fn(context.Background(), map[string]any{}); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
}
