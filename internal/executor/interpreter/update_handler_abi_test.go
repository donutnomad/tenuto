package interpreter

import (
	"testing"

	"github.com/donutnomad/tenuto/internal/executor"
	wlruntime "github.com/donutnomad/wlang/runtime"
)

func TestClassifyUpdateHandlerSignature(t *testing.T) {
	tests := []struct {
		name    string
		sig     wlruntime.FunctionSignature
		kind    executor.UpdateHandlerABIKind
		wantErr string
	}{
		{name: "error only", sig: wlruntime.FunctionSignature{Params: []string{"workflow.Context", "string"}, Returns: []string{"error"}}, kind: executor.UpdateHandlerABITemporalError},
		{name: "result and error", sig: wlruntime.FunctionSignature{Params: []string{"workflow.Context", "int64"}, Returns: []string{"string", "error"}}, kind: executor.UpdateHandlerABITemporalResultError},
		{name: "map result and error", sig: wlruntime.FunctionSignature{Params: []string{"workflow.Context", "map<string,any>"}, Returns: []string{"map<string,any>", "error"}}, kind: executor.UpdateHandlerABITemporalResultError},
		{name: "map without error", sig: wlruntime.FunctionSignature{Params: []string{"workflow.Context", "map<string,any>"}, Returns: []string{"map<string,any>"}}, wantErr: "must return"},
		{name: "context", sig: wlruntime.FunctionSignature{Params: []string{"string"}, Returns: []string{"error"}}, wantErr: "first parameter"},
		{name: "returns", sig: wlruntime.FunctionSignature{Params: []string{"workflow.Context"}, Returns: []string{"string"}}, wantErr: "must return"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			abi, err := executor.ClassifyUpdateHandlerSignature(tt.sig, nil)
			if tt.wantErr != "" {
				if err == nil || !containsError(err, tt.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if abi.Kind != tt.kind {
				t.Fatalf("kind = %q, want %q", abi.Kind, tt.kind)
			}
		})
	}
}

func containsError(err error, want string) bool {
	return err != nil && len(want) > 0 && stringContains(err.Error(), want)
}

func stringContains(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
