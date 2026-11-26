package protocol

import (
	"errors"
	"strings"
	"testing"
)

// 长度按字符计（与 MySQL VARCHAR(n) 一致），超长时是 ErrInvalidRequest；errors.Join 合并多项检查。
func TestCheckLengthCountsCharacters(t *testing.T) {
	if err := CheckLength("workflow id", strings.Repeat("界", MaxWorkflowIDLength), MaxWorkflowIDLength); err != nil {
		t.Fatalf("191 characters = %v", err)
	}
	err := errors.Join(
		CheckLength("namespace", "default", MaxNamespaceLength),
		CheckLength("workflow id", strings.Repeat("x", MaxWorkflowIDLength+1), MaxWorkflowIDLength),
	)
	if !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "workflow id has 192 characters, the limit is 191") {
		t.Fatalf("err = %v", err)
	}
	if err := errors.Join(CheckLength("a", "", 1), CheckLength("b", "x", 1)); err != nil {
		t.Fatalf("valid values = %v", err)
	}
}
