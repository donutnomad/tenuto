package interpreter

import (
	"errors"
	"testing"

	"github.com/donutnomad/tenuto/sdk/apperror"
)

// 普通 error 经 Encode 统一成 Failure，Type 为空。
func TestOrdinaryErrorEncodesToFailureWithoutType(t *testing.T) {
	failure, err := apperror.Encode(errors.New("plain failure"), apperror.JSONEncoder)
	if err != nil {
		t.Fatal(err)
	}
	if failure.Message != "plain failure" || failure.Type != "" {
		t.Fatalf("encoded failure = %+v, want message=plain failure type=\"\"", failure)
	}
}
