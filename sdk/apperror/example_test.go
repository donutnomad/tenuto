package apperror_test

import (
	"errors"
	"fmt"

	"github.com/donutnomad/tenuto/sdk/apperror"
)

// 应用错误带 Type、NonRetryable 与 details；errors.As 在调用方取出它。
func ExampleNewNonRetryable() {
	err := fmt.Errorf("charge: %w", apperror.NewNonRetryable("card declined", "PaymentDeclined", nil, map[string]string{"code": "expired"}))

	var appErr *apperror.Error
	if errors.As(err, &appErr) {
		var detail map[string]string
		_ = appErr.Details(&detail)
		fmt.Println(appErr.Type(), appErr.NonRetryable(), detail["code"])
	}
	fmt.Println(apperror.IsCanceled(apperror.NewCanceled()))
	// Output:
	// PaymentDeclined true expired
	// true
}
