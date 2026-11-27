// Package calc 是 initclosure fixture 的本地 Activity 包。
package calc

import "context"

// Num 是 Activity 的入参与结果。
type Num struct {
	N int `json:"n"`
}

// @Activity(type="calc.double",queue="calc")
func Double(ctx context.Context, in Num) (Num, error) {
	return Num{N: in.N * 2}, nil
}
