// Package splitpkg 是同包跨文件的回归 fixture：@Workflow 编排、@Activity 实现与 DTO
// 分别放在三个源文件里。生成器重写出来的 marker 必须仍然是跨文件的包级函数符号，
// 不能因为 marker 声明在别的文件里就退化成变量引用。
package splitpkg

// OrderInput 是 fixture workflow 的入参。
type OrderInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// OrderOutput 是 fixture workflow 的返回值。
type OrderOutput struct {
	ChargeID string `json:"charge_id"`
	Receipt  string `json:"receipt"`
}

// ChargeInput 是扣款入参。
type ChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// ChargeOutput 是扣款结果。
type ChargeOutput struct {
	ChargeID string `json:"charge_id"`
}

// RecordInput 是登记入参。
type RecordInput struct {
	OrderID  string `json:"order_id"`
	ChargeID string `json:"charge_id"`
}

// RecordOutput 是登记结果。
type RecordOutput struct {
	Receipt string `json:"receipt"`
}

// ReverseInput 是撤销扣款入参（补偿步骤）。
type ReverseInput struct {
	OrderID  string `json:"order_id"`
	ChargeID string `json:"charge_id"`
}

// ReverseOutput 是撤销结果。
type ReverseOutput struct {
	Reversed bool `json:"reversed"`
}
