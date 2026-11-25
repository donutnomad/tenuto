package splitpkg

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

// @Workflow(id="splitpkg-flow",name="Split Package Flow",policy=auto_upgrade,version=v1)
func SplitFlow(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
	// 第二次 `out, err :=` 里 err 已在作用域中：重写必须保留 Go 语义（err 用 =），
	// 否则重写产物的整包类型检查失败，跨文件的 activity marker 会退化成变量。
	charged, err := CallCharge(ctx, ChargeInput{OrderID: in.OrderID, Amount: in.Amount}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return OrderOutput{}, err
	}
	recorded, err := CallRecord(ctx, RecordInput{OrderID: in.OrderID, ChargeID: charged.ChargeID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		// 补偿：登记失败时撤销扣款，补偿失败单独报错。
		reversed, reverseErr := CallReverse(ctx, ReverseInput{OrderID: in.OrderID, ChargeID: charged.ChargeID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if reverseErr != nil || !reversed.Reversed {
			return OrderOutput{}, workflow.NewApplicationError("record failed, reverse charge failed", "CompensationFailed")
		}
		return OrderOutput{}, workflow.NewApplicationError("record failed, charge reversed", "RecordFailed")
	}
	return OrderOutput{ChargeID: charged.ChargeID, Receipt: recorded.Receipt}, nil
}
