package v3

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"
)

type ReturnInput struct {
	OrderID string `json:"order_id"`
	Qty     int    `json:"qty"`
}

type ReturnOutput struct {
	Ticket    string `json:"ticket"`
	Condition string `json:"condition"`
	LabelFee  int64  `json:"label_fee"`
	QuotedBy  string `json:"quoted_by"`
	Restocked bool   `json:"restocked"`
	Notified  bool   `json:"notified"`
}

// OpenResult 是 Update "open" 的返回值：调用方同步拿到退货单号。
type OpenResult struct {
	Ticket string `json:"ticket"`
}

// ReturnFlow 处理一次退货：登记 → 等仓库签收 → 算退货运费 → 重新入库。
//
// policy=pinned：Run 固定使用启动时的 Program。之后发布的新 Program 只影响新启动的 Run。
//
// @Workflow(id="return-flow",name="Return Flow",queue="fulfillment",policy=pinned,version=v3)
func ReturnFlow(ctx workflow.Context, in ReturnInput) (ReturnOutput, error) {
	// RandomUUID 把首次执行生成的值写进 History，重放读回同一个值。
	ticket := workflow.RandomUUID(ctx)
	stage := "inspecting"

	// Update Handler 在主流程第一次阻塞之前注册，才能接到随启动一起到达的 Update。
	if err := workflow.SetUpdateHandler(ctx, "open", func(ctx workflow.Context) (OpenResult, error) {
		return OpenResult{Ticket: ticket}, nil
	}); err != nil {
		return ReturnOutput{}, err
	}
	if err := workflow.SetQueryHandler(ctx, "status", func(ctx workflow.Context, _ map[string]any) map[string]any {
		return map[string]any{"stage": stage, "ticket": ticket}
	}); err != nil {
		return ReturnOutput{}, err
	}

	// v3 把通知挪到登记之前，并删掉了 GetVersion。
	// 之前启动的 Run，History 里第一条 Activity 是 fulfillment.inspect；
	// 用这份代码重放时第一条命令是 fulfillment.notify，两者对不上。
	sent, err := CallNotify(ctx, NotifyInput{OrderID: in.OrderID, Condition: "pending"}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ReturnOutput{}, err
	}
	inspected, err := CallInspect(ctx, InspectInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ReturnOutput{}, err
	}

	stage = "awaiting-parcel"
	// 等仓库签收包裹。Signal 写进 History；Run 在这里可以停留任意长的时间。
	var received map[string]any
	if err := workflow.WaitSignal(ctx, "received").Get(ctx, &received); err != nil {
		return ReturnOutput{}, err
	}

	label, err := CallQuote(ctx, QuoteInput{OrderID: in.OrderID, Qty: in.Qty}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ReturnOutput{}, err
	}
	restocked, err := CallRestock(ctx, RestockInput{OrderID: in.OrderID, Condition: inspected.Condition}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return ReturnOutput{}, err
	}
	return ReturnOutput{
		Ticket:    ticket,
		Condition: inspected.Condition,
		LabelFee:  label.Fee,
		QuotedBy:  label.ServedBy,
		Restocked: restocked.Restocked,
		Notified:  sent.Sent,
	}, nil
}
