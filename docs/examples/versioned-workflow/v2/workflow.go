// Package v2 是 lifecycle 示例的当前版本：GetVersion 分支 + 跨包 remote 契约。
//
// 与 v1 的差异全部落在 `GetVersion(ctx, "charge-policy", 3, 6)` 之后：
//   - 新执行记录 6，走 ChargeV2 并调用 remote 清算（policy=v2）；
//   - 历史里记录 4（v1 写入）时保留旧路径 Charge、不调用 remote（policy=legacy）；
//   - 历史越过这一点却没有记录时 GetVersion 得到 workflow.DefaultVersion（-1），落在支持范围
//     [3, 6] 之外，本次 Workflow Task 以非确定性错误失败。v1 从第一版起就调用 GetVersion，
//     所以这个示例的历史都有记录；要接住没有记录的历史，把 minSupported 写成 workflow.DefaultVersion。
package v2

import (
	"time"

	workflow "github.com/donutnomad/tenuto/sdk/workflow"

	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/activities"
	"github.com/donutnomad/tenuto/docs/examples/versioned-workflow/remote"
)

// Input 是 lifecycle 的入参。
type Input struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

// Output 是 v2 的返回值：带上实际扣款分支与 remote 清算结果。
type Output struct {
	ReservationID string `json:"reservation_id"`
	ChargeID      string `json:"charge_id"`
	Policy        string `json:"policy"`
	Receipt       string `json:"receipt"`
	Delivered     bool   `json:"delivered"`
}

// @Workflow(type="lifecycle-demo",name="Lifecycle Demo",policy=auto_upgrade,version=v2)
func Lifecycle(ctx workflow.Context, in Input) (Output, error) {
	version := workflow.GetVersion(ctx, "charge-policy", 3, 6)
	reserved, err := activities.CallReserve(ctx, activities.ReserveInput{OrderID: in.OrderID}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Output{}, err
	}
	var charged activities.ChargeOutput
	var settled remote.SettleOutput
	policy := "legacy"
	if version >= 6 {
		charged, err = activities.CallChargeV2(ctx, activities.ChargeInput{
			OrderID:       in.OrderID,
			ReservationID: reserved.ReservationID,
		}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if err != nil {
			return Output{}, err
		}
		// remote 契约在独立包：桥名 CallSettle，显式 queue 走 payments 队列。
		settled, err = remote.CallSettle(ctx, remote.SettleInput{OrderID: in.OrderID, Amount: in.Amount},
			workflow.ActivityOptions{StartToCloseTimeout: time.Minute, TaskQueue: "payments"})
		if err != nil {
			return Output{}, err
		}
		policy = "v2"
	} else {
		charged, err = activities.CallCharge(ctx, activities.ChargeInput{
			OrderID:       in.OrderID,
			ReservationID: reserved.ReservationID,
		}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		if err != nil {
			return Output{}, err
		}
	}
	notified, err := activities.CallNotify(ctx, activities.NotifyInput{OrderID: in.OrderID, Note: policy}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	if err != nil {
		return Output{}, err
	}
	return Output{
		ReservationID: reserved.ReservationID,
		ChargeID:      charged.ChargeID,
		Policy:        policy,
		Receipt:       settled.Receipt,
		Delivered:     notified.Delivered,
	}, nil
}
