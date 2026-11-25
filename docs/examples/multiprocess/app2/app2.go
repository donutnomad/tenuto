// Package app2 是 multiprocess 示例里 app-2 进程的 activity 集：库存（inventory 队列）与账务（ledger 队列）。
//
// 它只实现契约、不编排任何 Workflow；契约形状来自 contracts 包（remote 声明），实现随 app-2
// 的二进制发布。app-2 的 Activity worker 按 BuildID（app2-v1 / app2-v2）在线后，app-1 发起的
// Workflow 才能在这些队列上调度成功。
package app2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	wfsdk "github.com/donutnomad/tenuto/sdk/activity"

	"github.com/donutnomad/tenuto/docs/examples/multiprocess/contracts"
)

// stockLocation 是 app-2 的库存能力，v2 构建从 error 修正为 2 号仓。
const stockLocation = "warehouse-2"

// RegisterActivities 把 app-2 的全部实现登记到 registry；契约与 contracts 包里
// @RemoteActivity 的声明一致，Worker 登记时随 registry 上报。
func RegisterActivities(registry *wfsdk.Registry) error {
	return errors.Join(
		wfsdk.RegisterActivity(registry, "inventory.check", CheckStock),
		wfsdk.RegisterActivity(registry, "inventory.hold", HoldStock),
		wfsdk.RegisterActivity(registry, "inventory.release", ReleaseHold),
		wfsdk.RegisterActivity(registry, "ledger.settle", Settle),
		wfsdk.RegisterActivity(registry, "risk.review", RiskReview),
		wfsdk.RegisterActivity(registry, "ledger.charge", Charge),
	)
}

// CheckStock 检查库存并返回仓位。
func CheckStock(_ context.Context, in contracts.CheckStockInput) (contracts.CheckStockOutput, error) {
	return contracts.CheckStockOutput{StockID: "stock-" + in.OrderID, Location: stockLocation}, nil
}

// HoldStock 占用库存。
func HoldStock(_ context.Context, in contracts.HoldStockInput) (contracts.HoldStockOutput, error) {
	return contracts.HoldStockOutput{HoldID: "hold-" + in.OrderID + "-" + in.StockID}, nil
}

// ReleaseHold 释放库存占用（库存的补偿路径）。
func ReleaseHold(_ context.Context, in contracts.ReleaseHoldInput) (contracts.ReleaseHoldOutput, error) {
	return contracts.ReleaseHoldOutput{Released: true}, nil
}

// Settle 结算扣款。
func Settle(ctx context.Context, in contracts.SettleInput) (contracts.SettleOutput, error) {
	// 示例专用 gate：driver 先创建 <order>.hold，activity 上报 entered 后等待 release。
	if dir := os.Getenv("MP_READY_DIR"); dir != "" {
		base := filepath.Join(dir, filepath.Base(in.OrderID))
		if _, err := os.Stat(base + ".hold"); err == nil {
			if err := os.WriteFile(base+".entered", []byte("ledger.settle\n"), 0o644); err != nil {
				return contracts.SettleOutput{}, err
			}
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				if _, err := os.Stat(base + ".release"); err == nil {
					break
				} else if !os.IsNotExist(err) {
					return contracts.SettleOutput{}, err
				}
				select {
				case <-ctx.Done():
					return contracts.SettleOutput{}, ctx.Err()
				case <-ticker.C:
				}
			}
		} else if !os.IsNotExist(err) {
			return contracts.SettleOutput{}, err
		}
	}
	return contracts.SettleOutput{Receipt: fmt.Sprintf("receipt-%s-%d", in.OrderID, in.Amount), Settled: true}, nil
}

// RiskReview 是 v2 新增的风控审查。
func RiskReview(_ context.Context, in contracts.RiskReviewInput) (contracts.RiskReviewOutput, error) {
	return contracts.RiskReviewOutput{Level: "low"}, nil
}

// Charge 是 v3 新增的金库扣款。
func Charge(_ context.Context, in contracts.ChargeInput) (contracts.ChargeOutput, error) {
	return contracts.ChargeOutput{ChargeID: fmt.Sprintf("charge-%s-%d", in.OrderID, in.Amount), Remote: true}, nil
}
