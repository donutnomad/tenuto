// Package cancellation 演示 Workflow 取消之后的清理，与文档 develop/cancellation-timeouts 同步。
//
// Run 收到取消请求时根 Context 被取消，Workflow 代码继续运行：正在等待的 Activity 被请求取消，
// Future 返回取消错误。清理 Activity 发在 NewDisconnectedContext 返回的 Context 上，它不随根 Context
// 取消；Workflow 最后返回取消错误，Run 以 Cancelled 结束。
package cancellation

import (
	"context"
	"time"

	"github.com/donutnomad/tenuto/sdk/activity"
	"github.com/donutnomad/tenuto/sdk/workflow"
)

// BookTrip 先占座，再等待付款；等待期间被取消时释放座位。
func BookTrip(ctx workflow.Context, input map[string]any) error {
	var hold map[string]any
	if err := workflow.ExecuteActivity(ctx, HoldSeat, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(ctx, &hold); err != nil {
		return err
	}
	var payment map[string]any
	err := workflow.ExecuteActivity(ctx, AwaitPayment, input, workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
		// 取消只经心跳送达：可取消的 Activity 必须心跳并设置 HeartbeatTimeout。
		HeartbeatTimeout: 10 * time.Second,
	}).Get(ctx, &payment)
	if workflow.IsCanceledError(err) {
		// ctx 已取消，在它上面发起的 Activity 会立即以取消错误返回；清理用不随它取消的 Context。
		cleanupCtx, _ := workflow.NewDisconnectedContext(ctx)
		var released map[string]any
		if releaseErr := workflow.ExecuteActivity(cleanupCtx, ReleaseSeat, input, workflow.ActivityOptions{StartToCloseTimeout: time.Minute}).Get(cleanupCtx, &released); releaseErr != nil {
			return releaseErr
		}
		// 返回取消错误：Run 以 Cancelled 结束。返回 nil 则以 Completed 结束。
		return err
	}
	return err
}

// HoldSeat 占座。
func HoldSeat(ctx context.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"held": true}, nil
}

// AwaitPayment 等待付款回调，每秒心跳一次；Workflow 请求取消时下一次心跳之后 ctx 被取消，
// 返回 ctx.Err() 即向 Server 确认取消。
func AwaitPayment(ctx context.Context, input map[string]any) (map[string]any, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		activity.RecordHeartbeat(ctx, nil)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// ReleaseSeat 释放座位。
func ReleaseSeat(ctx context.Context, input map[string]any) (map[string]any, error) {
	return map[string]any{"released": true}, nil
}
