package workflow

func temporalLikeSDKCompileFixture(ctx Context) {
	var reserve map[string]any
	future := ExecuteActivity(ctx, "activities.ReserveInventory", map[string]any{"order_id": "o-1"})
	_ = future.Get(ctx, &reserve)
	// 非结构化 Go（照 Temporal workflow.Go = NewCoroutine）：起了即返回、不 join、无返回值。
	Go(ctx, func(ctx Context) {})
	// join 由 WaitGroup 显式做（wg.Go = Add(1) + Go(defer Done)）。
	wg := NewWaitGroup(ctx)
	wg.Go(ctx, func(ctx Context) {})
	wg.Wait(ctx)
}
