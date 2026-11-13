package protocol

import "context"

type workerRoutingContextKey struct{}

type WorkerRoutingInfo struct {
	Namespace string
	Identity  string
	BuildID   string
}

func WithWorkerRoutingInfo(ctx context.Context, info WorkerRoutingInfo) context.Context {
	return context.WithValue(ctx, workerRoutingContextKey{}, info)
}

func WorkerRoutingInfoFrom(ctx context.Context) (WorkerRoutingInfo, bool) {
	info, ok := ctx.Value(workerRoutingContextKey{}).(WorkerRoutingInfo)
	return info, ok
}
