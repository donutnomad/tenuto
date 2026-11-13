package protocol

import "context"

type ActivityFunc func(ctx context.Context, input map[string]any) (map[string]any, error)
