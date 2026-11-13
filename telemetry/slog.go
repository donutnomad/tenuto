package telemetry

import (
	"context"
	"log/slog"
)

type fanoutHandler struct {
	handlers []slog.Handler
}

func newFanoutHandler(handlers ...slog.Handler) slog.Handler {
	filtered := handlers[:0]
	for _, handler := range handlers {
		if handler != nil {
			filtered = append(filtered, handler)
		}
	}
	return &fanoutHandler{handlers: filtered}
}

func (h *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *fanoutHandler) Handle(ctx context.Context, record slog.Record) error {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, record.Level) {
			if err := handler.Handle(ctx, record.Clone()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		handlers = append(handlers, handler.WithAttrs(attrs))
	}
	return &fanoutHandler{handlers: handlers}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h.handlers))
	for _, handler := range h.handlers {
		handlers = append(handlers, handler.WithGroup(name))
	}
	return &fanoutHandler{handlers: handlers}
}

type minimumLevelHandler struct {
	level slog.Level
	next  slog.Handler
}

func (h minimumLevelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level && h.next.Enabled(ctx, level)
}

func (h minimumLevelHandler) Handle(ctx context.Context, record slog.Record) error {
	return h.next.Handle(ctx, record)
}

func (h minimumLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return minimumLevelHandler{level: h.level, next: h.next.WithAttrs(attrs)}
}

func (h minimumLevelHandler) WithGroup(name string) slog.Handler {
	return minimumLevelHandler{level: h.level, next: h.next.WithGroup(name)}
}
