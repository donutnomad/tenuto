package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	taskqgormstore "github.com/donutnomad/taskq/store/gormstore"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"gorm.io/gorm"
)

type healthState struct {
	ready  atomic.Bool
	checks []func(context.Context) error
}

// partitionAdmin 是 /admin/partitions 端点消费的窄接口——只需 taskQStore 的分区读写两法。
// 写用 grow-only 的 GrowPartitions 而非无条件的 SetPartitions：后者允许缩容、会孤立高位分区
// 里的在途任务，只应由 matching 的 ResizePartitions 编排器在 drain 后调用。
// memory 后端无此能力，传 nil，端点回 501。
type partitionAdmin interface {
	GrowPartitions(ctx context.Context, queueName string, read, write int) error
	ResolvePartitions(ctx context.Context, queueName string) (int, int, error)
}

// newHTTPHandler 组合 HTTP 监听的路由。auth 非 nil 时 /runtime/* 与 /admin/* 先校验 Bearer token
// （未鉴权的请求不论是否就绪都得到 401）；/health/* 始终不鉴权。
func newHTTPHandler(runtime http.Handler, health *healthState, partitions partitionAdmin, auth *bearerAuth) http.Handler {
	mux := health.mux()
	mux.Handle("/runtime/", auth.require(health.requireReady(runtime)))
	mux.Handle("/admin/partitions", auth.require(health.requireReady(partitionHandler(partitions))))
	return mux
}

// mux 返回只含 /health/live 与 /health/ready 的路由。
func (h *healthState) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", h.live)
	mux.HandleFunc("/health/ready", h.readiness)
	return mux
}

// startHealthListener 在 addr 上以明文 HTTP 提供 /health/*（WORKFLOW_HEALTH_ADDR）。主监听开启 mTLS 后，
// 编排平台的 HTTP 探针出示不了客户端证书，改探这个端口。
func (a *application) startHealthListener(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen health on %s: %w", addr, err)
	}
	a.healthServer = &http.Server{
		Handler:           a.health.mux(),
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadHeaderTimeout,
		WriteTimeout:      10 * time.Second,
	}
	go func() {
		if err := a.healthServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.telemetry.Logger.Error("health listener stopped", "err", err)
		}
	}()
	return nil
}

func (h *healthState) live(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeHealth(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	writeHealth(w, http.StatusOK, "ok")
}

func (h *healthState) readiness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeHealth(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	if !h.ready.Load() {
		writeHealth(w, http.StatusServiceUnavailable, "starting")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	for _, check := range h.checks {
		if err := check(ctx); err != nil {
			writeHealth(w, http.StatusServiceUnavailable, "unavailable")
			return
		}
	}
	writeHealth(w, http.StatusOK, "ok")
}

func (h *healthState) requireReady(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.ready.Load() {
			writeHealth(w, http.StatusServiceUnavailable, "starting")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeHealth(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": value})
}

// MaxQueuePartitions 是一个物理 queue 的分区数上限，/admin/partitions 与启动配置共用。
// 分区数只增不减，每个分区在持有它的副本上常驻租约续约与 GC；上限挡住一次误输入把队列永久扩成
// 无法承载的分区数。
const MaxQueuePartitions = 1024

// partitionHandler 提供 /admin/partitions 运行时分区数入口。入参 queue 为**物理 queue 名**。
// 与 /runtime 相同：配置 WORKFLOW_HTTP_AUTH_TOKEN 时校验 Bearer token，否则靠网络隔离。
func partitionHandler(admin partitionAdmin) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if admin == nil {
			writeError(w, http.StatusNotImplemented, "partition administration is unavailable for this backend")
			return
		}
		switch r.Method {
		case http.MethodGet:
			handleGetPartitions(w, r, admin)
		case http.MethodPost:
			handleSetPartitions(w, r, admin)
		default:
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func handleGetPartitions(w http.ResponseWriter, r *http.Request, admin partitionAdmin) {
	queue := strings.TrimSpace(r.URL.Query().Get("queue"))
	if queue == "" {
		writeError(w, http.StatusBadRequest, "queue is required")
		return
	}
	read, write, err := admin.ResolvePartitions(r.Context(), queue)
	if err != nil {
		writePartitionResolveError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queue": queue, "read": read, "write": write})
}

func handleSetPartitions(w http.ResponseWriter, r *http.Request, admin partitionAdmin) {
	if !runtimehttp.IsJSONRequest(r) {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	var body struct {
		Queue      string `json:"queue"`
		Partitions int    `json:"partitions"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	queue := strings.TrimSpace(body.Queue)
	if queue == "" {
		writeError(w, http.StatusBadRequest, "queue is required")
		return
	}
	if body.Partitions < 1 || body.Partitions > MaxQueuePartitions {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("partitions must be between 1 and %d", MaxQueuePartitions))
		return
	}
	// 此读**仅供响应展示**（previousRead/previousWrite），不参与 grow-only 判定——判定已由
	// 下面的 GrowPartitions 用单条条件 UPDATE 原子完成，故这里不再是 TOCTOU 决策点。
	curRead, curWrite, err := admin.ResolvePartitions(r.Context(), queue)
	if err != nil {
		writePartitionResolveError(w, err)
		return
	}
	if err := admin.GrowPartitions(r.Context(), queue, body.Partitions, body.Partitions); err != nil {
		if errors.Is(err, taskqgormstore.ErrPartitionShrinkRejected) {
			// err 自带当前值与目标值，直接透出对调用方更有意义。
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to set partitions")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"queue":         queue,
		"read":          body.Partitions,
		"write":         body.Partitions,
		"previousRead":  curRead,
		"previousWrite": curWrite,
	})
}

// isDefaultPartitionMissing 判定 ResolvePartitions 的错误是否为「queue 无 override 行且
// `_default_` 也未 bootstrap」。`_default_` 由 matching 负责 bootstrap（内嵌模式即本进程的
// matching.NewServer，外置模式由独立 matching 进程），
// store 此时返回一个错误串含 "default partition config" 的 error；对 gorm.ErrRecordNotFound
// 也做 errors.Is 双保险。唯一消费者是 writePartitionResolveError：把这种「读不出有效分区数」
// 映射成 503 而非 500。（启动初始化不再用它——GrowPartitions 自己处理「override 行不存在」。）
func isDefaultPartitionMissing(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound) || strings.Contains(err.Error(), "default partition config")
}

// writePartitionResolveError 把 ResolvePartitions 的错误映射到 HTTP 状态：`_default_` 行
// 缺失→503，其余→500。
func writePartitionResolveError(w http.ResponseWriter, err error) {
	if isDefaultPartitionMissing(err) {
		writeError(w, http.StatusServiceUnavailable, "default partition config is missing")
		return
	}
	writeError(w, http.StatusInternalServerError, "failed to resolve partitions")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
