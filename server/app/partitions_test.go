package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	taskqgormstore "github.com/donutnomad/taskq/store/gormstore"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	wftaskq "github.com/donutnomad/tenuto/server/infra/taskqueue/taskq"
	"github.com/donutnomad/tenuto/telemetry"
)

// newTaskQStore 返回独立 MySQL 库上的 TaskQ 存储，表按 TaskQ 的 schema.sql 建好；
// 没有开启 MySQL 后端测试时跳过 t。TaskQ 只有 GORM 存储。
func newTaskQStore(t *testing.T) *taskqgormstore.Store {
	t.Helper()
	store, err := taskqgormstore.New(mysqltest.NewTaskQDB(t), taskqgormstore.Config{})
	if err != nil {
		t.Fatalf("open TaskQ store: %v", err)
	}
	return store
}

func readyHandler(admin partitionAdmin) http.Handler {
	health := &healthState{}
	health.ready.Store(true)
	return newHTTPHandler(http.NotFoundHandler(), health, admin, nil)
}

func doJSON(t *testing.T, handler http.Handler, method, target string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	request := httptest.NewRequest(method, target, reader)
	request.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, request)
	var decoded map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &decoded)
	}
	return rec, decoded
}

// --- endpoint tests ---

// recordingPartitionAdmin 记录 GrowPartitions 调用，用于不依赖 MySQL 的端点校验。
type recordingPartitionAdmin struct{ grows []int }

func (a *recordingPartitionAdmin) GrowPartitions(_ context.Context, _ string, read, _ int) error {
	a.grows = append(a.grows, read)
	return nil
}

func (a *recordingPartitionAdmin) ResolvePartitions(context.Context, string) (int, int, error) {
	return 4, 4, nil
}

// 超过 MaxQueuePartitions 的分区数与非 JSON 的 POST 都在写库之前被拒绝；上限本身照常受理。
func TestPartitionsEndpointRejectsOversizedAndNonJSON(t *testing.T) {
	admin := &recordingPartitionAdmin{}
	handler := readyHandler(admin)

	rec, _ := doJSON(t, handler, http.MethodPost, "/admin/partitions", map[string]any{"queue": "q", "partitions": MaxQueuePartitions + 1})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized status = %d, want 400", rec.Code)
	}
	request := httptest.NewRequest(http.MethodPost, "/admin/partitions", bytes.NewReader([]byte(`{"queue":"q","partitions":8}`)))
	request.Header.Set("Content-Type", "text/plain")
	plain := httptest.NewRecorder()
	handler.ServeHTTP(plain, request)
	if plain.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("text/plain status = %d, want 415", plain.Code)
	}
	if len(admin.grows) != 0 {
		t.Fatalf("rejected requests grew partitions: %v", admin.grows)
	}
	rec, _ = doJSON(t, handler, http.MethodPost, "/admin/partitions", map[string]any{"queue": "q", "partitions": MaxQueuePartitions})
	if rec.Code != http.StatusOK || len(admin.grows) != 1 || admin.grows[0] != MaxQueuePartitions {
		t.Fatalf("max status = %d grows = %v", rec.Code, admin.grows)
	}
}

func TestPartitionsEndpointNilBackendReturns501(t *testing.T) {
	handler := readyHandler(nil)
	rec, _ := doJSON(t, handler, http.MethodGet, "/admin/partitions?queue=q", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

func TestPartitionsEndpointGrowSucceeds(t *testing.T) {
	store := newTaskQStore(t)
	ctx := context.Background()
	if err := store.SetPartitions(ctx, "payments-q", 4, 4); err != nil {
		t.Fatal(err)
	}
	handler := readyHandler(store)

	rec, body := doJSON(t, handler, http.MethodPost, "/admin/partitions", map[string]any{"queue": "payments-q", "partitions": 8})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%v", rec.Code, body)
	}
	if body["read"] != float64(8) || body["previousRead"] != float64(4) {
		t.Fatalf("body = %v, want read=8 previousRead=4", body)
	}
	read, write, err := store.ResolvePartitions(ctx, "payments-q")
	if err != nil || read != 8 || write != 8 {
		t.Fatalf("resolved = %d/%d err=%v, want 8/8", read, write, err)
	}
}

func TestPartitionsEndpointRejectsShrink409(t *testing.T) {
	store := newTaskQStore(t)
	ctx := context.Background()
	if err := store.SetPartitions(ctx, "payments-q", 8, 8); err != nil {
		t.Fatal(err)
	}
	handler := readyHandler(store)

	rec, _ := doJSON(t, handler, http.MethodPost, "/admin/partitions", map[string]any{"queue": "payments-q", "partitions": 4})
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
	read, write, _ := store.ResolvePartitions(ctx, "payments-q")
	if read != 8 || write != 8 {
		t.Fatalf("resolved = %d/%d, want unchanged 8/8", read, write)
	}
}

func TestPartitionsEndpointEqualIsIdempotent(t *testing.T) {
	store := newTaskQStore(t)
	ctx := context.Background()
	if err := store.SetPartitions(ctx, "payments-q", 8, 8); err != nil {
		t.Fatal(err)
	}
	handler := readyHandler(store)

	rec, _ := doJSON(t, handler, http.MethodPost, "/admin/partitions", map[string]any{"queue": "payments-q", "partitions": 8})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for equal request", rec.Code)
	}
}

func TestPartitionsEndpointValidation(t *testing.T) {
	store := newTaskQStore(t)
	if err := store.SetPartitions(context.Background(), "payments-q", 4, 4); err != nil {
		t.Fatal(err)
	}
	handler := readyHandler(store)

	tests := []struct {
		name   string
		method string
		target string
		body   any
		want   int
	}{
		{"empty queue post", http.MethodPost, "/admin/partitions", map[string]any{"queue": "", "partitions": 4}, http.StatusBadRequest},
		{"partitions below one", http.MethodPost, "/admin/partitions", map[string]any{"queue": "payments-q", "partitions": 0}, http.StatusBadRequest},
		{"empty queue get", http.MethodGet, "/admin/partitions", nil, http.StatusBadRequest},
		{"method not allowed", http.MethodDelete, "/admin/partitions", nil, http.StatusMethodNotAllowed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, _ := doJSON(t, handler, tt.method, tt.target, tt.body)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestPartitionsEndpointGetReadsCurrent(t *testing.T) {
	store := newTaskQStore(t)
	if err := store.SetPartitions(context.Background(), "payments-q", 5, 3); err != nil {
		t.Fatal(err)
	}
	handler := readyHandler(store)

	rec, body := doJSON(t, handler, http.MethodGet, "/admin/partitions?queue=payments-q", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body["read"] != float64(5) || body["write"] != float64(3) {
		t.Fatalf("body = %v, want read=5 write=3", body)
	}
}

func TestPartitionsEndpointDefaultMissingReturns503(t *testing.T) {
	store := newTaskQStore(t)
	handler := readyHandler(store)

	// No override for "unknown-q" and no _default_ row bootstrapped -> ResolvePartitions errors.
	rec, _ := doJSON(t, handler, http.MethodGet, "/admin/partitions?queue=unknown-q", nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET status = %d, want 503", rec.Code)
	}
	rec, _ = doJSON(t, handler, http.MethodPost, "/admin/partitions", map[string]any{"queue": "unknown-q", "partitions": 4})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST status = %d, want 503", rec.Code)
	}
}

// TestGrowPartitionsConcurrentEndsAtLargest 证明 grow-only 判定已无 TOCTOU：两个并发的
// GrowPartitions（目标 8 与 16）不论完成顺序，最终值都必须是较大的 16——若判定仍是
// read-check-write 三步，两者都能基于「当前=4」的旧快照通过检查，后落地的 8 会覆盖 16
// （last-writer-wins），本断言即会失败。
func TestGrowPartitionsConcurrentEndsAtLargest(t *testing.T) {
	store := newTaskQStore(t)
	ctx := context.Background()
	if err := store.SetPartitions(ctx, "payments-q", 4, 4); err != nil {
		t.Fatal(err)
	}

	targets := []int{8, 16}
	errs := make([]error, len(targets))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, target := range targets {
		wg.Go(func() {
			<-start
			err := store.GrowPartitions(ctx, "payments-q", target, target)
			if errors.Is(err, taskqgormstore.ErrPartitionShrinkRejected) {
				// 8 在 16 之后到达时被合法拒绝，不算失败。
				err = nil
			}
			errs[i] = err
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("GrowPartitions(target=%d): %v", targets[i], err)
		}
	}

	read, write, err := store.ResolvePartitions(ctx, "payments-q")
	if err != nil {
		t.Fatal(err)
	}
	if read != 16 || write != 16 {
		t.Fatalf("resolved = %d/%d, want 16/16 (larger target must win, not last-writer)", read, write)
	}
}

// --- startup initialization tests ---

func TestInitializeQueuePartitionsWritesConfiguredRoutes(t *testing.T) {
	store := newTaskQStore(t)
	app := &application{
		telemetry:  telemetry.NoopProviders(),
		taskQStore: store,
		cfg: Config{TaskQ: TaskQConfig{
			WorkflowQueue: "workflow-tasks",
			ActivityQueues: map[string]ActivityQueueConfig{
				"payments": {Physical: "payments-q", Partitions: 8},
				"emails":   {Physical: "emails-q"}, // no partitions -> no override row
			},
		}},
	}
	if err := app.initializeQueuePartitions(context.Background()); err != nil {
		t.Fatalf("initialize: %v", err)
	}

	// Sized route wrote an override row equal to configured partitions, and its physical
	// name matches the router's single source of truth.
	physical := wftaskq.PhysicalQueueName(app.activityPhysicalOverrides(), "default", "payments")
	if physical != "payments-q" {
		t.Fatalf("physical name = %q, want payments-q", physical)
	}
	read, write, err := store.ResolvePartitions(context.Background(), "payments-q")
	if err != nil || read != 8 || write != 8 {
		t.Fatalf("payments-q resolved = %d/%d err=%v, want 8/8", read, write, err)
	}

	// Unsized route wrote no override row -> ResolvePartitions falls back and (no _default_) errors.
	if _, _, err := store.ResolvePartitions(context.Background(), "emails-q"); err == nil {
		t.Fatal("expected no override row for unsized route emails-q")
	}
}

// TestInitializeQueuePartitionsIsGrowOnly 回归:进程重启时启动初始化不得把运行时已扩大的
// 分区数缩回配置值。DB 里 activity 与 workflow queue 各已有 override=16(模拟运行时经
// /admin/partitions 扩容),config 仍配 partitions=8,跑 initializeQueuePartitions 后两者
// 都应保持 16(未被缩回),且不 fatal。
func TestInitializeQueuePartitionsIsGrowOnly(t *testing.T) {
	store := newTaskQStore(t)
	ctx := context.Background()
	if err := store.SetPartitions(ctx, "payments-q", 16, 16); err != nil {
		t.Fatal(err)
	}
	if err := store.SetPartitions(ctx, "workflow-tasks", 16, 16); err != nil {
		t.Fatal(err)
	}
	app := &application{
		telemetry:  telemetry.NoopProviders(),
		taskQStore: store,
		cfg: Config{TaskQ: TaskQConfig{
			WorkflowQueue:           "workflow-tasks",
			WorkflowQueuePartitions: 8,
			ActivityQueues: map[string]ActivityQueueConfig{
				"payments": {Physical: "payments-q", Partitions: 8},
			},
		}},
	}
	if err := app.initializeQueuePartitions(ctx); err != nil {
		t.Fatalf("initialize must not fatal on grow-only skip: %v", err)
	}
	if read, write, _ := store.ResolvePartitions(ctx, "payments-q"); read != 16 || write != 16 {
		t.Fatalf("payments-q = %d/%d, want kept at runtime 16/16 (not shrunk to 8)", read, write)
	}
	if read, write, _ := store.ResolvePartitions(ctx, "workflow-tasks"); read != 16 || write != 16 {
		t.Fatalf("workflow-tasks = %d/%d, want kept at runtime 16/16 (not shrunk to 8)", read, write)
	}
}

// TestInitializeQueuePartitionsGrowsWhenConfiguredLarger 验证目标值更大时正常扩容写入。
func TestInitializeQueuePartitionsGrowsWhenConfiguredLarger(t *testing.T) {
	store := newTaskQStore(t)
	ctx := context.Background()
	if err := store.SetPartitions(ctx, "payments-q", 4, 4); err != nil {
		t.Fatal(err)
	}
	app := &application{
		telemetry:  telemetry.NoopProviders(),
		taskQStore: store,
		cfg: Config{TaskQ: TaskQConfig{
			WorkflowQueue: "workflow-tasks",
			ActivityQueues: map[string]ActivityQueueConfig{
				"payments": {Physical: "payments-q", Partitions: 8},
			},
		}},
	}
	if err := app.initializeQueuePartitions(ctx); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if read, write, _ := store.ResolvePartitions(ctx, "payments-q"); read != 8 || write != 8 {
		t.Fatalf("payments-q = %d/%d, want grown to 8/8", read, write)
	}
}

func TestInitializeQueuePartitionsWritesWorkflowQueue(t *testing.T) {
	store := newTaskQStore(t)
	app := &application{
		telemetry:  telemetry.NoopProviders(),
		taskQStore: store,
		cfg: Config{TaskQ: TaskQConfig{
			WorkflowQueue:           "workflow-tasks",
			WorkflowQueuePartitions: 6,
		}},
	}
	if err := app.initializeQueuePartitions(context.Background()); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	read, write, err := store.ResolvePartitions(context.Background(), "workflow-tasks")
	if err != nil || read != 6 || write != 6 {
		t.Fatalf("workflow-tasks resolved = %d/%d err=%v, want 6/6", read, write, err)
	}
}
