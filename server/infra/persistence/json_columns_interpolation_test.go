package persistence

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/infra/persistence/mysqltest"
	"gorm.io/gorm"
)

// 客户端插值（interpolateParams=true，Server 与 mysqltest 都这样连接）下 []byte 参数以 _binary'...' 发出，MySQL 拒绝
// 用它构造 JSON 值。本用例在插值连接上写入并读回每一个 JSON 列，并核对：库里的 JSON 列正好是这里覆盖的这些（新增
// JSON 列时这里失败，提醒把它的 PO 字段绑定成字符串）；整个过程没有一条语句走服务端预处理。
func TestJSONColumnsUnderClientSideInterpolation(t *testing.T) {
	db := mysqltest.NewDB(t)
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	// 一个连接：会话级的 Com_stmt_prepare 计数覆盖下面全部语句。
	sqlDB.SetMaxOpenConns(1)

	var columns []string
	if err := db.Raw(`SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND DATA_TYPE = 'json' ORDER BY 1`).Scan(&columns).Error; err != nil {
		t.Fatal(err)
	}
	covered := []string{"tenuto_activity_states.trace_context_json", "tenuto_timer_states.trace_context_json"}
	if !slices.Equal(columns, covered) {
		t.Fatalf("JSON columns = %v, this test covers %v", columns, covered)
	}

	store, err := New(db, Config{})
	if err != nil {
		t.Fatal(err)
	}
	prepared := sessionStatementPrepares(t, db)
	now := time.Now().UTC().Truncate(time.Millisecond)
	trace := map[string]string{"traceparent": `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`, "quote'and\\backslash": "值"}

	for _, traceContext := range []map[string]string{trace, nil} {
		eventID := int64(len(traceContext) + 1)
		activity := &execution.ActivityState{
			RunID: "run-json", ScheduledEventID: eventID, ActivityID: "a", ActivityType: "t", TaskQueue: "q",
			Attempt: 1, Stamp: 1, Status: execution.ActivityStatusEnums.Scheduled, ScheduledTime: now,
			TraceContext: traceContext, CreatedAt: now, UpdatedAt: now,
		}
		if err := store.Executions.UpsertActivityState(activity); err != nil {
			t.Fatalf("write activity trace context %v: %v", traceContext, err)
		}
		gotActivity, ok, err := store.Executions.GetActivityState("run-json", eventID)
		if err != nil || !ok {
			t.Fatalf("read activity: ok=%v err=%v", ok, err)
		}
		if !maps.Equal(gotActivity.TraceContext, traceContext) {
			t.Fatalf("activity trace context = %v, want %v", gotActivity.TraceContext, traceContext)
		}

		timer := &execution.TimerState{
			RunID: "run-json", ScheduledEventID: eventID, TimerID: "t", Deadline: now.Add(time.Minute),
			Status: execution.TimerStatusEnums.Scheduled, Kind: execution.TimerKindEnums.UserTimer,
			TraceContext: traceContext, CreatedAt: now, UpdatedAt: now,
		}
		if err := store.Executions.UpsertTimerState(timer); err != nil {
			t.Fatalf("write timer trace context %v: %v", traceContext, err)
		}
		gotTimer, ok, err := store.Executions.GetTimerState("run-json", eventID)
		if err != nil || !ok {
			t.Fatalf("read timer: ok=%v err=%v", ok, err)
		}
		if !maps.Equal(gotTimer.TraceContext, traceContext) {
			t.Fatalf("timer trace context = %v, want %v", gotTimer.TraceContext, traceContext)
		}
	}
	// 列里存的是 JSON 值：MySQL 能按路径取出字段。
	var traceparent string
	if err := db.Raw(`SELECT JSON_UNQUOTE(JSON_EXTRACT(trace_context_json, '$.traceparent')) FROM tenuto_timer_states
		WHERE run_id = ? AND scheduled_event_id = ?`, "run-json", int64(len(trace)+1)).Scan(&traceparent).Error; err != nil {
		t.Fatal(err)
	}
	if traceparent != trace["traceparent"] {
		t.Fatalf("JSON_EXTRACT traceparent = %q", traceparent)
	}
	if got := sessionStatementPrepares(t, db); got != prepared {
		t.Fatalf("Com_stmt_prepare went from %d to %d: the connection prepares statements instead of interpolating", prepared, got)
	}
}

func sessionStatementPrepares(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var row struct {
		VariableName string `gorm:"column:Variable_name"`
		Value        int64  `gorm:"column:Value"`
	}
	if err := db.Raw("SHOW SESSION STATUS LIKE 'Com_stmt_prepare'").Scan(&row).Error; err != nil {
		t.Fatal(err)
	}
	return row.Value
}
