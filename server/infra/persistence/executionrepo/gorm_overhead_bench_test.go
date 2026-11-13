package executionrepo

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// BenchmarkAppendProjectedEventGoSide 量出一个只追加一个事件的事务（加锁读 execution state、插入事件、更新 state 三条语句）
// 在 Go 侧的 CPU：gsql 构建、gorm 回调与反射扫描、database/sql。驱动是进程内的假驱动，每条查询返回同一行
// state，不含 MySQL 协议编解码、网络与服务端执行。
func BenchmarkAppendProjectedEventGoSide(b *testing.B) {
	repo, _ := newCannedRepository(b)
	event := history.Event{ActivityCompleted: &history.ActivityCompletedEvent{ActivityID: "seq-1", ScheduledEventID: 12, Attempt: 1, ActivityType: "cluster.compute", TaskQueue: "cluster", Output: map[string]any{"value": int64(7)}}}
	b.ReportAllocs()
	for b.Loop() {
		if err := repo.inTx(func(tx *gormExecutionTx) error {
			_, err := tx.Append("run-1", "tree-1", "branch-1", event)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// newCannedRepository 返回连到 cannedConnector 的仓储；查询读到的 execution state 是一个 NextEventID 为 40 的
// Running Run。
func newCannedRepository(tb testing.TB) (*Repository, *cannedConnector) {
	tb.Helper()
	state := execution.NewWorkflowExecutionState(&execution.Run{ID: "run-1", Status: history.StatusRunning})
	state.NextEventID = 40
	row, err := executionStateToRow(state)
	if err != nil {
		tb.Fatal(err)
	}
	connector := &cannedConnector{row: row}
	sqlDB := sql.OpenDB(connector)
	tb.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: sqlDB, SkipInitializeWithVersion: true}),
		&gorm.Config{TranslateError: true, DisableAutomaticPing: true})
	if err != nil {
		tb.Fatal(err)
	}
	repo, err := New(db, Config{})
	if err != nil {
		tb.Fatal(err)
	}
	return repo, connector
}

// cannedConnector 是只为测试准备的 database/sql 驱动：查询一律返回 row 这一行，写入一律影响一行。
// statements 记下每条发出的 SQL（不含 BEGIN/COMMIT）。
type cannedConnector struct {
	row        *WorkflowExecutionStatePO
	once       sync.Once
	columns    []string
	values     []driver.Value
	mu         sync.Mutex
	statements []string
}

func (c *cannedConnector) record(query string) {
	c.mu.Lock()
	c.statements = append(c.statements, query)
	c.mu.Unlock()
}

func (c *cannedConnector) Connect(context.Context) (driver.Conn, error) {
	c.once.Do(func() {
		parsed, err := schema.Parse(c.row, &sync.Map{}, schema.NamingStrategy{})
		if err != nil {
			panic(err)
		}
		rv := reflect.ValueOf(c.row)
		for _, field := range parsed.Fields {
			if field.DBName == "" {
				continue
			}
			value, zero := field.ValueOf(context.Background(), rv)
			c.columns = append(c.columns, field.DBName)
			c.values = append(c.values, driverValue(value, zero))
		}
	})
	return cannedConn{c}, nil
}

func (c *cannedConnector) Driver() driver.Driver { return nil }

func driverValue(value any, zero bool) driver.Value {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		value = rv.Elem().Interface()
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case []byte:
		if zero {
			return nil
		}
		return v
	case string, int64, bool, time.Time:
		return v
	default:
		panic("unsupported canned value")
	}
}

type cannedConn struct{ c *cannedConnector }

func (cannedConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (cannedConn) Close() error                        { return nil }
func (cannedConn) Begin() (driver.Tx, error)           { return cannedTx{}, nil }
func (cannedConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return cannedTx{}, nil
}
func (c cannedConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.c.record(query)
	return cannedResult{}, nil
}
func (c cannedConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.c.record(query)
	return &cannedRows{c: c.c}, nil
}

type cannedResult struct{}

func (cannedResult) LastInsertId() (int64, error) { return 1, nil }
func (cannedResult) RowsAffected() (int64, error) { return 1, nil }

type cannedTx struct{}

func (cannedTx) Commit() error   { return nil }
func (cannedTx) Rollback() error { return nil }

type cannedRows struct {
	c    *cannedConnector
	done bool
}

func (r *cannedRows) Columns() []string { return r.c.columns }
func (r *cannedRows) Close() error      { return nil }
func (r *cannedRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	copy(dest, r.c.values)
	return nil
}
