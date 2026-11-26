package db_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/donutnomad/gsql"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"github.com/donutnomad/tenuto/server/infra/persistence/executionrepo"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var errConnectionLost = errors.New("connection lost while reading rows")

// brokenRowsConnector 的每个查询返回 goodRows 行 run_id 之后读取失败，模拟读结果集时连接中断。
type brokenRowsConnector struct{ goodRows int }

func (c brokenRowsConnector) Connect(context.Context) (driver.Conn, error) { return brokenRowsConn(c), nil }
func (c brokenRowsConnector) Driver() driver.Driver                        { return nil }

type brokenRowsConn brokenRowsConnector

func (brokenRowsConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (brokenRowsConn) Close() error                        { return nil }
func (brokenRowsConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c brokenRowsConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &brokenRows{remaining: c.goodRows}, nil
}

type brokenRows struct{ remaining int }

func (*brokenRows) Columns() []string { return []string{"run_id"} }
func (*brokenRows) Close() error      { return nil }
func (r *brokenRows) Next(dest []driver.Value) error {
	if r.remaining == 0 {
		return errConnectionLost
	}
	r.remaining--
	dest[0] = "run-1"
	return nil
}


func openBrokenRowsDB(t *testing.T, goodRows int) *gorm.DB {
	t.Helper()
	pool := sql.OpenDB(brokenRowsConnector{goodRows: goodRows})
	t.Cleanup(func() { _ = pool.Close() })
	db, err := gorm.Open(mysql.New(mysql.Config{Conn: pool, SkipInitializeWithVersion: true}), &gorm.Config{DisableAutomaticPing: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// gsql v0.3.1 的 Find 在读第一行失败时只看 RowsAffected == 0，返回空结果、丢弃读取错误；infradb.Find 返回它。
// 读出若干行之后失败时两者都返回错误。
func TestFindReturnsRowsReadError(t *testing.T) {
	query := func() *gsql.QueryBuilderG[executionrepo.TimerStatePO] {
		return gsql.From[executionrepo.TimerStatePO](executionrepo.TimerStateSchema).
			Where(executionrepo.TimerStateSchema.RunID.Eq("run-1"))
	}

	rows, err := query().Find(openBrokenRowsDB(t, 0))
	if err != nil || rows != nil {
		t.Fatalf("gsql Find on a failed first row = %d rows, %v; the dependency now reports the error, drop infradb.Find", len(rows), err)
	}

	for _, goodRows := range []int{0, 1} {
		rows, err := infradb.Find(openBrokenRowsDB(t, goodRows), query())
		if !errors.Is(err, errConnectionLost) || rows != nil {
			t.Fatalf("good rows %d: infradb.Find = %d rows, %v; want errConnectionLost", goodRows, len(rows), err)
		}
	}
}
