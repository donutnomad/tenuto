package app

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// probeDriver 是只支持 Ping 的 database/sql 驱动，down 为 true 时 Ping 失败。
type probeDriver struct{ down *atomic.Bool }

func (d probeDriver) Open(string) (driver.Conn, error) { return probeConn(d), nil }

type probeConn probeDriver

func (c probeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c probeConn) Close() error                        { return nil }
func (c probeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c probeConn) Ping(context.Context) error {
	if c.down.Load() {
		return driver.ErrBadConn
	}
	return nil
}

var probeDown atomic.Bool

func init() { sql.Register("tenuto-probe", probeDriver{down: &probeDown}) }

// readiness 的数据库检查用专用连接：连接池被业务占满时照常通过，数据库失联时失败，恢复后重新取连接。
func TestDBProbeDoesNotQueueOnBusyPool(t *testing.T) {
	database, err := sql.Open("tenuto-probe", "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(2)
	probe := &dbProbe{db: database}
	defer probe.close()
	check := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		return probe.check(ctx)
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
	busy, err := database.Conn(context.Background()) // 业务占满剩下的连接
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	if err := check(); err != nil {
		t.Fatalf("probe on a saturated pool = %v", err)
	}
	probeDown.Store(true)
	if err := check(); err == nil {
		t.Fatal("probe passed while the database is down")
	}
	probeDown.Store(false)
	_ = busy.Close()
	if err := check(); err != nil {
		t.Fatalf("probe after recovery = %v", err)
	}
}

// DSN 没有给出 I/O 超时时用配置的缺省值；DSN 给出的值保留；迁移用的零值不设超时。
func TestMySQLConfigAppliesDefaultIOTimeouts(t *testing.T) {
	pool := DatabaseConfig{ReadTimeout: defaultDatabaseReadTimeout, WriteTimeout: defaultDatabaseWriteTimeout}
	parsed, err := mysqlConfig("u:p@tcp(db:3306)/tenuto", pool)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ReadTimeout != defaultDatabaseReadTimeout || parsed.WriteTimeout != defaultDatabaseWriteTimeout {
		t.Fatalf("timeouts = %s/%s", parsed.ReadTimeout, parsed.WriteTimeout)
	}
	parsed, err = mysqlConfig("u:p@tcp(db:3306)/tenuto?readTimeout=5s&writeTimeout=7s", pool)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ReadTimeout != 5*time.Second || parsed.WriteTimeout != 7*time.Second {
		t.Fatalf("DSN timeouts = %s/%s", parsed.ReadTimeout, parsed.WriteTimeout)
	}
	parsed, err = mysqlConfig("u:p@tcp(db:3306)/tenuto", DatabaseConfig{MaxOpenConns: 2, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ReadTimeout != 0 || parsed.WriteTimeout != 0 {
		t.Fatalf("migration timeouts = %s/%s", parsed.ReadTimeout, parsed.WriteTimeout)
	}
}

// 探针长期占用一条连接：连接上限为 1 的池在启动时报错，否则业务查询在池上无限等待而 readiness 照常通过。
func TestDBProbeRejectsSingleConnectionPool(t *testing.T) {
	database, err := sql.Open("tenuto-probe", "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	if _, err := newDBProbe("workflow", database); err == nil || !strings.Contains(err.Error(), "WORKFLOW_DB_MAX_OPEN_CONNS") {
		t.Fatalf("newDBProbe on a 1-connection pool = %v, want an error naming WORKFLOW_DB_MAX_OPEN_CONNS", err)
	}
	for _, limit := range []int{0, 2} {
		database.SetMaxOpenConns(limit)
		if _, err := newDBProbe("workflow", database); err != nil {
			t.Fatalf("newDBProbe with max open conns %d = %v", limit, err)
		}
	}
}
