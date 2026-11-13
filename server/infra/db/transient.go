package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"syscall"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// MySQL 的瞬时错误码：数据库暂时不能服务，稍后重试可以成功。
var transientMySQLErrors = map[uint16]bool{
	1040: true, // ER_CON_COUNT_ERROR：Too many connections
	1053: true, // ER_SERVER_SHUTDOWN：服务端正在关闭
	1203: true, // ER_TOO_MANY_USER_CONNECTIONS：超过 max_user_connections
	1205: true, // ER_LOCK_WAIT_TIMEOUT：InnoDB 锁等待超时
	1213: true, // ER_LOCK_DEADLOCK：死锁，InnoDB 回滚整个事务
	1317: true, // ER_QUERY_INTERRUPTED：语句被 KILL QUERY 或服务端关闭中止
	2006: true, // CR_SERVER_GONE_ERROR：MySQL server has gone away
	2013: true, // CR_SERVER_LOST：Lost connection to MySQL server during query
	4031: true, // ER_CLIENT_INTERACTION_TIMEOUT：服务端因空闲超时断开连接（8.0.24+）
}

// IsTransient 报告 err 是否是数据库的瞬时失败：连接数已满、锁等待超时、死锁、连接没有建立或在途中断开、
// 读写超时。错误链上任何一层（含 errors.Join）命中即可。
//
// 瞬时失败不说明这次请求有没有部分生效：连接可能断在 COMMIT 之后，多步操作的前几步可能已经提交。
// context 的取消与超时返回 false：它们来自调用方的期限，由调用方映射（context.DeadlineExceeded 也满足
// net.Error，需要先排除）。
func IsTransient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var mysqlErr *mysqldriver.MySQLError
	if errors.As(err, &mysqlErr) {
		return transientMySQLErrors[mysqlErr.Number]
	}
	// ErrBadConn：database/sql 换连接重试后仍失败；ErrInvalidConn：驱动在读写途中发现连接已断。
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysqldriver.ErrInvalidConn) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
