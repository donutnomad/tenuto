package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DB 抽象本项目对数据库句柄的依赖，使调用方可以注入自己持有的 gorm 连接
// （含已包装 tracing / 多租户 scope 的实例），而不必把 *gorm.DB 的构造交给本库。
// *gorm.DB 原样满足该接口，无需适配器。
//
// 注意：`Transaction` 回调参数仍是 *gorm.DB，这是 gorm 固定的签名，无法接口化；
// 因此事务内部的代码依旧直接操作 *gorm.DB。若某处确实需要具体类型
// （如 Migrator、Callback），用 `db.WithContext(ctx)` 取即可，无需额外的还原helper。
type DB interface {
	Model(value any) (tx *gorm.DB)
	Scan(dest any) (tx *gorm.DB)
	Create(value any) (tx *gorm.DB)
	Select(query any, args ...any) (tx *gorm.DB)
	Save(value any) (tx *gorm.DB)
	Where(query any, args ...any) (tx *gorm.DB)
	Assign(attrs ...any) (tx *gorm.DB)
	Table(name string, args ...any) (tx *gorm.DB)
	Delete(value any, conds ...any) (tx *gorm.DB)
	Transaction(fn func(tx *gorm.DB) error, opts ...*sql.TxOptions) error
	Unscoped() *gorm.DB
	FirstOrCreate(dest any, conds ...any) (tx *gorm.DB)
	First(dest any, conds ...any) (tx *gorm.DB)
	Find(dest any, conds ...any) (tx *gorm.DB)
	Scopes(funcs ...func(*gorm.DB) *gorm.DB) (tx *gorm.DB)
	Clauses(conds ...clause.Expression) (tx *gorm.DB)
	Raw(sql string, values ...any) (tx *gorm.DB)
	Exec(sql string, values ...any) (tx *gorm.DB)
	WithContext(ctx context.Context) *gorm.DB
	DB() (*sql.DB, error)

	Pluck(column string, dest any) *gorm.DB
	Count(count *int64) *gorm.DB
	Updates(values any) *gorm.DB
	Update(column string, value any) *gorm.DB
	UpdateColumn(column string, value any) *gorm.DB
	UpdateColumns(values any) *gorm.DB
	Take(dest any, conds ...any) *gorm.DB
	Last(dest any, conds ...any) *gorm.DB

	Distinct(args ...any) *gorm.DB
	Offset(offset int) *gorm.DB
	Limit(limit int) *gorm.DB
	Order(value any) *gorm.DB
	Group(name string) *gorm.DB

	Joins(query string, args ...any) *gorm.DB
	InnerJoins(query string, args ...any) *gorm.DB
	Session(config *gorm.Session) *gorm.DB
}

// 编译期断言：调用方可直接传入 *gorm.DB。
var _ DB = (*gorm.DB)(nil)

// unsafeInterpolationCharsets 是客户端插值不能安全转义的连接字符集：它们的多字节字符的尾字节可能是 0x5c（反斜杠）。
// 驱动对这些字符集的 collation 拒绝插值，charset 参数由这里检查。
var unsafeInterpolationCharsets = map[string]bool{
	"big5": true, "gbk": true, "gb2312": true, "gb18030": true, "sjis": true, "cp932": true,
}

// ConfigureMySQL 设置 Tenuto 与 TaskQ 的 MySQL 连接依赖的 DSN 参数，并校验它们能一起使用：
//
//   - parseTime=true：DATETIME 列读成 time.Time。
//   - interpolateParams=true：驱动在客户端把参数插进 SQL 文本，每条带参数的语句一次往返。不插值时 database/sql 对每条
//     带参数的语句依次发 prepare、execute、close（close 不等应答），每条多一次往返，语句文本也要在服务端多解析一次。
//     插值后 []byte 参数以 _binary'...' 发出，MySQL 不接受 binary 字符集的字符串作为 JSON 列的值，所以写进 JSON 列的
//     参数一律绑定成字符串（见 executionrepo 的 trace_context_json）。时间参数按 loc 格式化，与服务端预处理时相同；
//     NO_BACKSLASH_ESCAPES 由驱动按连接状态处理。
//
// 时区由调用方决定：Server 固定 loc=UTC。连接字符集或 collation 不能安全插值（GBK、Big5、SJIS 等）时返回错误。
func ConfigureMySQL(cfg *mysqldriver.Config) error {
	cfg.ParseTime = true
	cfg.InterpolateParams = true
	formatted := cfg.FormatDSN()
	if _, query, ok := strings.Cut(formatted, "?"); ok {
		params, err := url.ParseQuery(query)
		if err != nil {
			return fmt.Errorf("parse MySQL DSN parameters: %w", err)
		}
		for _, charset := range strings.Split(params.Get("charset"), ",") {
			if unsafeInterpolationCharsets[strings.ToLower(strings.TrimSpace(charset))] {
				return fmt.Errorf("MySQL DSN charset %q cannot be used with interpolateParams=true; use utf8mb4", charset)
			}
		}
	}
	// ParseDSN 拒绝与插值同用的不安全 collation。
	if _, err := mysqldriver.ParseDSN(formatted); err != nil {
		return fmt.Errorf("MySQL DSN with interpolateParams=true: %w", err)
	}
	return nil
}
