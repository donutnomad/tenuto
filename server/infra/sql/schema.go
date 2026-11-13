// Package sql 持有 Tenuto 在 MySQL 上的 schema：按编号排序、只向前的迁移文件（migrations/ 与 taskq_migrations/，
// 嵌入二进制，由 tenuto-server migrate 执行，见 migrate.go），以及可读的当前完整 schema（schema.sql）。
// Tenuto 运行期不建表也不改表：Server 启动时只检查库已执行完全部迁移。
// 执行完 migrations/ 的库与按 schema.sql 建出的库相同，由 MySQL 守卫测试核对。
package sql

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	infradb "github.com/donutnomad/tenuto/server/infra/db"
)

// Schema 是 schema.sql 的内容：Workflow 库当前的完整表结构，供阅读、PO 守卫与迁移守卫测试对照。
//
//go:embed schema.sql
var Schema string

// Exec 逐条执行 script 里的语句。整行的 -- 注释先去掉，再按行尾的分号切分语句；
// 脚本里的注释与 COMMENT 字符串因此可以含分号与问号。
func Exec(ctx context.Context, db infradb.DB, script string) error {
	for _, statement := range Statements(script) {
		if err := db.WithContext(ctx).Exec(statement).Error; err != nil {
			return fmt.Errorf("execute %q: %w", firstLine(statement), err)
		}
	}
	return nil
}

// Statements 返回 script 里以行尾分号结束的语句，去掉整行注释与首尾空白，不含结尾的分号。
func Statements(script string) []string {
	var statements []string
	var current strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			statement := strings.TrimSuffix(strings.TrimSpace(current.String()), ";")
			statements = append(statements, statement)
			current.Reset()
		}
	}
	if rest := strings.TrimSpace(current.String()); rest != "" {
		statements = append(statements, rest)
	}
	return statements
}

func firstLine(statement string) string {
	line, _, _ := strings.Cut(statement, "\n")
	return line
}
