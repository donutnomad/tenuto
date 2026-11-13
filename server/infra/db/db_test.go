package db

import (
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestConfigureMySQLSetsRequiredParameters(t *testing.T) {
	cfg, err := mysqldriver.ParseDSN("u:p@tcp(127.0.0.1:3306)/tenuto?interpolateParams=false&parseTime=false&charset=utf8mb4")
	if err != nil {
		t.Fatal(err)
	}
	if err := ConfigureMySQL(cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.InterpolateParams || !cfg.ParseTime {
		t.Fatalf("interpolateParams=%v parseTime=%v, want both true", cfg.InterpolateParams, cfg.ParseTime)
	}
	if dsn := cfg.FormatDSN(); !strings.Contains(dsn, "interpolateParams=true") || !strings.Contains(dsn, "parseTime=true") {
		t.Fatalf("DSN %q lacks the required parameters", dsn)
	}
}

// 尾字节可能是反斜杠的多字节字符集不能与客户端插值同用。
func TestConfigureMySQLRejectsCharsetsUnsafeForInterpolation(t *testing.T) {
	for _, params := range []string{"charset=gbk", "charset=utf8mb4,big5", "collation=sjis_japanese_ci"} {
		cfg, err := mysqldriver.ParseDSN("u:p@tcp(127.0.0.1:3306)/tenuto?" + params)
		if err != nil {
			t.Fatal(err)
		}
		if err := ConfigureMySQL(cfg); err == nil {
			t.Fatalf("%s: accepted with interpolateParams=true", params)
		}
	}
}
