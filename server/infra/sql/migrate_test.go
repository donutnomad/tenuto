package sql

import (
	"strings"
	"testing"
	"testing/fstest"
)

// appliedChecksums 是已发布迁移的 SHA-256。已发布的迁移不再修改：改动它们会让已执行过的库在 migrate 与启动检查时报 checksum 不一致。
// 新增迁移时在这里追加它的 checksum（测试失败信息里给出），已有的值不改。
var appliedChecksums = map[string]map[string]string{
	"tenuto": {
		"0001_init": "8dea9b99d89e83c2d954a5d5381e0873801f5a98425a51efbb537e342bffc943",
	},
	"taskq": {
		"0001_init": "8fb9cecc9a83a709b4fc302c70e457813d48d5c362ca65d5775f3286b170020e",
	},
}

// TestMigrationChecksumsAreStable 断言每个迁移文件的 checksum 与登记的值相同，每个迁移都已登记。
func TestMigrationChecksumsAreStable(t *testing.T) {
	for _, migrations := range []Migrations{TenutoMigrations, TaskQMigrations} {
		want := appliedChecksums[migrations.Component]
		for _, migration := range migrations.List {
			got := migration.Checksum
			switch recorded, ok := want[migration.Name]; {
			case !ok:
				t.Errorf("%s migration %s 没有登记 checksum：在 appliedChecksums 里加上 %q: %q", migrations.Component, migration.Name, migration.Name, got)
			case recorded != got:
				t.Errorf("%s migration %s 的内容变了（checksum %s，登记的是 %s）：已发布的迁移不能修改，改表请新增迁移文件", migrations.Component, migration.Name, got, recorded)
			}
		}
		if len(want) != len(migrations.List) {
			t.Errorf("%s：appliedChecksums 登记了 %d 个迁移，目录里有 %d 个", migrations.Component, len(want), len(migrations.List))
		}
	}
}

// TestLoadMigrationsRejectsBadDirectories 覆盖文件名、编号连续与空文件的校验。
func TestLoadMigrationsRejectsBadDirectories(t *testing.T) {
	file := func(content string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(content)} }
	const create = "CREATE TABLE t (id INT);\n"
	cases := map[string]struct {
		files fstest.MapFS
		want  string
	}{
		"gap":           {fstest.MapFS{"m/0001_a.sql": file(create), "m/0003_c.sql": file(create)}, "want 2"},
		"duplicate":     {fstest.MapFS{"m/0001_a.sql": file(create), "m/0001_b.sql": file(create)}, "want 2"},
		"starts at 2":   {fstest.MapFS{"m/0002_a.sql": file(create)}, "want 1"},
		"bad name":      {fstest.MapFS{"m/0001_A-b.sql": file(create)}, "not a migration file"},
		"other file":    {fstest.MapFS{"m/0001_a.sql": file(create), "m/README.md": file("x")}, "not a migration file"},
		"no statements": {fstest.MapFS{"m/0001_a.sql": file("-- only a comment\n")}, "no statements"},
		"subdirectory":  {fstest.MapFS{"m/.keep/x": file("")}, "not a migration file"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := LoadMigrations("test", tc.files, "m")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// 换行统一为 LF 后计算 checksum：CRLF 检出的文件与 LF 检出的文件是同一个迁移。
func TestMigrationChecksumIgnoresLineEndings(t *testing.T) {
	lf, err := LoadMigrations("test", fstest.MapFS{"m/0001_a.sql": {Data: []byte("CREATE TABLE t (\n  id INT\n);\n")}}, "m")
	if err != nil {
		t.Fatal(err)
	}
	crlf, err := LoadMigrations("test", fstest.MapFS{"m/0001_a.sql": {Data: []byte("CREATE TABLE t (\r\n  id INT\r\n);\r\n")}}, "m")
	if err != nil {
		t.Fatal(err)
	}
	if lf.List[0].Checksum != crlf.List[0].Checksum {
		t.Fatalf("checksum differs between LF and CRLF: %s vs %s", lf.List[0].Checksum, crlf.List[0].Checksum)
	}
}

func testMigrations(t *testing.T, count int) Migrations {
	t.Helper()
	files := fstest.MapFS{}
	for i := 1; i <= count; i++ {
		name := "m/000" + string(rune('0'+i)) + "_step.sql"
		files[name] = &fstest.MapFile{Data: []byte("CREATE TABLE t" + string(rune('0'+i)) + " (id INT);\nCREATE INDEX i ON t (id);\n")}
	}
	migrations, err := LoadMigrations("test", files, "m")
	if err != nil {
		t.Fatal(err)
	}
	return migrations
}

func rowFor(m Migration, statements int, applied bool) migrationRow {
	return migrationRow{Version: m.Version, Name: m.Name, Checksum: m.Checksum, StatementsApplied: statements, Applied: applied}
}

// TestMigrationPlan 覆盖库里的记录与迁移文件比较的各种情况。
func TestMigrationPlan(t *testing.T) {
	m := testMigrations(t, 3)
	first, second, third := m.List[0], m.List[1], m.List[2]

	t.Run("empty database runs everything", func(t *testing.T) {
		plan, err := m.plan(nil)
		if err != nil || plan.current != 0 || len(plan.pending) != 3 || plan.resume != 0 {
			t.Fatalf("plan = %+v, err = %v", plan, err)
		}
	})
	t.Run("up to date", func(t *testing.T) {
		plan, err := m.plan([]migrationRow{rowFor(first, 2, true), rowFor(second, 2, true), rowFor(third, 2, true)})
		if err != nil || plan.current != 3 || len(plan.pending) != 0 || len(plan.unknown) != 0 {
			t.Fatalf("plan = %+v, err = %v", plan, err)
		}
	})
	t.Run("behind", func(t *testing.T) {
		plan, err := m.plan([]migrationRow{rowFor(first, 2, true)})
		if err != nil || plan.current != 1 || len(plan.pending) != 2 || plan.pending[0].Name != second.Name {
			t.Fatalf("plan = %+v, err = %v", plan, err)
		}
	})
	t.Run("partially applied resumes after the last applied statement", func(t *testing.T) {
		plan, err := m.plan([]migrationRow{rowFor(first, 2, true), rowFor(second, 1, false)})
		if err != nil || plan.current != 1 || len(plan.pending) != 2 || plan.pending[0].Name != second.Name || plan.resume != 1 {
			t.Fatalf("plan = %+v, err = %v", plan, err)
		}
	})
	t.Run("database ahead of the binary", func(t *testing.T) {
		plan, err := m.plan([]migrationRow{rowFor(first, 2, true), rowFor(second, 2, true), rowFor(third, 2, true),
			{Version: 4, Name: "0004_new", Checksum: "x", Applied: true}})
		if err != nil || len(plan.pending) != 0 || len(plan.unknown) != 1 || plan.unknown[0] != 4 {
			t.Fatalf("plan = %+v, err = %v", plan, err)
		}
	})
	errorCases := map[string]struct {
		rows []migrationRow
		want string
	}{
		"edited applied migration": {[]migrationRow{{Version: 1, Name: first.Name, Checksum: "other", StatementsApplied: 2, Applied: true}}, "must never be edited"},
		"edited partial migration": {[]migrationRow{{Version: 1, Name: first.Name, Checksum: "other", StatementsApplied: 1}}, "must never be edited"},
		"applied after a gap":      {[]migrationRow{rowFor(first, 2, true), rowFor(third, 2, true)}, "earlier migration 0002_step is not"},
		"partial after a gap":      {[]migrationRow{rowFor(second, 1, false)}, "earlier migration 0001_step is not"},
		"ahead while behind":       {[]migrationRow{rowFor(first, 2, true), {Version: 5, Name: "0005_new", Checksum: "x", Applied: true}}, "does not know"},
	}
	for name, tc := range errorCases {
		t.Run(name, func(t *testing.T) {
			if _, err := m.plan(tc.rows); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
