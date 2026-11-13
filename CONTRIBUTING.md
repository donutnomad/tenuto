# Contributing to Tenuto

Thanks for your interest in Tenuto. This guide covers the local toolchain, the
commands the repository provides, and the rules a change has to follow before
review. The user documentation lives in `docs/` and is written in Chinese;
issues and pull requests may be written in Chinese or English.

## Prerequisites

| Tool | Version | Used for |
| --- | --- | --- |
| Go | the version declared in `go.mod` | server, SDK, worker, compiler, Console WASM module |
| Node.js | 20 or newer | Next.js Console and VitePress docs |
| pnpm | 10 or newer | Console and docs dependencies |
| make, curl, lsof | system versions | `Makefile` targets and health checks |
| Docker | optional | MySQL tests through testcontainers |

All Go commands run from the repository root with `GOWORK=off`; the `Makefile`
sets it for every target.

```bash
git clone https://github.com/donutnomad/tenuto && cd tenuto
pnpm --dir console install
pnpm --dir docs install
```

## Make targets

| Target | What it does |
| --- | --- |
| `make generate` | Runs the repository-wide code generation (`go generate ./...`) |
| `make vet` | `go vet ./...` |
| `make test` | `go test ./...` and the Console unit tests |
| `make check` | `generate`, fails on any resulting diff, then `vet` and `test` |
| `make dev` | Builds the WASM module and starts the server, the demo worker/seeder and the Console |
| `make dev-backend` | Starts only the server and the demo worker/seeder in the background |
| `make dev-server` | Runs the server in the foreground with the memory backend |
| `make dev-demo` | Runs the demo worker/seeder against a running server |
| `make dev-console` | Builds the WASM module and runs the Console dev server |
| `make stop` | Stops the background processes started by `dev-backend` (PID files in `.run/`) |
| `make wasm` | Builds `console/public/replay.wasm` and copies `wasm_exec.js` |
| `make build-server` | Builds `bin/tenuto-server` |
| `make build-demo` | Builds `bin/tenuto-demo` |
| `make build-example` | Generates and builds the startup-workflow example worker |
| `make run-server` | Runs the server with configuration from the environment |
| `make dev-docs` | Serves the docs site at http://localhost:7402/tenuto/ |
| `make build-docs` | Builds the docs site |
| `make check-docs` | Checks docs pages and links, tests `docs/examples`, builds the docs site |
| `make preview-docs` | Serves the built docs site |

Ports default to `APP_PORT=7400`, `UI_PORT=7401` and `DOCS_PORT=7402`; override
them on the command line, for example `make dev APP_PORT=7500 UI_PORT=7501`.
If the Console dev server misses file changes, pass
`CONSOLE_DEV_ENV=WATCHPACK_POLLING=true`.

## Code generation

The repository keeps its `//go:generate` directives in one place,
`server/cmd/tenuto-server/main.go`. They cover domain setters, PO patches, gsql
schemas, workflow artifacts and the other annotation targets. Run generation
from the repository root:

```bash
GOWORK=off go generate ./...    # or: make generate
```

Do not add `//go:generate` directives elsewhere; packages carry only their
annotations and the generated files. Generated files (`generate.go`,
`zz_*_gen.go`, the example `*.json` artifacts) are maintained by the
generators: change the source or the annotation and regenerate. Commit the
generated diff together with the change that caused it; CI fails when
`go generate ./...` leaves a diff.

## Running tests

```bash
GOWORK=off go test ./...                 # Go tests on the in-memory backend
GOWORK=off go vet ./...
pnpm --dir console test                  # Console unit tests (vitest)
pnpm --dir console exec tsc --noEmit     # Console type check
pnpm --dir docs check                    # docs frontmatter, navigation and links
pnpm --dir docs build                    # docs site build
```

MySQL is the only SQL backend. Tests of the GORM repositories, TaskQ and the
application entry point are opt-in: without the variables below they skip, and
`go test ./...` needs neither Docker nor a database. Each MySQL test creates its
own database and builds it through the same migrations that
`tenuto-server migrate` applies (`server/infra/sql/migrations`, and
`server/infra/sql/taskq_migrations` for TaskQ); nothing creates tables at
runtime.

```bash
# Use an existing MySQL server; each test creates its own database there
TENUTO_TEST_MYSQL=1 TENUTO_TEST_MYSQL_DSN='user:password@tcp(127.0.0.1:3306)/' \
  GOWORK=off go test ./server/... -count=1

# Start a disposable mysql:8.0 container through testcontainers (needs Docker)
TENUTO_TEST_MYSQL=1 GOWORK=off go test ./server/... -count=1
```

CI runs the first form in the `mysql` job against a MySQL 8 service container.

See `docs/develop/testing.md` for the test layers and the MySQL coverage.

## Changing a table

Tables are created and upgraded only by the versioned, forward-only migrations
embedded in `tenuto-server` and applied with `tenuto-server migrate`. A schema
change updates four places in the same commit:

1. Add the next numbered file to `server/infra/sql/migrations/`, for example
   `0002_add_run_owner.sql`. Every statement ends with a semicolon at the end of
   a line; whole-line `--` comments may sit between statements.
2. Update `server/infra/sql/schema.sql` so it equals the schema after all
   migrations. It stays the readable full schema.
3. Update the PO fields and the PO's `MysqlCreateTable()` (identical to the
   table's statement in `schema.sql`), then run `go generate ./...`.
4. Record the new migration's checksum in `appliedChecksums` in
   `server/infra/sql/migrate_test.go`; the failing test prints the value.

Never edit a migration that has been released: the checksum guard fails, and
databases that already ran it refuse to migrate or start. Fix a released
migration with a new one. Each migration keeps the previous release working
(add first; drop or rename in a later release), because replicas of the old
version keep running during a rolling upgrade.

The guards: `schema_guard_test.go` (PO vs `schema.sql`, no database),
`migrate_test.go` (file names, numbering, checksums, no database) and, on MySQL,
`TestTenutoMigrationsBuildSchemaSQL`, which builds one database from the
migrations and one from `schema.sql` and compares `SHOW CREATE TABLE` for every
table. When a TaskQ upgrade changes TaskQ's `store/gormstore/schema.sql`,
`TestTaskQMigrationsBuildTaskQSchemaSQL` fails until a migration is added to
`server/infra/sql/taskq_migrations/`.

## Architecture rules

Read the `## Mutable State 架构` section of `README.md` before changing
`server/biz/domain/execution`, `server/biz/service/engine` or the storage
backends in `server/infra/memory` and `server/infra/persistence`. Every
backend implements the same `ExecutionManager` contract, and a change to one
backend lands with the matching change to the other.

## Documentation and skill sync

Every functional change (SDK API, annotations, accepted workflow syntax,
worker options, server behaviour, Console operations, deployment
configuration) updates three things in the same commit:

1. The matching user page in `docs/`, with example code that matches the
   current signatures.
2. The matching entry in `docs/skills/tenuto/`: the index and key points in
   `SKILL.md` and the topic file under `references/`.
3. A passing docs build: run `node_modules/.bin/vitepress build .` in `docs/`
   (or `make build-docs`).

A new feature adds its docs page and skill entry together with the code; a
removed feature removes all three. Docs and the skill describe the current
behaviour of the code only.

## Commits and pull requests

Commit subjects follow the style already in `git log`: a type prefix, an
optional scope, and a lowercase imperative summary.

```text
feat: accept ordinary Go in workflows and make activity contracts mandatory
fix(console): refetch the full history when live pages outrun it
refactor: move the worker runtime into the worker package
docs: add the upgrade-demo example
test: enforce the dependency matrix in strict mode
```

Keep one logical change per commit. Before opening a pull request, run
`make check` and, for docs changes, `make build-docs`. Describe in the pull
request what changed, why, and how you verified it.

## Reporting security issues

Do not file security issues publicly. Follow [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
