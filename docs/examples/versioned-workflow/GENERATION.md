# 注解生成（tenuto-workflow-gen）

本目录的 v1/v2、activities、remote、worker 都由 `compiler/cmd/tenuto-workflow-gen`
从 Go 注解生成可注册产物。本文是当前入口与生成规则；README.md 保留 JSON 形状、版本语义与验收入口。

## 注解语法

逗号分隔的 `key=value`，值可以带双引号也可以不带。注解写在声明的文档注释里。

```go
// @Workflow(type="lifecycle-demo",name="Lifecycle Demo",policy=auto_upgrade,version=v2)
func Lifecycle(ctx workflow.Context, in Input) (Output, error)

// @Activity(type="payments.charge",queue="payments")
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error)

// @RemoteActivity(type="payments.settle",queue="payments")
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)
```

`@Workflow` 的 key：`type`（必填）、`name`、`policy`、`version`。
`policy` 缺省 `auto_upgrade`，合法值 `auto_upgrade` / `pinned`；`policy` 与 `version`
原样写入 `workflow.json` 的 `policy` / `version`。

`version` 是作者声明的发布标签，会写进 config JSON 并参与内容 hash；服务端存储和 Run 引用的是
config JSON 的 sha256 内容版本。同一 `(namespace, workflow_id, 内容版本)` 只保留一份 JSON，
重复登记幂等；Pinned Run 保存版本引用，多个 Run 共享同一份内容，不为每个 Run 复制 JSON。

`@Activity` 与 `@RemoteActivity` 的 key：`type`（必填）、`queue`。`@RemoteActivity` 只能标注**命名函数类型**
（`type X func(context.Context, In) (Out, error)`），有函数体的实现用 `@Activity`。`queue` 为空时定义不声明队列，
由 Server 按 Worker 的登记选路。

注解写错、未知 key、缺 `id`/`type`、重复 id、未知 policy 都会报 `file:line`。

## 生成产物

| 文件 | 内容 |
| --- | --- |
| `workflow.json` | Runtime 接受的 WorkflowConfig：`program`（WLang 程序）、`activities`（符号 → `type` / `task_queue` / `input` / `output` wire 指纹）、`policy` / `version` |
| `zz_<pkg>_contract_gen.go` | 每个契约包的 typed 桥与稳定 marker，本包有本地实现时额外给出 `RegisterActivities`（Code generated） |
| `zz_<pkg>_artifacts_gen.go` | 把同目录 `workflow.json` embed 进二进制，并导出 `WorkflowDefinition()`（Code generated） |

`input` / `output` 是 wire 指纹（`wire:<hash>`）：`config.WLangWireFingerprint` 把 WLang 类型描述符转成
与 Go 包名/类型名无关的 JSON 结构指纹，worker 侧用 `activity.ContractOf[I, O]` 计算同一算法，因此
跨模块各自定义的 DTO 只要 JSON 字段名、类型与可选性一致就判定匹配。本地实现的契约由生成 helper
启动时用 `ContractOf` 现算；`workflow.json` 的 `activities` 是同一算法在生成侧的结果，两边不一致由服务端注册时对照
（`docs/examples/versioned-workflow/example_test.go` 另有逐字段回归）。

`symbol` 是 marker 在翻译产物里的符号：同包实现形如 `v1.Charge`，跨包形如 `activities.Charge`，
remote 契约形如 `remote.SettleRemoteActivity`。remote 的 marker 生成成**可导出标记函数**而不是零值变量：
go2wlang 只把包级函数翻译成 `symbol` 节点，包级变量会退化成 `var` 节点，运行期就会报
`unknown package`。标记函数带 panic body，运行期只按符号绑定 activity type，从不调用。

同包（workflow 与 contract 在同一个包里）与跨包的调用都支持：`CallCharge(ctx, in)` 与
`activities.CallCharge(ctx, in)` 走同一条重写路径，marker 分别是不带限定符的 `ChargeRemoteActivity`
和带别名的 `remote.SettleRemoteActivity`。

直调注解函数本身（`return Charge(ctx, in)` / `return activities.Charge(...)`）不是支持的写法：
生成器在重写阶段报 `file:line` 错误，指出应改用生成的桥（错误信息带桥名，如 `CallCharge`），
并且失败时不写任何 JSON 产物。这样不会出现“Go 能编译、WLang 却把 activity 当普通函数内联、
worker 收不到派发”的静默错误。

产物里的 `source` / `metadata.source` 是**相对包目录**的路径（如 `workflow.go`），不含 checkout
绝对路径；同一份源码在不同 checkout 生成相同 definition version 与 content hash，登记幂等不被路径破坏。

## 包级批量注册（`RegisterActivities`）

生成器为每个**有本地实现**的契约包写一个批量登记 helper：

```go
func RegisterActivities(registry *activity.Registry) error
```

调用方一行把本包全部 `@Activity` 实现登记到 `registry`。每个实现的契约（`type`、输入输出 wire 指纹）
由 `activity.RegisterActivity` 从实现的参数与返回类型算出，保存在同一个 `registry` 里：

```go
registry := wfsdk.NewRegistry()
if err := activities.RegisterActivities(registry); err != nil {
    return err
}
activityWorker, err := worker.New(c, worker.Options{
    Role:       worker.RoleActivity,
    Activities: registry,
    // ...
})
```

Worker 在 `Start` 时把 `registry.Contracts()` 随路由一起上报，Server 用它对照 Workflow 侧的声明。

- 一个包多条 `@Activity` 时按桥名（`CallCharge`、`CallNotify` …）稳定排序生成，两次生成逐字节相同。
- 业务实现在另一个包时，worker 导入那个包即可调用它的 `RegisterActivities`；helper 引用的是本包实现与 DTO。
- `@RemoteActivity` 的命名函数类型只生成调用桥（`CallSettle`）与 marker，**不进入** `RegisterActivities`：
  remote-only 包（如示例 `remote/`）没有 helper，混合包里也只注册本地实现。remote 实现由拥有它的
  worker 手写注册（示例 worker 里 `payments.settle` 就是这唯一一条手写路径）。
- 契约在登记时现算：I/O 类型不是 struct 或 `map[string]any`、或字段类型不在 wire 支持子集时返回错误。

## 启动自动登记（作者主路径）

生成器除了写 5 份 JSON，还在 Workflow 源码包里写 `zz_<pkg>_artifacts_gen.go`：

```go
//go:embed workflow.json
var tenutoWorkflowArtifacts embed.FS

func WorkflowDefinition() registration.Definition { return registration.Definition{FS: tenutoWorkflowArtifacts} }
```

只打包运行必需的 `workflow.json`（WLang 程序就在它的 `program` 字段里），不需要
源码目录、其余 JSON 或运行期反射。文件名与 `zz_<pkg>_contract_gen.go` 同一命名族，内容只依赖包名，
两次生成逐字节相同。

作者在 Worker 配置里按包引用定义，一个包一行，`Start` 自动上传/登记：

```go
workflowWorker, err := worker.New(c, worker.Options{
	Role:      worker.RoleWorkflow,
	Workflows: []registration.Definition{startupflow.WorkflowDefinition()},
})
if err != nil { return err }
workflowWorker.Start(ctx)      // 先登记定义，再开始 poll
if err := workflowWorker.WaitReady(ctx); err != nil { return err }
```

- 定义经 Worker 所用 Client（`client.Dial`）的连接登记到它的 Namespace；不需要额外的
  `registration.Register` / `Publish` 调用，SDK 不 import server。
- `Start` 在启动 poll 之前同步登记；登记失败不启动 poll，错误由 `WaitReady` 返回。
- Activity Worker 与 Workflow 定义的上线顺序不作要求：登记期查不到 Activity 路由时记 warning。
  Run 走到无路由的 Activity 时，Workflow Task 按 1s→60s 退避重试并显示为 Blocked，Worker 上线后自动继续。

登记走 `POST /runtime/workflow/artifacts` + `activate_if_new=true`，激活规则（Rule A）：

| 输入 | latest |
| --- | --- |
| 新内容首次出现 | 指向新内容 |
| 同 hash 重复启动且已有 latest（旧 binary 重启） | latest 不变；静态校验后幂等返回，不校验 Activity 路由 |
| 内容已登记但 latest 尚未建立 | 补激活该内容 |
| 坏配置 | 不变，登记失败 |

登记成功不依赖 Activity worker 在线，Activity 路由 TTL 过期不会阻止 workflow worker 启动和 poll。

显式发布与回滚仍走 `POST /runtime/workflow/register`（CLI `-publish`、`WORKFLOW_ARTIFACTS_PUBLISH=true`），
语义是「总是激活」。激活/登记只改 latest，不改变运行中 Run：`auto_upgrade` 仍在下一个 WFT 边界采用
latest，`pinned` 固定原内容。`WORKFLOW_ARTIFACTS` 目录模式保留，默认仍只登记。

## 常用命令

从仓库根目录：

```sh
# 作者主路径：统一生成 → 构建（定义 embed 进二进制）→ 启动后自动登记
make build-example
./bin/tenuto-startup-example

# 手动等价：先生成，再构建；普通 go build 不会执行 go:generate
go generate ./...
go build -o bin/tenuto-startup-example ./docs/examples/startup-workflow/worker/

# 手动生成（高级：只要 JSON/桥，不构建二进制）
go run ./compiler/cmd/tenuto-workflow-gen generate \
  ./docs/examples/versioned-workflow/v1 ./docs/examples/versioned-workflow/v2

# 多目录 + -watch：先对所有目录生成一次，再合并监听；任一手写源码变化后重新生成全部目录
go run ./compiler/cmd/tenuto-workflow-gen generate -watch \
  ./docs/examples/versioned-workflow/v1 ./docs/examples/versioned-workflow/v2

# 高级：显式发布 latest（跳过二进制自带定义，用于运维发布/回滚）
go run ./compiler/cmd/tenuto-workflow-gen generate \
  -watch -url http://localhost:7400 -namespace lifecycle-demo -publish \
  ./docs/examples/versioned-workflow/v2

# 高级：只登记 artifact、不切换 latest（回滚候选/旧 Worker 幂等登记）
go run ./compiler/cmd/tenuto-workflow-gen generate \
  -url http://localhost:7400 -namespace lifecycle-demo \
  ./docs/examples/versioned-workflow/v2
```

不带 `-url` 时默认不联网。`-url` 只提交 `workflow.json`（请求体是 `namespace` 与 `workflow`）。带 `-url` 且不带 `-publish` 时调用
`POST /runtime/workflow/artifacts`（只登记，不改变 active 版本）；带 `-publish` 时调用
`POST /runtime/workflow/register`（注册并激活 latest）。非 2xx 一律打印响应体并失败。

`-watch` 只比较**手写依赖源码**的内容哈希（入口文件、同包 DTO、被引用契约包），每次成功生成后按新的依赖更新监听目录。输出 JSON 与
`zz_*_contract_gen.go` 永不参与哈希，所以不会自循环。生成失败时打印诊断并保留上次成功的产物，
修好再保存会继续同步。

## 编排源码里直接调用生成的桥

生成器在把 workflow 源码交给 go2wlang 之前，会重写函数体里的桥调用：

```go
out, err := inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})          // → var out inventory.ReserveOutput; err := workflow.ExecuteActivity(...).Get(ctx, &out)
out, err = inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})           // → err = workflow.ExecuteActivity(...).Get(ctx, &out)
return inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})               // → 临时输出变量 + Get + return
if _, err := inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); ...    // → 连同 if 包进一个块，临时输出变量在块内声明
```

带 opts 的调用原样作为 `ExecuteActivity` 的可变参数透传（`opts...` 或内联
`workflow.ActivityOptions{...}`）。其他形态（嵌在任意表达式、多返回值重组、丢弃返回值）会报
`file:line`，不做猜测。

重写只作用于内存中的源码副本，不会改写用户文件；文件没导入 sdk/workflow 时会注入
`workflow "github.com/donutnomad/tenuto/sdk/workflow"` 别名。

### 输入载荷

runtime 的 `ExecuteActivity` 输入参数是 `any`。生成器在重写桥调用时，把**结构体字面量和结构体变量**
都按 `json` tag 展开成 `map[string]any`（嵌套结构体与切片递归处理，未导出字段跳过）。Workflow 入口
形参可以直接传给桥，静态类型由 Go 签名保证：

```go
func Flow(ctx workflow.Context, in ChargeInput) (ChargeOutput, error) {
    return CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
}
// → return workflow.ExecuteActivity(ctx, ChargeRemoteActivity, map[string]any{"order_id": in.OrderID}).Get(...)
```

同包与跨包（`activities.CallReserve(ctx, in)`）走同一套展开规则；`map[string]any` 仍然可用。

## 示例结构与验证

- `activities/`：本地 activity 实现（`@Activity` + `inventory` 队列同类示例），生成同包桥与
  `RegisterActivities` 批量注册 helper。
- `remote/`：跨包 remote 契约 `SettleRemote`（命名函数类型），生成桥 `CallSettle` + 标记函数
  `SettleRemoteActivity`（remote-only，无 `RegisterActivities`）；Workflow 内直接 `remote.CallSettle(ctx, in)`。
- `v1/`、`v2/`：同一 workflow 的两代源码。v1 记录 `GetVersion(3..4)`，v2 提高到 `GetVersion(3..6)`：
  新执行记录 6 走 `lifecycle.charge-v2` + `payments.settle`；v1 历史读回 4 保留旧 `lifecycle.charge`。
- `worker/`：从 `workflow.json` 的 `activities` 读契约元数据，注册 workflow worker 与 Activity worker 并上报契约。
- `../startup-workflow/`：作者最短路径示例（struct IO + 注解 + 一行 `Workflows`）。`worker/main_test.go`
  编译二进制、复制到没有源码/JSON 的目录后启动，验证仍能登记并激活定义（内存 engine + httptest）。

```sh
go test ./docs/examples/versioned-workflow/...
go test ./compiler/...
go test ./compiler/artifact/...
go test ./compiler/cmd/tenuto-workflow-gen/...
```

`example_test.go` 里的 `TestSourceVersionCompatibility` 用真实 sdkgen 产物跑解释器：先执行 v1
拿到历史，再把同一份历史交给 v2 Program 重放，最后验证 v2 新执行调度到正确 activity 与队列。
`TestGeneratedArtifactsAreStable` 重新生成并与仓库里的 JSON/桥文件逐字节比对。
