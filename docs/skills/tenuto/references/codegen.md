# 注解与代码生成

代码位置：`compiler/sdkgen/annotation.go`（注解）、`compiler/sdkgen/contracts.go`（契约与桥）、`compiler/cmd/tenuto-workflow-gen/main.go`（命令）。

## 注解语法

注解写在声明文档注释的 `//` 行里，形如 `@Kind(key=value,key="value")`：逗号分隔，值可带双引号也可不带。

```go
// @Workflow(id="lifecycle-demo",name="Lifecycle Demo",queue="lifecycle",policy=auto_upgrade,version=v2)
func Lifecycle(ctx workflow.Context, in Input) (Output, error)

// @Activity(type="lifecycle.charge",queue="lifecycle",version=v1)
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error)

// @Activity(type="payments.settle",queue="payments",version=v1,remote=true)
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)
```

### @Workflow

| key | 必填 | 含义 |
| --- | --- | --- |
| `id` | 是 | Workflow 定义 ID；启动 Run、Child、Continue-As-New、Schedule 都用它 |
| `name` | 否 | 展示名，缺省等于 `id` |
| `queue` | 否 | 默认 Activity Task Queue；`@Activity` 省略 `queue` 时使用它 |
| `policy` | 否 | `auto_upgrade`（缺省）或 `pinned`；其他值（包括 `auto`）报错 |
| `version` | 否 | 作者声明的发布标签，写入 `workflow.json` 并参与内容 hash |

Workflow 函数规则：

- 包级普通函数，第一个参数 `workflow.Context`；输入是一个 struct DTO（或其指针）、`map[string]any` 或省略，形参名任意（不必叫 `input`）；标量、切片、`any` 入参生成时报 `Workflow W 的输入类型 ... 不是 JSON object`；返回 `(Out, error)`、`error` 或只返回数据。
- 输出类型是 struct 或 `map[string]any`；Run 的 Output 存成 JSON object。
- 一个包目录只有一条 `@Workflow`；多个 Workflow 放在多个包。
- `sdk/workflow` 可以用命名别名导入（`wf "github.com/donutnomad/tenuto/sdk/workflow"`），生成器把别名改写成 Worker 绑定的包名 `workflow`；用别名时同一文件里不能再有名为 `workflow` 的标识符（生成期报 `file:line`）。点导入与空白导入被拒绝。

### @Activity

| key | 必填 | 含义 |
| --- | --- | --- |
| `type` | 是 | Activity Type：路由、注册、重放对照共用的身份 |
| `queue` | 否 | Activity Task Queue；省略时取所属 `@Workflow` 的 `queue` |
| `version` | 否 | 契约版本标签，Server 只做逐字相等对照 |
| `remote` | 否 | `true` 表示实现在别的 Worker；只能标在命名函数类型上 |

两种形态：

| 形态 | 写法 | 生成内容 |
| --- | --- | --- |
| 本地实现 | 注解标在包级函数上 | 桥 `CallX`、`activity.Caller` 与 `activity.Handler` 签名断言、进入 `RegisterActivities` |
| 远程契约 | `remote=true` 标在 `type X func(context.Context, In) (Out, error)` 上 | 桥 `CallX` 与标记函数 `<类型名>Activity`（如 `SettleRemoteActivity`）；实现由拥有它的 Worker 用 `activity.RegisterActivity` 登记 |

约束：

- 签名固定 `func(context.Context, In) (Out, error)`；`In` / `Out` 是 struct 或 `map[string]any`。
- 实现是包级函数，方法被拒绝；命名函数类型必须声明 `remote=true`。
- 一个声明只带一条 `@Activity`；同一批契约里 `type` 唯一、桥名唯一。

### 桥名

桥名从 `type` 派生，与 Go 函数名无关：按 `.` 切分 `type`，去掉末尾的版本段（`v1`、`V2`、`2`、`v1beta1`，至少保留一段），取剩下的最后一段，按 `_` `-` `/` 分词转成导出式驼峰，加前缀 `Call`。

| `type` | 桥 |
| --- | --- |
| `startup.say-hello` | `CallSayHello` |
| `payments.settle` | `CallSettle` |
| `payments.charge.v1` | `CallCharge` |
| `lifecycle.charge-v2` | `CallChargeV2` |

两个 `type` 派生出同一个桥名时生成失败（`生成的桥名 CallCharge 与 <file:line> 冲突`），例如 `payments.charge` 与 `refunds.charge`、`payments.charge` 与 `payments.charge.v2`；改其中一个 `type` 的末段（`payments.charge-v2`）。

## 运行生成

业务模块先登记 tool：`go get -tool github.com/donutnomad/tenuto/compiler/cmd/tenuto-workflow-gen@latest`（版本随 `require github.com/donutnomad/tenuto` 固定）。Workflow 包里写 `//go:generate go tool tenuto-workflow-gen generate .`，从模块根目录执行 `go generate ./...`；`go build` 不执行生成，产物与源码一起提交。

```bash
# 只生成，不联网；可以一次传多个包目录
go tool tenuto-workflow-gen generate ./path/to/pkg

# 保存后自动重新生成（监听手写源码的内容哈希；失败保留上次成功产物）
go tool tenuto-workflow-gen generate -watch ./path/to/pkg

# 生成后只登记，latest 不变（POST /runtime/workflow/artifacts）
go tool tenuto-workflow-gen generate \
  -url http://localhost:7400 -namespace orders ./path/to/pkg

# 生成后注册并激活 latest（POST /runtime/workflow/register），用于显式发布与回滚
go tool tenuto-workflow-gen generate \
  -url http://localhost:7400 -namespace orders -publish ./path/to/pkg
```

| 参数 | 作用 |
| --- | --- |
| `<pkgdir>...` | 包目录，位于某个 Go 模块内，至少含一条 `@Workflow` 或 `@Activity` |
| `-watch` | 监听入口文件、同包 DTO、被引用的契约包；每次成功生成后更新监听目录；生成进行中的保存在本次结束后再触发一次生成 |
| `-url` | 生成成功后提交 `workflow.json` 到该 Server（只发 `namespace` 与 `workflow`）；省略时不联网 |
| `-namespace` | 提交使用的 Namespace，缺省 `default` |
| `-publish` | 配合 `-url`：注册并激活 latest；省略时只登记 |

Server 开启 `WORKFLOW_HTTP_AUTH_TOKEN` 时用环境变量 `TENUTO_AUTH_TOKEN` 传 token。

生成完打印 `output: <dir> (content version <version>)`，`<version>` 与服务端登记后报告的版本相同。

在 Tenuto 仓库内：

```bash
go generate ./...     # 唯一入口：server/cmd/tenuto-server/main.go 的 //go:generate
make build-example    # 先生成再构建示例二进制
```

仓库内新增 Workflow 包时，把包目录追加到 `server/cmd/tenuto-server/main.go` 里 `tenuto-workflow-gen generate` 那条指令的参数列表。Workflow 包引用的契约包（只含 `@Activity` 的包）在生成该 Workflow 包时一并写出桥文件；契约包与 Workflow 包位于同一个 Go 模块。

`go build` 不执行生成：改了 Workflow、Activity 签名、DTO 或注解之后先生成再构建，否则二进制里 embed 的是上一次的定义。

## 生成产物

| 文件 | 内容 |
| --- | --- |
| `zz_<pkg>_contract_gen.go` | 每个契约包的 `CallX` 桥、签名断言；有本地实现时含 `RegisterActivities(registry *activity.Registry) error` |
| `zz_<pkg>_artifacts_gen.go` | embed `workflow.json`，导出 `WorkflowDefinition() registration.Definition` |
| `workflow.json` | Runtime 接受的 WorkflowConfig：`program`（WLang 程序）、`activities`（符号 → 类型、队列、契约指纹）、`policy`、`version` |
| `wlang.json` | `workflow.json` 里 `program` 的单独一份，便于审阅 diff |
| `activities.json` | Activity 清单：`type`、`version`、输入输出 wire 指纹、`queue`、`symbol` |

只含 `@Activity` 的包目录只生成 `zz_<pkg>_contract_gen.go`。内容相同时不重写文件；同一份源码在不同 checkout 得到相同的内容 hash。全部产物提交进版本库，全部由生成器维护。

生成的桥（来源：`docs/examples/startup-workflow/zz_startupflow_contract_gen.go`）：

```go
func CallSayHello(ctx workflow.Context, in SayHelloInput, opts ...workflow.ActivityOptions) (SayHelloOutput, error) {
    var out SayHelloOutput
    err := workflow.ExecuteActivity(ctx, SayHello, in, opts...).Get(ctx, &out)
    return out, err
}

var _ activity.Caller[SayHelloInput, SayHelloOutput] = CallSayHello
var _ activity.Handler[SayHelloInput, SayHelloOutput] = SayHello

func RegisterActivities(registry *activity.Registry) error {
    if err := activity.RegisterActivity(registry, "startup.say-hello", "v1", SayHello); err != nil {
        return err
    }
    return nil
}
```

## 桥调用规则

生成器在翻译前把 Workflow 函数体里的桥调用重写成 `workflow.ExecuteActivity(...).Get(...)`。支持的形态：

```go
out, err := inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
out, err = inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
return inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if _, err := inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); err != nil { ... }
paid, err := CallCharge(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
```

- 输入可以是 struct 字面量、struct 变量、Workflow 入口形参或 `map[string]any`。
- 同包写 `CallX(ctx, in, opts)`，跨包写 `pkg.CallX(ctx, in, opts)`，Workflow 文件 import 契约包。`opts` 是 `workflow.ActivityOptions`，至少设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`，否则生成器报错。
- `out` 可以是 `_`，`err` 必须接收。

## 契约与 wire 指纹

`activities.json` 的 `input` / `output` 是 `wire:<hash>`，由 JSON 字段名、字段类型、`omitempty`、嵌套结构计算，与 Go 包名和类型名无关。Worker 侧 `activity.ContractOf[In, Out](type, version)` 用同一算法。两侧 DTO 可以各自定义，wire 结构相同则指纹相同。增加一个字段（即使 `omitempty`）指纹就变。

wire 支持的类型：字符串、布尔、定宽整数与 `int`、浮点、指针、slice、定长数组、字符串 key 的 map、struct、空接口，以及由它们组成的具名类型与递归类型。

返回错误的类型：`uint`、`[]byte`、匿名嵌入字段、实现 `json.Marshaler` / `encoding.TextMarshaler` 的类型（包括 `time.Time`）、`omitempty` 之外的 json tag 选项、非字符串 key 的 map、非空接口。时间用 RFC 3339 字符串或 Unix 整数字段传递。

## 生成器错误

全部带 `file:line`，失败时不写任何 JSON 产物。

| 错误原文（节选） | 处理 |
| --- | --- |
| `注解必须写成 // 行注释` | 把 `/* */` 里的注解改成 `//` |
| `@Workflow 未知 key "..."` / `@Activity 未知 key "..."` / `key "..." 重复` | 只用上表的 key |
| `@Workflow 缺少 id` / `@Activity 缺少 type` | 补必填项 |
| `@Workflow policy="..." 未知，支持 auto_upgrade、pinned` | 改成两者之一 |
| `@Workflow 注解只能有一个，实际 N 个` | 一个包目录一个 Workflow，拆包 |
| `@Workflow 函数不能是方法` / `@Activity 实现必须是普通函数，不能是方法` | 改成包级函数 |
| `remote=true 只能标注命名函数类型` / `命名函数类型 X 必须声明 remote=true` | 按两种形态之一书写 |
| `activity 契约必须有两个参数 (ctx, in)` / `第一个参数必须是 context.Context` / `必须返回 (out, error)` | 改成固定签名 |
| `@Activity type="..." 的输入类型 X 不是 JSON object；Activity 的输入输出必须是 struct 或 map[string]any` | 包进 struct 字段 |
| `Workflow W 的输入类型 X 不是 JSON object；Workflow 的输入必须是 struct（或其指针）或 map[string]any` | 包进 struct 字段 |
| `@Activity type="..." 重复声明` / `生成的桥名 ... 与 ... 冲突` | 改 `type` |
| `X 直接调用了 @Activity 实现 ...；activity 只能通过生成的桥 CallX 调度` | 改成 `CallX(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})` |
| `CallX(ctx, in) 没有设置 StartToCloseTimeout 或 ScheduleToCloseTimeout` | 给调用传 `workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}`（或设置 `ScheduleToCloseTimeout`） |
| `X 只能用作 out, err := ...、out, err = ... 或 return ... 的完整右值` | 先赋给变量，再在表达式里用变量 |
| `X 的返回值被丢弃` / `X 的输出与错误都被丢弃；必须接收 err` | 至少接收 `err` |
| `activity X 的 marker Y 在翻译产物里是变量引用` | Workflow 函数体内有同名局部变量、参数或函数值遮蔽了 Activity 符号，改名；或包没通过类型检查，先让它编译 |
| `workflow 里不能用 ...；用 ...` | 见 `go-subset.md` |
| `本包函数 X 不能作为值使用` / `方法 M 不能作为值使用` | 写闭包调用它 |
| `<pkg>.<Func> is not supported` / `call of <pkg>.<Func> is not supported` / `type strings.Builder is not supported` | 换用 `go-subset.md` 清单里的标准库函数，或放进 Activity |
| `generic function` / `variadic function` / `pointer receiver` / `package-level variable ... is not supported` | 见 `go-subset.md` 的「同包函数」 |
| `workflow.SetUpdateHandler handler must be an inline function` 等 | 见 `messaging.md` |
| `workflow.IsApplicationError 只支持完整 if 条件` | 见 `errors.md` |
| `... (chan int): channels cannot cross workflow boundaries` / `embedded field X is outside json/plain-v1` 等 | 见 `go-subset.md` 的「数据类型」 |
| `常量 X 由 iota 或省略的表达式定义` | 给常量写出显式的值 |
| `<dir>: 既没有 @Workflow 也没有 @Activity 注解` | 目录传错，或注解语法没被识别 |
