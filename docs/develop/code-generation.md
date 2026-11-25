---
title: 注解与代码生成
description: "@Workflow / @Activity 注解、tenuto-workflow-gen 命令、生成产物与调用桥规则"
status: Stable
---

# 注解与代码生成

<FeatureStatus status="Stable" />

`tenuto-workflow-gen` 读取包目录里的 `@Workflow` 与 `@Activity` 注解，写出 typed 调用桥、批量注册函数和可登记的 JSON 产物。Workflow 定义随二进制发布，Worker 启动时自动登记。

## 注解语法

```go
// @Workflow(type="lifecycle-demo",name="Lifecycle Demo",policy=auto_upgrade,version=v2)
func Lifecycle(ctx workflow.Context, in Input) (Output, error)

// @Activity(type="lifecycle.charge",queue="lifecycle",version=v1)
func Charge(ctx context.Context, in ChargeInput) (ChargeOutput, error)

// @RemoteActivity(type="payments.settle",queue="payments",version=v1)
type SettleRemote func(context.Context, SettleInput) (SettleOutput, error)
```

- 注解写在声明文档注释的 `//` 行内，形如 `@Kind(key=value,key="value")`，逗号分隔，值可带双引号。
- `@Workflow` 的 key：`type`（必填）、`name`、`policy`、`version`。
- `@Activity` 与 `@RemoteActivity` 的 key：`type`（必填）、`queue`、`version`。
- 未知 key、重复 key、缺少必填项、未闭合的引号、写在 `/* */` 里的注解都报 `file:line` 错误。

### @Activity 与 @RemoteActivity

| 形态 | 写法 | 生成内容 |
| --- | --- | --- |
| 本地实现 | `@Activity` 标在普通函数上 | 桥 `CallX`、签名断言、进入 `RegisterActivities` |
| 远程契约 | `@RemoteActivity` 标在命名函数类型上 | 桥 `CallX` 与标记函数 `<类型名>Activity`（如 `SettleRemoteActivity`），实现由拥有该 Activity 的 Worker 注册 |

约束（均由生成器检查）：

- 签名固定为 `func(context.Context, In) (Out, error)`：两个参数，第一个是 `context.Context`；两个返回值，第二个是 `error`，第一个是数据类型。
- 实现是包级普通函数，方法上的注解被拒绝。
- `@RemoteActivity` 只能标在 `type X func(...)` 上；命名函数类型上写 `@Activity` 报错。
- 一个声明只带一条 `@Activity` 或 `@RemoteActivity`；同一批契约里 `type` 唯一，生成的桥名唯一。
- `@Activity` 写了 `queue` 时，`workflow.json` 声明这个队列，生成的 `RegisterActivities` 用 `activity.RegisterActivityOnQueue` 一起登记，Worker 据此决定拉取的队列。省略时两边都不带队列：Server 按 Worker 的登记选路，Worker 在 `ActivityTaskQueues`（不写时是 `default`）上提供它。

### 桥名的派生

桥名由 Activity 的 `type` 决定，与 Go 函数名无关：

1. 按 `.` 切分 `type`，去掉末尾的版本段（`v1`、`V2`、`2`、`v1beta1` 这类形态），至少保留一段。
2. 取剩下的最后一段，以 `.`、`_`、`-`、`/` 为分隔把每个词首字母大写后拼接，前面加 `Call`。

| `type` | 桥名 |
| --- | --- |
| `startup.say-hello` | `CallSayHello` |
| `payments.charge` | `CallCharge` |
| `payments.charge.v1` | `CallCharge`（末尾版本段不参与命名） |
| `order.charge-v2` | `CallChargeV2`（`-v2` 属于最后一段本身） |

- 同一批契约里两个 `type` 派生出同一个桥名时生成失败，报 `生成的桥名 CallCharge 与 <另一处 file:line> 冲突`。例如 `payments.charge` 与 `refunds.charge`，或 `payments.charge` 与 `payments.charge.v2`。改其中一个 `type` 的最后一段（如 `refunds.refund-charge`、`payments.charge-v2`）。
- 最后一段含其他字符、或派生结果不以字母开头时报 `@Activity type="..." 无法派生 Go 标识符`。

## 运行生成

### 在业务模块中

```bash
go get github.com/donutnomad/tenuto@latest
go get -tool github.com/donutnomad/tenuto/compiler/cmd/tenuto-workflow-gen@latest
```

在 Workflow 包里写一条指令，从模块根目录执行 `go generate ./...`：

```go
//go:generate go tool tenuto-workflow-gen generate .
```

生成器版本随 `require github.com/donutnomad/tenuto` 固定，与 SDK 一起升级。下文的命令在业务模块里把 `go run ./compiler/cmd/tenuto-workflow-gen` 换成 `go tool tenuto-workflow-gen`。完整流程见[接入已有项目](/quickstart/integrate)。

### 在 Tenuto 仓库中

仓库只有一处 `//go:generate` 入口，位于 `server/cmd/tenuto-server/main.go`。从仓库根目录执行：

```bash
# 统一生成：gogen + 全部 Workflow 包
go generate ./...

# 生成后构建示例二进制（Makefile 目标，内部先执行统一生成）
make build-example
```

`go build` 不执行 `go:generate`，构建流程需要先运行生成。

直接调用生成器：

```bash
# 只生成，不联网
go run ./compiler/cmd/tenuto-workflow-gen generate ./docs/examples/startup-workflow

# 监听手写源码的内容哈希，保存后重新生成
go run ./compiler/cmd/tenuto-workflow-gen generate -watch ./path/to/pkg

# 生成后登记到 Server，latest 保持不变
go run ./compiler/cmd/tenuto-workflow-gen generate \
  -url http://localhost:7400 -namespace orders ./path/to/pkg

# 生成后注册并激活 latest
go run ./compiler/cmd/tenuto-workflow-gen generate \
  -url http://localhost:7400 -namespace orders -publish ./path/to/pkg
```

| 参数 | 作用 |
| --- | --- |
| `<pkgdir>...` | 一个或多个包目录，每个目录至少包含一条 `@Workflow` 或 `@Activity` |
| `-watch` | 监听入口文件、同包 DTO 和被引用契约包；每次成功生成后按新的依赖更新监听目录；生成进行中的保存在本次结束后再触发一次生成；生成失败时保留上次成功产物 |
| `-url` | 生成成功后把 `workflow.json` 提交到该 Server（请求体只有 `namespace` 与 `workflow`）；省略时不联网 |
| `-namespace` | 提交使用的 Namespace，默认 `default` |
| `-publish` | 配合 `-url`：调用 `POST /runtime/workflow/register`（注册并激活 latest）；省略时调用 `POST /runtime/workflow/artifacts`（只登记） |

Server 开启 `WORKFLOW_HTTP_AUTH_TOKEN` 时，用环境变量 `TENUTO_AUTH_TOKEN` 传 token（不提供命令行参数，参数会出现在进程列表里）：`TENUTO_AUTH_TOKEN=... tenuto-workflow-gen generate -url ...`。

每个包生成完打印 `output: <dir> (content version <version>)`。`<version>` 是 `workflow.json` 规范化内容的版本，与服务端登记后在 `list-configs`、Console 里显示的版本相同，换行风格不影响它。

## 生成产物

| 文件 | 内容 |
| --- | --- |
| `zz_<pkg>_contract_gen.go` | 每个契约包的 `CallX` 桥、签名断言；有本地实现时包含 `RegisterActivities` |
| `zz_<pkg>_artifacts_gen.go` | 把 `workflow.json` embed 进二进制，导出 `WorkflowDefinition()` |
| `workflow.json` | Runtime 接受的 WorkflowConfig：`program`（WLang 程序）、`activities`（程序里的 Activity 符号 → `type`、`task_queue`、`version`、输入输出 wire 指纹）、`policy`、`version` |

只含 `@Activity` 的包目录只生成 `zz_<pkg>_contract_gen.go`。产物内容相同时不重写文件；产物不含源码目录的绝对路径，同一份源码在不同 checkout 得到相同的内容 hash。

生成的桥形如：

```go
func CallSayHello(ctx workflow.Context, in SayHelloInput, opts ...workflow.ActivityOptions) (SayHelloOutput, error) {
    var out SayHelloOutput
    err := workflow.ExecuteActivity(ctx, SayHello, in, opts...).Get(ctx, &out)
    return out, err
}

var _ activity.Caller[SayHelloInput, SayHelloOutput] = CallSayHello
var _ activity.Handler[SayHelloInput, SayHelloOutput] = SayHello

func RegisterActivities(registry *activity.Registry) error
```

`activity.Handler` 与 `activity.Caller` 断言让实现签名和桥签名的漂移在 Go 编译期暴露。

## 桥调用规则

生成器在翻译前把 Workflow 函数体及其调用的同包函数里的桥调用重写成 `workflow.ExecuteActivity(...).Get(...)`。支持的形态：

```go
out, err := inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
out, err = inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
return inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
if _, err := inventory.CallReserve(ctx, in, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}); err != nil { ... }
```

- 带选项的调用把 `workflow.ActivityOptions{...}` 作为第三个参数传入。
- 输入可以是结构体字面量、结构体变量、Workflow 入口形参或 `map[string]any`；结构体按 `json` tag 展开。
- 同包调用写 `CallX(ctx, in, opts)`，跨包调用写 `pkg.CallX(ctx, in, opts)`，Workflow 文件需要 import 契约包。`opts` 是 `workflow.ActivityOptions`，至少设置 `StartToCloseTimeout` 或 `ScheduleToCloseTimeout`：两者都缺少时生成器报出调用点 `file:line`。
- 嵌在其他表达式里的桥调用、丢弃全部返回值的桥调用、直接调用 `@Activity` 实现函数都报 `file:line` 错误，失败时不写任何 JSON 产物。
- `sdk/workflow` 可以用命名别名导入（`wf "github.com/donutnomad/tenuto/sdk/workflow"`），生成器把别名改写成 Worker 绑定的包名 `workflow`；这时同一文件里不能再有名为 `workflow` 的标识符，否则生成期报 `file:line`。点导入（`.`）和空白导入（`_`）在桥重写时被拒绝。

## Workflow 代码的生成期诊断

生成器把 Workflow 函数和它调用的同包函数（见[同包函数](/develop/go-subset#同包函数)）一起检查，下列写法报 `file:line` 并给出替代写法，失败时不写任何产物：

| 写法 | 诊断内容 |
| --- | --- |
| `time.Now`、`math/rand`、`os`、`net`、`sync`、`errors`、`github.com/google/uuid` | 对应的 `workflow.*` 替代或放进 Activity |
| [可调用的标准库函数](/develop/go-subset#标准库函数)以外的标准库函数，如 `strings.Title`、`sort.Strings` | `<pkg>.<Func> is not supported`，并列出该包可调用的函数 |
| 标准库类型的值与方法，如 `strings.Builder`、`time.Duration` 的 `String()` | `type ... is not supported` / `method call ... is not supported` |
| `sdk/workflow` 以外的包的函数与方法 | `call of <pkg>.<Func> is not supported: the package is not bound by the host` |
| `fmt` 的非常量格式串、`%T`、`%q`、动词与操作数不匹配、个数不一致、带方法的类型或复合类型作操作数 | 指出格式串或操作数 |
| 泛型、可变参数的同包函数，指针接收者方法，包级变量 | 指向函数声明或变量使用处 |
| 同包函数或方法当成值使用 | `本包函数 X 不能作为值使用` |

## 启动自动登记

Worker 配置引用生成的定义，`Start` 在开始 poll 之前同步登记：

```go
w, err := worker.New(c, worker.Options{
    Workflows:  []registration.Definition{startupflow.WorkflowDefinition()},
    Activities: registry, // startupflow.RegisterActivities 登记的 Activity
})
if err != nil {
    return err
}
w.Start(ctx)
if err := w.WaitReady(ctx); err != nil {
    return err
}
```

`c` 是 `client.Dial` 创建的 Client，定义登记到它的 Namespace。

| 登记输入 | latest |
| --- | --- |
| 新内容首次出现 | 指向新内容 |
| 同 hash 重复启动且已有 latest | 保持不变 |
| 内容已登记、latest 尚未建立 | 补激活该内容 |
| 配置非法 | 保持不变，登记失败，`WaitReady` 返回错误 |

登记只改变 latest。运行中的 Run 按自身 `policy` 决定是否采用新内容，见 [Workflow 版本管理](/develop/versioning)。

## 契约对照

`workflow.json` 的 `activities` 里的 `input` / `output` 是 wire 指纹（`wire:<hash>`），由 JSON 字段名、类型和可选性计算，与 Go 包名和类型名无关。Worker 侧 `activity.ContractOf[In, Out](type, version)` 使用同一算法，Worker 启动时随路由上报自己的契约。Server 在调度 Activity 时对照两侧契约：Workflow Task 提交 Activity 调度命令后，Server 用该 Run 的 Program 声明的 `version`、`input`、`output` 选择契约一致的 Worker；没有一致的候选时命令不派发，Run 记录 `ConfigError`（`activity "..." contract mismatch on worker "..." ...`）并暂停，History 保持不变，见 [Workflow 版本管理](/develop/versioning)。Worker 注册和定义登记阶段不做这项对照。

wire 指纹支持的类型：字符串、布尔、定宽整数与 `int`、浮点、指针、slice、定长数组、字符串 key 的 map、结构体、空接口，以及由它们组成的具名类型与递归类型（递归位置记为指向外层类型的回指，与类型名无关）。以下类型返回错误：`uint`、`[]byte`、匿名嵌入字段、自定义 `json.Marshaler` / `encoding.TextMarshaler`、`omitempty` 之外的 json tag 选项、非字符串 key 的 map。

完整示例见仓库 `docs/examples/startup-workflow`、`docs/examples/saga-workflow`、`docs/examples/versioned-workflow`、`docs/examples/multiprocess`。
