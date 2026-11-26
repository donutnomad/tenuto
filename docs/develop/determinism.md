---
title: 确定性约束
description: 编写可安全重放的 Tenuto Workflow
status: Stable
---

# 确定性约束

<FeatureStatus status="Stable" />

同一份 Workflow Program 和 Event History 必须产生相同的命令序列。这条规则保证 Worker 重启、缓存逐出和故障恢复后的执行结果一致。Worker 在重放时按 Workflow Task 逐批比对：每个已完成的 Workflow Task 里代码产生的命令与该 Workflow Task 写入 History 的命令事件逐条一致，命令挪到另一个 Workflow Task（例如把并行的两个 Activity 改成先后执行）时即使总顺序不变也不一致。不一致时报告非确定性错误并把 Run 标记为 Blocked。

## 确定性值的来源

| 需求 | Workflow 写法 | 持久化方式 |
| --- | --- | --- |
| 当前时间 | `workflow.Now(ctx)` | 取自本次 Workflow Task 的 `WorkflowTaskStarted` 时间戳 |
| 等待一段时间 | `workflow.Sleep(ctx, d)` | Timer Event |
| UUID | `workflow.RandomUUID(ctx)` | Marker |
| 随机整数 | `workflow.RandomInt(ctx, max)` | Marker |
| 一次性求值 | `workflow.SideEffect(ctx, f)` | Marker |
| 可更新的值 | `workflow.MutableSideEffect(ctx, id, f, equals)` | Marker |
| 代码版本分支 | `workflow.GetVersion(ctx, changeID, min, max)` | Marker |
| 外部数据、系统时间、加密随机数 | Activity | Activity 结果 Event |

```go
now := workflow.Now(ctx)
id := workflow.RandomUUID(ctx)
shard := workflow.RandomInt(ctx, 16)
```

各 API 的语义见 [Side Effect 与确定性值](/develop/side-effects) 和 [Timer 与 Workflow Sleep](/develop/timers)。

## 稳定的 ID

Activity（没有指定 `ActivityOptions.ActivityID` 时）、Timer、Child Workflow、随机值和 Side Effect 的 ID 由「协程路径 + 该协程内同类原语的序号」生成：

```text
#a1      主协程的第 1 个 Activity
#t2      主协程的第 2 个 Timer
/0#a1    第 1 条 workflow.Go 协程里的第 1 个 Activity
/1/0#c1  第 2 条协程里嵌套的第 1 条协程的第 1 个 Child Workflow
```

协程路径按 `workflow.Go` / `WaitGroup.Go` 的调用顺序编号。由此得到三条规则：

- 同一协程内，同类原语的调用顺序在所有重放中保持一致。
- 协程的启动顺序保持一致。循环启动协程时，循环的数据源顺序保持一致。
- 不同种类的原语各自编号，条件分支里多调度一个 Timer 不影响 Activity 的编号。
- 指定了 `ActivityID` 的 Activity 同样占用序号，给某个调用加上或去掉 ID 只改变它自己的 ID。

## 需要版本保护的修改

对已经有运行中 Run 的 Workflow，下列修改改变命令序列，需要 `workflow.GetVersion` 保护或使用 `pinned` 策略：

- 新增、删除或重排 Activity、Timer、Child Workflow，或把它们挪到另一个 Workflow Task（并行改成先后执行、在调用前加等待，或反过来）。
- 修改协程启动顺序或数量。
- 修改 Selector case 的添加顺序。
- 新增、删除或重排 `SideEffect`、`RandomUUID`、`RandomInt` 调用。
- 修改 Activity Type、`ActivityID`、子 Workflow 的 Workflow Type 或 Child ID。

做法见 [Workflow 版本管理](/develop/versioning)。

## Map 与迭代顺序

Go 不规定 map 的遍历顺序，每次运行都可能不同。Workflow 里的 `for k, v := range m` 按 key 升序遍历（字符串按字节序、数字按大小），每次执行与重放顺序相同；这是 Go 允许的顺序之一，所以翻译前后行为一致。遍历中删除的、尚未访问的 key 不会再出现。

按 key 升序是确定的，但通常不是业务想要的顺序。决定 Activity 调度顺序、协程启动顺序和 Selector case 顺序的数据使用 slice，由 Workflow 输入或 Activity 结果按固定顺序给出。

```go
// items 是 []Item，顺序来自 Workflow 输入
failed := false
wg := workflow.NewWaitGroup(ctx)
for _, item := range items {
    current := item
    wg.Go(ctx, func(ctx workflow.Context) {
        _, err := CallProcess(ctx, ProcessInput{ID: current.ID}, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second})
        if err != nil {
            failed = true
        }
    })
}
wg.Wait(ctx)
```

需要排序时在 Activity 中完成，把有序结果返回给 Workflow。

## 并发原语

Workflow 并发只使用 `workflow.Go`、`WaitGroup`、`Channel`、`Selector`。原生 `go` 语句、`chan`、`select`、`sync`，以及 `time.Now`、`math/rand`、`crypto/rand`、`uuid`、`os`、`net` 这些每次执行结果不同的来源，在生成期报错并给出替代写法。见 [Workflow 可用的 Go 语法](/develop/go-subset)。

## 条件闭包与 Handler

- `workflow.Await` 的条件闭包只读取 Workflow 内存变量。
- Query Handler 只读取状态；它产生的命令被丢弃。
- Update Handler 属于 Workflow 代码，遵守本页全部规则。

## 放进 Activity 的工作

- 数据库读写。
- HTTP、gRPC 和 MQ。
- 文件系统与环境变量读取。
- 系统时间和加密随机数。
- [可调用的标准库函数](/develop/go-subset#标准库函数)以外的计算，例如排序、时间解析、正则表达式、编码解码。
- 依赖进程本地状态的业务判断。

Workflow 代码保留确定性的编排状态、分支和等待逻辑。构造大字符串、大数组的计算也放进 Activity：解释器对单个字符串（16 MiB）、数组与 map 的大小设有上限，超出时 Run 以 `E_BUDGET` 失败，见[执行预算](/develop/go-subset#执行预算)。

## 非确定性错误的表现

重放失配时，Workflow Task 失败并按退避重试，Run 显示为 Blocked 并保存原因。修复方式是部署兼容的代码（恢复旧命令序列或补上 `GetVersion` 分支），Run 在下一次 Workflow Task 自动继续。排查入口见 [Visibility 与 Search Attributes](/concepts/visibility)。
