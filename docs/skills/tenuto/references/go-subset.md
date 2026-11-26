# Workflow 可用的 Go 语法

只约束带 `@Workflow` 注解的函数体（含其中的闭包与 Handler）。Activity 是原生 Go，没有这些限制。

两层边界：生成器（`compiler/sdkgen/lower.go` 的诊断与改写 + 翻译器 go2wlang）接受哪些语法；解释器在运行期能解析哪些符号。第一层在生成期报 `file:line`，第二层在运行期让 Workflow Task 失败。

## 运行期可用的符号

- `sdk/workflow` 包的函数与类型。
- 本 Program 引用的 Activity 符号（`@Activity` 实现函数、remote 标记函数）。
- Workflow 文件及契约包里的 struct 类型。
- `time.Time` 值（来自 `workflow.Now(ctx)`）的方法：`Add(d)`、`Sub(t)`、`Before(t)`、`After(t)`、`Equal(t)`、`Unix()`、`UnixMilli()`、`Format(layout)`（按 UTC 格式化）、`String()`。其他 `time.Time` 方法与 `time.Duration` 的方法（`d.Seconds()` 等，`time.Duration` 在 Workflow 里是 int64 纳秒）在生成期报 `file:line`。

生成期折叠成字面量的写法：

- `time` 常量：`time.Nanosecond` … `time.Hour`，布局常量 `time.RFC3339`、`time.RFC3339Nano`、`time.DateTime`、`time.DateOnly`、`time.TimeOnly`、`time.Kitchen` 等。
- `time.Duration` 类型与整数运算：`30 * time.Second`、`time.Duration(n) * time.Minute`。`time.Duration` 在 WLang 里是 `int64` 纳秒。
- 常量：包级与函数内的 `const`（含 `iota`、同包其他文件的常量）、常量运算，按 Go 推定的类型折叠（`var x int32 = 5` 得到 `int32`）。
- `workflow.DefaultVersion`。

```go
const maxRounds = 3

deadline := workflow.Now(ctx).Add(2 * time.Hour)
for round := 1; round <= maxRounds; round++ {
    if err := workflow.Sleep(ctx, time.Duration(round)*10*time.Second); err != nil {
        return err
    }
    if workflow.Now(ctx).After(deadline) {
        break
    }
}
```

## 标准库函数

可调用的标准库函数（结果与原生 Go 相同，wlang 的差分测试逐个验证）：

| 包 | 函数 |
| --- | --- |
| `strings` | `Contains` `Count` `EqualFold` `Fields` `HasPrefix` `HasSuffix` `Index` `Join` `LastIndex` `Repeat` `Replace` `ReplaceAll` `Split` `ToLower` `ToUpper` `Trim` `TrimLeft` `TrimPrefix` `TrimRight` `TrimSpace` `TrimSuffix` |
| `strconv` | `Atoi` `FormatBool` `FormatFloat` `FormatInt` `Itoa` `ParseBool` `ParseFloat` `ParseInt` |
| `fmt` | `Sprintf` `Sprint` `Sprintln` `Errorf` |
| `math` | `Abs` `Ceil` `Floor` `Inf` `IsInf` `IsNaN` `Max` `Min` `Mod` `NaN` `Round` `Sqrt` `Trunc`；常量 `math.MaxInt64`、`math.Pi` 等生成期折叠 |

- 任意 import 别名都可以（`import str "strings"`）。多返回值与 Go 相同，`strconv.Atoi("x")` 的错误文本是 `strconv.Atoi: parsing "x": invalid syntax`；`fmt.Errorf("...: %w", err)` 可作为 Workflow 的失败返回。
- `fmt` 规则：格式串是常量；动词 `%v %d %b %o %x %X %s %t %e %E %f %F %g %G`，`Errorf` 另有 `%w`；标志 `+ - 空格 0`，`#` 只配 `%x %X %o %b`；宽度精度写数字。操作数只能是布尔、整数、浮点、字符串（含无方法的具名类型）与 `error`。`%T`、`%q`、`%*d`、`%[n]d`、动词与操作数不匹配、个数不一致、带方法的类型（`time.Duration`、实现 `String()` 的类型）、struct、slice、map、`any` 都在生成期报错。
- 清单外：`strconv.Quote` 与 `%q`（转义依赖 Go 版本的 Unicode 表）、`math.Pow` 等非精确舍入函数（arm64 上可能编译成 FMA），以及 `sort`、`errors`、`bytes`、`strings.Builder`、`strings.Title` 等，全部生成期报 `file:line` 并列出可调用的函数。
- 行为由构建 Worker 的 Go 工具链决定。`ToUpper`/`ToLower`/`EqualFold` 依赖 Unicode 表：只有新 Unicode 版本新增的大小写字符会在升级 Go 后变化；分支依赖这类数据时按版本控制处理 Go 升级。

## 同包函数

Workflow 函数可以调用同包普通函数和值接收者方法（任意文件），被调用的函数再调用的也一起翻译进程序。

- 支持：多返回值、命名返回值、`defer`、递归与互相调用（调用深度上限 128）、`workflow.Context` 参数（在函数里调度 Activity、timer、子 Workflow，调用桥 `CallX`）、标准库清单、包级常量。
- 结构体参数是副本（值语义），slice / map 共享。函数只看见自己的参数、局部变量、常量和其他同包函数。
- 生成期拒绝（`file:line`）：泛型函数、可变参数函数、指针接收者方法、提升方法、经本包接口调用的方法、读写包级变量、把同包函数或方法当值（`f := helper`，改写成闭包；`workflow.ExecuteActivity` 的 Activity 参数除外）。
- 同包函数的函数体在产物里：只改它也改变 Workflow 内容版本。
- 生成器对这些函数做与 Workflow 函数相同的检查和改写；直接调用 `@Activity` 实现仍报错。

## 生成期拒绝的包与调用

| 写法 | 报错给出的替代 |
| --- | --- |
| `time.Now` | `workflow.Now(ctx)` |
| `time.Since(t)` | `workflow.Now(ctx).Sub(t)` |
| `time.Until(t)` | `t.Sub(workflow.Now(ctx))` |
| `time.Sleep`、`time.After`、`time.NewTimer` | `workflow.Sleep(ctx, d)` |
| `time.Tick`、`time.NewTicker` | 循环里的 `workflow.Sleep(ctx, d)` |
| `time.AfterFunc` | `workflow.Go` 加 `workflow.Sleep(ctx, d)` |
| `time` 包的其他函数（`time.Parse`、`time.Unix`、`time.Date`） | 放进 Activity |
| `math/rand`、`math/rand/v2`、`crypto/rand` | `workflow.RandomInt(ctx, max)`、`workflow.RandomUUID(ctx)`、`workflow.SideEffect` |
| `github.com/google/uuid` | `workflow.RandomUUID(ctx)` |
| `sync.WaitGroup` | `workflow.NewWaitGroup(ctx)` |
| `sync` 其他、`sync/atomic` | 共享变量直接读写；等待用 `workflow.NewWaitGroup(ctx)`、`workflow.Await(ctx, cond)` |
| `os`、`os/*`、`net`、`net/*` | 放进 Activity，或用 `workflow.SideEffect` 记录一次结果 |
| `go` 语句 | `workflow.Go(ctx, func(ctx workflow.Context) { ... })` |
| `select` | `workflow.NewSelector(ctx)` |
| `chan`、`ch <- v`、`<-ch` | `workflow.NewChannel(ctx)` / `NewBufferedChannel(ctx, n)`、`Send`、`Receive` |
| `errors` 包 | `fmt.Errorf`、`workflow.NewApplicationError`；判断用 `workflow.IsApplicationError`、`workflow.IsCanceledError` |
| 清单外的标准库函数与类型、其他包的函数与方法 | Activity |
| 同包函数或方法当值 | 闭包 |
| `int8(v)`、`int32(v)`、`uint*(v)`、`float32(v)`、`float64(v)`、`string(v)`、`byte(v)`、`rune(v)`、`int(f)`（`f` 为浮点）、`[]byte(s)` | 只有不改变数值表示的转换：`int`、`int64` 互转，`time.Duration(n)`（`n` 为整数），具名字符串类型与 `string`；其余放进 Activity |

## 支持的语句

翻译产物与 Go 源码行为一致；做不到一致的写法在生成期报错。

- 变量：`var x = expr`、`var x T`、`x := expr`、`x = expr`、`a, b := call()`、`a, b = call()`（更新已有变量）、`_ = expr`。
- 多值赋值：`a, b := x, y`、`a, b = b, a`、`i, xs[i] = 1, 9`：先求值左侧下标与全部右值，再依次赋值。
- 复合赋值 `+=` `-=` `*=` `/=` `%=`；`i++`、`i--`。
- `return`，包括多返回值和命名返回值。
- `if` / `else` / `else if` / `if init; cond`（`init` 的变量只在该 `if` 内可见）。
- 表达式 `switch`（tag 只求值一次）、条件 `switch`、类型 `switch`；`case` 里的 `break` 退出 `switch`。
- 三段式循环 `for i := from; cond; post`：每轮求值 `cond`，`post` 是任意简单语句，循环体对 `i` 的写入带入下一轮，闭包捕获每轮自己的 `i`。
- `for i := range xs`（`i` 是下标）、`for i, item := range xs`、`for _, item := range xs`、`for range xs`、`for i := range n`。
- `for k, v := range m`：按 key 升序遍历，每次执行与重放顺序相同。
- `break`、`continue`，包括带标签的形式。
- `panic(expr)`、`defer` 调用。
- 内联函数字面量（闭包），可以赋值给变量再调用。

## 支持的表达式

- 字面量：字符串、整数、浮点、布尔、`nil`。
- 运算：`+ - * / %`、比较、`&&`、`||`、`!`、一元 `-`；整数溢出回绕、除法向零截断，与 Go 相同。
- `==`、`!=`、`switch` 的结果与 Go 相同，含 `any` 与 nil 的比较（见「比较」）。
- 字段访问 `user.Name`、索引 `xs[i]`、切片 `xs[a:b]`（按容量判断越界）、map 读取 `m[k]` 与 `v, ok := m[k]`；key 不存在时得到元素类型的零值（结构体是零值结构体）。
- 复合字面量：`[]T{...}`、`map[string]T{...}`、`pkg.Type{Field: v}`、`Type{Field: v}`、具名 slice/map 类型 `Tags{"a"}`。
- 省略元素类型的复合字面量 `[]Point{ {X: 1} }`、`map[string]Amounts{"x": {1, 2}}`。
- 内建函数：`len`、`cap`、`copy`、`delete`、`append`（语句 `xs = append(xs, v...)`，含 `ys...`）。
- `make(map[K]V)`、`make([]T, 0)`；`xs := make([]T, n)`、`xs := make([]T, n, c)` 写成独立语句（容量与 Go 相同）。
- 类型转换：`int` / `int64` 互转、`time.Duration(n)`、具名字符串类型与 `string`。
- 取址输出参数 `&ident`、`&selector`，只能直接作为调用实参，用于 `Get(ctx, &out)`。
- 类型断言 `x.(T)`、`v, ok := x.(T)`，以及赋值、`return`、表达式语句里的 `x.(T).Field`。

## 生成期拒绝的语法

- 省略 init 或 condition 的 `for`（`for {}`、`for cond {}`）。无限循环写成有上限的计数循环，配合 Continue-As-New。
- 类型检查得不到类型的比较、`switch`、map 读取（包有编译错误或导入无法解析）：修正编译错误后重新生成。
- `goto`、`fallthrough`；`range` 遍历 string、channel、函数。
- 可变参数展开 `fn(xs...)`（桥调用里的 `opts...` 由生成器处理）；可变参数函数字面量。
- `&xs[i]`、`&T{...}` 等复杂取址：先存入局部变量再传 `&local`；`new(T)`（`T` 为本包结构体）。
- 调用实参之外的 `&x`（`p := &a`、`return &a`）：没有指针别名，直接用值。
- 不是独立赋值语句的 `make([]T, n)`、`make([]T, n, c)`；带标签、带初始化语句、tag 是 `any` 的 `switch`。
- 匿名参数的函数字面量。
- string 下标 `s[i]`、切片 `s[a:b]`；完整切片表达式 `xs[a:b:c]`。
- 移位与位运算 `<<` `>>` `&` `|` `^` `&^`（常量运算除外）。
- 计算值上的字段访问与赋值 `xs[i].F`、`f().F`、`xs[i].F = v`：先赋给局部变量。
- 表达式里的 `append`（`ys := append(xs, v)`）；只支持语句 `xs = append(xs, ...)`。
- `recover`、`min`、`max`、`clear`、`print`。
- 不是独立赋值语句的 `make([]T, n)`。
- 条件表达式、闭包或带函数调用的操作数里的 `x.(T).Field`：先 `v := x.(T)`，再用 `v.Field`。
- 读写包级变量、点导入。

## 数据类型

- Workflow 代码接触到的每个数据类型都登记进类型表，嵌套深度不限，包括只出现在子 Workflow / Activity / continue-as-new 实参或 Query 结果字面量里的 struct。
- 当前包与契约包、DTO 包的 struct、具名基础类型、具名 slice/map 可用；递归与互相递归类型经指针、slice 或 map 递归即可；`any` 字段可以持有 struct 值。
- 定长数组（`[N]T`、`[...]T{}`、底层是数组的具名类型、数组字段）生成期报错（`file:line`）：Go 数组是值、WLang 只有共享的 slice；改用 `[]T`。
- 生成期报错（带 `file:line` 与路径，如 `ChildInput.Items[].Meta (chan int): channels cannot cross workflow boundaries`）：channel、函数、有方法的接口、匿名 struct、嵌入字段（改成具名字段）、未导出字段、泛型类型实例化、非字符串 map 键、自定义 JSON 编解码、`complex` / `uintptr`。

## SDK 调用的形态约束

| 调用 | 约束 |
| --- | --- |
| 桥 `CallX` | 只能是 `out, err :=`、`out, err =`、`return`、`if _, err := ...; err != nil` 的完整右值 |
| `workflow.ExecuteActivity` | 第二个参数是 Activity 函数符号（`Charge`、`activities.Charge`）；Future 可以立即 `Get`、存进变量或切片、交给 `Selector.AddFuture`、丢弃 |
| `workflow.Go`、`WaitGroup.Go` | 参数是内联闭包 `func(ctx workflow.Context) { ... }` |
| `workflow.SideEffect`、`MutableSideEffect` | 闭包参数是内联闭包或保存闭包的局部变量 |
| `workflow.SetUpdateHandler` | 内联函数，首参 `workflow.Context`，业务参数个数固定，返回 `error` 或 `(Result, error)` |
| `workflow.SetQueryHandler` | 名称非空；内联的 `func(ctx workflow.Context, input map[string]any) map[string]any` |
| `Selector.AddFuture` / `AddSignalFuture` / `AddChildFuture` | 第二个参数是对应原语返回的 Future 变量或其内联调用 |
| `workflow.IsApplicationError` | 完整 `if` 条件（可带 `!`）或先赋值给变量 |

## 数据类型

- Workflow 与 Activity 的输入输出用带 `json` tag 的 struct。Go `int` 在 WLang 里归一化为 `int64`。
- struct 是值（赋值、传参、存入容器时复制）；slice、map 是引用（`ys := xs` 后元素写入互相可见，`append` 容量够时复用底层数组）。
- `var xs []T`、`var m map[K]V` 是 nil，输出编码为 `null`；写 nil map 让 Workflow Task 失败。
- `map[string]any` 可以作为输入输出和 `Get` 的目标，`d["approved"] == true`、`switch d["action"]` 与 Go 结果相同。

## 比较

生成器按类型信息把与 Go 结果不同的比较改写成 `workflow` 包调用，代码直接写运算符：

- 一侧是 `any` 的 `==` / `!=` → `workflow.Equal`：动态类型与值都相等才为 `true`；`switch` 的 tag 或 case 是 `any` 时每个 case 同样比较。
- slice / map 与 `nil` 比较 → `workflow.IsNil`：`var xs []T`、载荷省略或 `null` 的字段是 nil，`[]T{}`、`make([]T, 0)`、载荷的 `[]` 不是。只关心有无元素写 `len(xs) == 0`。
- 载荷里 `null` 的 slice / map 字段（Go 把 nil slice、nil map 编码成 `null`）与 Go 相同：nil，`len` 为 0，读 map 得零值。覆盖 Workflow 输入、Update 参数，以及按 `Get` 目标类型解码的 Activity / 子 Workflow 结果；同一 Activity type 或子 Workflow Type 的几处 `Get` 目标类型不同、有一处是 `map[string]any` 或目标类型无法静态确定时原样解码，结构体里的 `null` 集合字段先判 `out.Tags != nil`。
- 载荷解进结构体时 `any`、`map[string]any`、`[]any` 字段里的数字是 `float64`（与 `encoding/json` 相同）：`in.V == 1` 为 `false`，`in.V == 1.0` 为 `true`，`in.V.(float64)` 成立。载荷本身是 `map[string]any` 时数字按 `float64` 比较：`d["n"] == 3` 为 `false`，`d["n"] == 3.0` 为 `true`；按数字取值用 struct DTO。
- 差异：`any` 里的具名基础类型值（`type Status string`）按底层类型比较，`var x any = Status("a"); x == "a"` 为 `true`（Go 为 `false`）；先断言成具名类型再比较。
- Side Effect 与 Marker 的值经过 JSON 持久化，超过 2^53 的整数用字符串表示。

## 执行预算

| 预算 | 上限 |
| --- | --- |
| 执行步数（每个 Workflow Task 执行的语句，循环每轮都计入） | 1,000,000 |
| 协程数 | 1,024 |
| 调用深度 | 128 |
| 数组长度 | 100,000 |
| map / 对象 key 数 | 100,000 |
| 单个字符串 | 16 MiB |

步数在每个 Workflow Task 开始时清零，重放时每个 Workflow Task 重新计数，长期运行的 Run 不会因累计步数失败；其余上限针对单个值或同时存在的协程。生成器给每个循环体插入一条语句，循环每轮至少计一步。超出时 Workflow Task 以 `workflow_panic` 失败（reason 以 `[E_BUDGET]` 开头，带超出的预算与位置），Run 保持 Running、显示 Blocked 并退避重试，修复代码发布后继续；Workflow 代码不能捕获。字符串 `+` / `+=`，`strings.Repeat`、`Replace`、`ReplaceAll`、`Join`、`Split`、`Fields`，`fmt.Sprintf`、`Sprint`、`Sprintln`、`Errorf`，`strconv.FormatFloat` 在分配之前按参数算出结果大小，`strings.Repeat("x", 1<<34)` 立即失败；`m[k] = v` 新增 key、`append` 分别检查 key 数与长度；其余函数在返回后检查结果。上限针对单个值，构造大字符串或大数组的计算放进 Activity。

单个 Workflow Task 另有超时，Worker 缺省 10 秒（`worker.DefaultWorkflowTaskTimeout`）。长循环在循环内等待 Activity、Timer 或 Signal，让执行跨越多个 Workflow Task，各自计步；History 接近上限前用 Continue-As-New。
