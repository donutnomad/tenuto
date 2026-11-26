---
title: Workflow 可用的 Go 语法
description: 翻译器接受的 Go 语法、运行期可用的符号与明确的限制
status: Stable
---

# Workflow 可用的 Go 语法

<FeatureStatus status="Stable" />

Workflow 函数体由翻译器 go2wlang 转成 WLang，再由 Worker 里的解释器执行。因此有两层边界：翻译器接受哪些语法，解释器在运行期能解析哪些符号。本页只约束带 `@Workflow` 注解的函数体；Activity 是原生 Go，不受这些限制。

## 运行期可用的符号

解释器绑定以下内容：

- `sdk/workflow` 包的函数与类型（`workflow.ExecuteActivity`、`workflow.Sleep`、`workflow.ActivityOptions` 等）。
- 本次 Program 引用的 Activity 符号（`@Activity` 实现函数与 remote 标记函数）。
- Workflow 文件及契约包里的结构体类型。
- `time.Time` 值（来自 `workflow.Now(ctx)`）的方法：`Add`、`Sub`、`Before`、`After`、`Equal`、`Unix`、`UnixMilli`、`Format`、`String`。其他 `time.Time` 方法（`Year`、`Weekday` 等）与 `time.Duration` 的方法（`Seconds`、`Milliseconds` 等；`time.Duration` 在 Workflow 里是 int64 纳秒）在生成期报 `file:line`。
- 下表列出的确定性标准库函数（见[标准库函数](#标准库函数)）。
- Workflow 函数调用的同包函数与方法（见[同包函数](#同包函数)）：它们的函数体与 Workflow 函数一起翻译进同一个程序。

生成器在生成期把下列写法折叠成字面量：

- `time` 包常量：`time.Nanosecond` 到 `time.Hour`，以及 `time.RFC3339`、`time.DateTime`、`time.DateOnly` 等布局常量。
- `time.Duration` 类型与它的整数运算，例如 `30 * time.Second`、`time.Duration(n) * time.Minute`。`time.Duration` 在 WLang 里是 `int64` 纳秒。
- 常量：包级与函数内的 `const`（包括 `iota` 和同包其他文件里的常量）、常量运算 `1 << 10`，按 Go 推定的类型折叠，`var x int32 = 5` 得到 `int32`。
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

下列包和函数在生成期报错，错误信息带文件行号与替代写法：

| 写法 | 替代 |
| --- | --- |
| `time.Now`、`time.Since`、`time.Until` | `workflow.Now(ctx)` 与 `time.Time` 的 `Sub` |
| `time.Sleep`、`time.After`、`time.NewTimer`、`time.Tick`、`time.NewTicker`、`time.AfterFunc` | `workflow.Sleep(ctx, d)` |
| `time` 包的其他函数（`time.Parse`、`time.Unix` 等） | 放进 Activity |
| `math/rand`、`math/rand/v2`、`crypto/rand` | `workflow.RandomInt`、`workflow.RandomUUID`、`workflow.SideEffect` |
| `github.com/google/uuid` | `workflow.RandomUUID(ctx)` |
| `sync`、`sync/atomic` | `workflow.NewWaitGroup(ctx)`、`workflow.Await(ctx, cond)`；协程是协作式调度，共享变量直接读写 |
| `os`、`os/*`、`net`、`net/*` | 放进 Activity，或用 `workflow.SideEffect` 记录一次结果 |
| `errors` 包 | 构造错误用 `fmt.Errorf` 或 `workflow.NewApplicationError`，判断错误用 `workflow.IsApplicationError`、`workflow.IsCanceledError` |
| [标准库函数](#标准库函数)表以外的标准库函数（`sort.Strings`、`strings.Title`、`math.Pow` 等）与类型（`strings.Builder`） | 报错列出该包可调用的函数；其余计算放进 Activity |
| 其他包的函数与方法（`sdk/workflow` 与 `time.Time` 值的方法除外） | 放进 Activity |
| 把同包函数或方法当成值（`f := helper`） | 写闭包 `func(x int) int { return helper(x) }`；`workflow.ExecuteActivity` 的 Activity 参数不受此限 |

## 标准库函数

Workflow 代码可以调用下面这些标准库函数，结果与原生 Go 相同（`go2wlang/differential_test.go` 对每个函数在原生 Go 与解释器里各跑一遍并比较结果）：

| 包 | 可调用的函数 |
| --- | --- |
| `strings` | `Contains`、`Count`、`EqualFold`、`Fields`、`HasPrefix`、`HasSuffix`、`Index`、`Join`、`LastIndex`、`Repeat`、`Replace`、`ReplaceAll`、`Split`、`ToLower`、`ToUpper`、`Trim`、`TrimLeft`、`TrimPrefix`、`TrimRight`、`TrimSpace`、`TrimSuffix` |
| `strconv` | `Atoi`、`FormatBool`、`FormatFloat`、`FormatInt`、`Itoa`、`ParseBool`、`ParseFloat`、`ParseInt` |
| `fmt` | `Sprintf`、`Sprint`、`Sprintln`、`Errorf` |
| `math` | `Abs`、`Ceil`、`Floor`、`Inf`、`IsInf`、`IsNaN`、`Max`、`Min`、`Mod`、`NaN`、`Round`、`Sqrt`、`Trunc`；常量（`math.MaxInt64`、`math.Pi` 等）在生成期折叠 |

```go
qty, err := strconv.Atoi(raw)
if err != nil {
    return fmt.Errorf("bad qty %s: %w", raw, err) // 文本：bad qty x: strconv.Atoi: parsing "x": invalid syntax
}
id := fmt.Sprintf("order-%05d", qty)
sku := strings.ToUpper(strings.TrimSpace(in.SKU))
```

- 多返回值与 Go 相同：`strconv.Atoi` 返回 `(int, error)`，错误值的 `Error()` 文本与 Go 一致，可以用 `%w` 包装后作为 Workflow 的失败返回。
- `strings.Split`、`strings.Fields` 返回 `[]string`，可以下标、`range`、`append`。
- `fmt` 的格式串必须是常量。支持的动词：`%v %d %b %o %x %X %s %t %e %E %f %F %g %G`，`fmt.Errorf` 另有 `%w`；标志 `+ - 空格 0`，`#` 只用于 `%x %X %o %b`；宽度与精度写数字（`%05.2f`）。操作数只能是布尔、整数、浮点、字符串（含无方法的具名类型，如 `type Status string`）与 `error`。生成器按 Go 的规则核对每个动词与操作数：`%T`、`%q`、`%*d`、`%[1]d`、动词与操作数不匹配、个数不一致、带 `String()` 等方法的类型（包括 `time.Duration`）、结构体、slice、map、`any` 都报 `file:line`。
- 这些函数在 Worker 进程里以 Go 实现执行，行为由构建 Worker 的 Go 工具链决定。会随 Go 版本或 CPU 架构变化的函数不在表里：`strconv.Quote` 与 `%q` 的转义取决于 Go 版本内置的 Unicode 表，`math.Pow` 等非精确舍入的浮点函数在 arm64 上可能被编译成 FMA 指令。`strings.ToUpper`、`ToLower`、`EqualFold` 同样依赖 Unicode 表，只有新 Unicode 版本新增的大小写字符会在升级 Go 后得到不同结果；参与分支判断的数据含这类字符时，升级 Go 版本按[版本控制](/develop/versioning)处理。

## 同包函数

Workflow 函数可以调用同一个包里的普通函数和值接收者方法，包括其他文件里的函数。生成器从 Workflow 函数出发找出所有被调用的同包函数（包括它们再调用的），把函数体和 Workflow 函数一起翻译进同一个程序。

```go
func (l Line) Total() int { return l.Qty * l.Cents }

func reserve(ctx workflow.Context, sku string) (string, error) {
    out, err := CallReserve(ctx, ReserveInput{SKU: sku}, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
    return out.ID, err
}

func PlaceOrder(ctx workflow.Context, in OrderInput) (OrderOutput, error) {
    id, err := reserve(ctx, normalize(in.SKU))
    ...
}
```

- 支持：多返回值与命名返回值、`defer`、递归与互相调用（调用深度上限 128）、把 `workflow.Context` 作为参数并在函数里调度 Activity、timer、子 Workflow、调用桥 `CallX`、调用上表的标准库函数、读取包级常量。
- 参数按 Go 语义传值：结构体参数是副本，slice 与 map 共享。函数只看得见自己的参数、局部变量、常量与其他同包函数。
- 生成期拒绝，报函数声明或调用点的 `file:line`：泛型函数、可变参数函数、指针接收者方法、嵌入字段提升的方法、经本包接口类型调用的方法、读写包级变量、把函数或方法当成值。
- 被调用函数的函数体属于产物：只改一个同包函数也会改变 Workflow 的内容版本，部署时按[版本控制](/develop/versioning)处理。
- 生成器对这些函数做与 Workflow 函数相同的检查与改写（`time.Now`、原生并发、桥调用形态等）。直接调用 `@Activity` 实现函数仍然报错，要经桥 `CallX` 调度。

## 支持的语句

翻译产物与 Go 源码行为一致：下面的写法在 WLang 里得到和 Go 相同的结果，做不到一致的写法在生成期报错（见[生成期拒绝的语法](#生成期拒绝的语法)）。

- 变量：`var x = expr`、`var x T`、`x := expr`、`x = expr`、`a, b := call()`、`a, b = call()`、`_ = expr`。`a, b = call()` 与 `v, ok = m[k]` 更新已有变量。
- 多值赋值：`a, b := x, y`、`a, b = b, a`、`i, xs[i] = 1, 9`，先求值左侧下标与全部右值，再依次赋值。
- 复合赋值：`+=`、`-=`、`*=`、`/=`、`%=`；`i++`、`i--`。
- `return`，包括多返回值和命名返回值。
- `if` / `else` / `else if` / `if init; cond`，`init` 声明的变量只在该 `if` 内可见。
- 表达式 `switch`（tag 只求值一次）、条件 `switch`、类型 `switch`；`case` 里的 `break` 退出 `switch`。
- 三段式循环 `for i := from; cond; post`：每轮重新求值 `cond`，`post` 可以是任意简单语句（`i++`、`i += 2`、`i = i * 2`），循环体对 `i` 的写入带入下一轮，闭包捕获每轮自己的 `i`。
- `for i := range xs`（`i` 是下标）、`for i, item := range xs`、`for _, item := range xs`、`for range xs`。迭代次数在循环开始时固定，元素在每轮开始时读取。
- `for i := range n`。
- `for k, v := range m`：按 key 升序遍历，见[确定性：Map 与迭代顺序](/develop/determinism#map-与迭代顺序)。
- `break`、`continue`，包括带标签的形式。
- `panic(expr)`、`defer` 调用（参数在 `defer` 处求值，按后进先出执行，可修改命名返回值）。
- 内联函数字面量（闭包），可以赋值给变量再调用。

## 支持的表达式

- 字面量：字符串、整数、浮点、布尔、`nil`。
- 运算：`+ - * / %`、比较、`&&`、`||`、`!`、一元 `-`。整数按位宽溢出回绕，整数除法与 `%` 向零截断，与 Go 相同。
- `==`、`!=` 与 `switch` 的比较结果与 Go 相同，见下文「比较」。
- 字段访问 `user.Name`、slice 索引 `xs[i]`、切片 `xs[a:b]`（按容量判断越界，与 Go 相同）、map 读取 `m[k]` 与 `v, ok := m[k]`。key 不存在（含 nil map）时得到元素类型的零值，元素是结构体时得到各字段为零值的结构体。
- 复合字面量：`[]T{...}`、`map[string]T{...}`、`pkg.Type{Field: v}`、当前包 `Type{Field: v}`、具名 slice/map 类型 `Tags{"a"}`（`type Tags []string`），以及省略元素类型的 `[]Point{ {X: 1} }`、`map[string]Amounts{"x": {1, 2}}`。
- 内建函数：`len`、`cap`、`copy`、`delete`、`append`（语句 `xs = append(xs, v...)`，包括 `xs = append(xs, ys...)`）。
- `make`：`make(map[K]V)`、`make([]T, 0)` 得到空容器；`xs := make([]T, n)` 与带容量的 `xs := make([]T, n, c)` 写成独立语句，得到 n 个零值、容量 c（`xs[:k]` 在 k 不超过 c 时有效）。
- 类型转换：不改变数值表示的转换，`int` 与 `int64` 互转、`time.Duration(n)`（`n` 为整数）、具名字符串类型与 `string` 互转。
- 取址输出参数：`&ident`、`&selector` 直接作为调用实参，用于 `Get(ctx, &out)` 等调用。
- 类型断言 `x.(T)`、`v, ok := x.(T)`、type switch，以及赋值、`return`、表达式语句里的 `x.(T).Field`（生成器先把断言赋给临时变量）。载荷里的嵌套数组与嵌套对象用 `x.([]any)`、`x.(map[string]any)` 与 `case []any`、`case map[string]any` 读取，结果与 Go 相同：动态类型不对时非 comma-ok 形式 panic（Workflow Task 以 `workflow_panic` 失败），comma-ok 形式得到 nil 与 `false`。差异：Workflow 代码里类型化的 slice、map 放进 `any` 之后（`var x any = []Order{...}`、`m["items"] = [][]any{...}`，以及 `[]map[string]any`、`map[string]Order`），解释器只保留它的元素值，`x.([]any)`、`x.(map[string]any)` 与对应的 case 判为成立，Go 里不成立；对这类值断言回它自己的类型（`x.([]Order)`）。

## 生成期拒绝的语法

以下写法返回带源码位置的诊断：

- 省略 init 或 condition 的 `for`（`for {}`、`for cond {}`）。
- 类型检查得不到类型的比较、`switch`、map 读取（包有编译错误或导入无法解析）：`类型检查没有得到 x 的类型`。生成器按类型决定这些写法是否改写，修正编译错误后重新生成。
- `go` 语句、`select`、原生 `chan`、`ch <- v`、`<-ch`，见下一节。
- `goto`、`fallthrough`。
- `range` 遍历 string、channel 或函数。
- 可变参数展开 `fn(xs...)`（桥调用里的 `opts...` 由生成器处理）。
- `&xs[i]`、`&T{...}` 等复杂取址表达式，先存入局部变量再传 `&local`；`new(T)`（`T` 为本包结构体）。
- 调用实参之外的 `&x`、`&x.F`，例如 `p := &a`、`return &a`、`Out{P: &a}`：Workflow 程序没有指针别名，经 `p` 的读写不会作用到 `a`；直接使用值。
- 匿名参数的函数字面量、可变参数函数字面量 `func(xs ...int)`。
- 改变数值表示的类型转换：`float64(v)`、`int(f)`（`f` 为浮点）、`string(v)`、`[]byte(s)`、`int32(v)`。
- string 的下标 `s[i]` 与切片 `s[a:b]`、完整切片表达式 `xs[a:b:c]`。
- 移位与位运算 `<<`、`>>`、`&`、`|`、`^`、`&^`（常量运算除外）。
- 计算值上的字段访问与赋值：`xs[i].Field`、`f().Field`、`xs[i].Field = v`，先赋给局部变量。
- 表达式里的 `append`，例如 `ys := append(xs, v)`、`f(append(xs, v))`；只支持语句 `xs = append(xs, ...)`。
- 内建函数 `recover`、`min`、`max`、`clear`、`print`。
- 不是独立赋值语句的 `make([]T, n)` 与 `make([]T, n, c)`。
- 带标签、带初始化语句、tag 是 `any` 的 `switch`（`L: switch v := f(); x { ... }`）：把初始化语句移到 `switch` 之前。
- 条件表达式、闭包或带函数调用的操作数里的 `x.(T).Field`：先写 `v := x.(T)`，再用 `v.Field`。
- 读写包级变量、点导入。
- 不能跨 Workflow 边界的数据类型，见下一节。

## 数据类型

Workflow 代码接触到的每个数据类型都登记进 Program 的类型表（`workflow.json` 里 `program.types`），嵌套深度不限：入口签名、局部变量、复合字面量（包括只出现在 `ExecuteChildWorkflow`、`ExecuteActivity`、`ContinueAsNew` 实参或 Query 结果里的字面量）、闭包与同包函数的参数和结果、Signal 与 Activity 结果的接收变量、Update Handler 的参数和结果，以及从这些类型经字段、指针、slice、map 元素和具名类型的底层类型能到达的全部类型。

- 当前包与其他非 SDK 包（契约包、DTO 包）声明的 struct、具名基础类型、具名 slice / map 都可以用；定长数组见下文「数据类型」。
- 递归与互相递归的类型可以用，递归要经过指针、slice 或 map，与 Go 相同：

```go
type Node struct {
    Name     string           `json:"name"`
    Children []Node           `json:"children"`
    Parent   *Node            `json:"parent"`
    Index    map[string]*Node `json:"index"`
}
```

- 字段类型 `any` 可以持有任意值，其中的 struct 值按它自己的类型编码。
- 同一源码两次生成的类型表逐字节相同，产物内容版本不变。

以下类型在生成期报错，诊断带 `file:line` 与类型内的路径（`[]` 是元素，`[key]` 是 map 键）：

| 类型 | 诊断（节选） |
| --- | --- |
| channel | `ChildInput.Items[].Meta (chan int): channels cannot cross workflow boundaries` |
| 函数 | `functions cannot cross workflow boundaries` |
| 有方法的接口 | `interfaces with methods cannot cross workflow boundaries` |
| 匿名 struct 类型 | `anonymous struct types are not supported; declare a named type` |
| 嵌入字段（`type In struct { Common; Amount int }`） | `embedded field Common is outside json/plain-v1`，改写成具名字段 |
| 未导出字段 | `unexported field x is outside json/plain-v1` |
| 泛型类型实例化（`Pair[int]`） | `generic type instantiations are not supported; declare a non-generic type` |
| 非字符串底层的 map 键 | `json/plain-v1 map keys must be string` |
| 实现 `MarshalJSON` / `UnmarshalJSON` 的类型 | `custom JSON codec on ... requires a dedicated payload encoding` |
| `complex64`、`uintptr` 等 | `type ... is outside json/plain-v1` |

## 并发用 SDK 原语

Workflow 协程由解释器按 History 协作式调度。原生并发语法在生成期报错，并给出对应的 SDK 写法：

| 原生语法 | Workflow 写法 |
| --- | --- |
| `go func() { ... }()` | `workflow.Go(ctx, func(ctx workflow.Context) { ... })` |
| `sync.WaitGroup` | `workflow.NewWaitGroup(ctx)` |
| `make(chan T)` / `<-ch` / `ch <- v` | `workflow.NewChannel(ctx)`、`ch.Receive`、`ch.Send` |
| `select` | `workflow.NewSelector(ctx)` |
| `time.Sleep` / `time.After` | `workflow.Sleep` |

见 [并发、Channel 与 Selector](/develop/concurrency)。

## SDK 调用的形态约束

生成器对 SDK 调用做额外检查：

| 调用 | 约束 |
| --- | --- |
| `workflow.ExecuteActivity` | 第二个参数是 Activity 函数的符号（`Charge`、`activities.Charge`）；返回的 Future 可以立即 `Get`、存进变量或切片、交给 `Selector.AddFuture`，也可以丢弃 |
| `workflow.Go`、`WaitGroup.Go` | 参数是内联闭包 `func(ctx workflow.Context) { ... }`，传包级函数名在运行期被拒绝 |
| `workflow.SideEffect`、`MutableSideEffect` | 闭包参数是内联闭包或保存闭包的局部变量 |
| `workflow.SetUpdateHandler` | Handler 是内联函数，第一个参数是 `workflow.Context`，业务参数个数固定（不支持 `...T`），返回 `error` 或 `(Result, error)` |
| `workflow.SetQueryHandler` | Query 名非空；Handler 是内联的 `func(ctx workflow.Context, input map[string]any) map[string]any` |
| `Selector.AddFuture` 系列 | 第二个参数是对应原语返回的 Future 变量或其内联调用；Future 的种类由 Go 类型检查保证与方法匹配 |
| `workflow.IsApplicationError` | 作为完整 `if` 条件（可带 `!`）或先赋值给变量；`else if`、`if init;`、`&&` / `||` 组合、`for` / `switch` 条件位置报错 |
| 桥 `CallX` | 只能是 `out, err :=`、`out, err =`、`return` 的完整右值 |

## 数据类型

- Workflow 输入输出、Activity 输入输出使用带 `json` tag 的结构体。Go `int` 在 WLang 里归一化为 `int64`。
- 结构体是值：赋值、传参、存入 slice / map / 其他结构体时复制，与 Go 相同。slice 与 map 是引用：`ys := xs` 后的元素写入互相可见，`append` 在容量足够时复用底层数组。
- 定长数组（`[N]T`、`[...]T{...}`、底层是数组的具名类型、数组字段）在生成期报错（`file:line`）：Go 的数组赋值与传参是复制，Workflow 程序里只有引用语义的 slice，翻译后会共享元素；改用 slice `[]T`。
- `var xs []T`、`var m map[K]V` 是 nil，作为输出编码为 JSON `null`；`[]T{}` 编码为 `[]`。写 nil map 让 Workflow Task 失败（Go 里是 panic）。
- `map[string]any` 可以作为输入输出和 `Get` 的目标，按字段分支直接写 `d["approved"] == true`、`switch d["action"] { ... }`，结果与 Go 相同（见「比较」）。
- Side Effect 和 Marker 的值经过 JSON 持久化，超过 2^53 的整数用字符串表示。

## 比较

`==`、`!=` 与 `switch` 按 Go 的规则比较。生成器按 Go 类型信息把 WLang 运算结果与 Go 不同的比较改写成 `workflow` 包的调用，Workflow 代码直接写运算符：

| 写法 | 改写成 | 结果 |
| --- | --- | --- |
| 一侧是 `any`（`d["action"] == "cancel"`、`input["n"] != 3`、`a == b`） | `workflow.Equal(a, b)` | 动态类型与值都相等时为 `true`；动态类型相同但不可比较（slice、map、含它们的结构体）时与 Go 一样 panic |
| `switch` 的 tag 或某个 `case` 是 `any` | tag 求值一次，每个 `case` 用 `workflow.Equal` 比较 | 命中的 `case` 与 Go 相同，`case nil` 匹配 nil 接口 |
| slice 或 map 与 `nil` 比较（`xs == nil`、`m != nil`） | `workflow.IsNil(x)` | 判断是否为 nil 值 |

- 常量按 Go 的默认类型参与比较：`d["n"] == 3` 比较的是 `int`。
- 载荷解进结构体时，`any` 字段、`map[string]any` 字段、`[]any` 字段里的 JSON 数字与 Go 的 `encoding/json` 一样是 `float64`：`{"V": 1}` 里 `in.V == 1` 是 `false`，`in.V == 1.0` 是 `true`，`in.V.(float64)` 成立，再编码成 JSON 仍是 `1`。
- 载荷本身是 `map[string]any`（Workflow 输入、Signal、Update 参数、Query 输入，以及 `Get` 进 `map[string]any` 的 Activity 结果与子 Workflow 结果；后三者要求同名载荷的各处 `Get` 都进 `map[string]any`，见下文）时，其中各层的数字与 Go 的 `encoding/json` 一样是 `float64`：`{"amount": 3}` 里 `d["amount"].(float64)` 成立，`d["amount"] == 3` 是 `false`，`d["amount"] == 3.0` 是 `true`，再编码成 JSON 仍是 `3`。
- 存储与 HTTP 两侧保留整数的精确值；交给 Workflow 代码时按 `float64` 表示，超过 2^53 的整数（如 `9007199254740993`）在 `map[string]any` 与 `any` 里丢失精度，Go 解码进 `map[string]any` 时同样如此。需要精确的大整数时声明结构体字段 `int64`，或用字符串传递。
- nil slice 与空 slice 可以区分：`var xs []T`、载荷里省略或为 `null` 的字段是 nil；`[]T{}`、`make([]T, 0)`、载荷里的 `[]` 不是 nil。map 同理。只关心有没有元素时写 `len(xs) == 0`。
- 载荷里为 `null` 的 slice、map 字段（Go 的 `encoding/json` 把 nil slice、nil map 编码成 `null`）与 Go 一样是 nil：`len` 为 0、`range` 不执行、读 map 得到元素零值。Workflow 输入、Update 参数按声明类型解码；Activity 结果与子 Workflow 结果按 Program 里 `Get` 的目标类型解码。Worker 在 `Get` 时不知道 out 的类型（解进结构体的转换在 WLang 内部完成），只能在结果进入解释器时按 Program 里全部 `Get` 的目标类型决定，所以同一个 Signal 名、Activity type 或子 Workflow Type 的载荷在几处 `Get` 进不同的类型（例如一处结构体、一处 `map[string]any`），或有一处目标类型无法静态确定时，载荷原样交给解释器：解进结构体时 `int64` 字段（例如雪花 ID）保持精确；与 Go 不同的是，`null` 集合字段在 `len`、`range` 之前先判断 `out.Tags != nil`，结构体 `any` 字段与 `map[string]any` 里的整数是 `int64`（`d["n"].(float64)` 不成立）。
- `any` 里的具名基础类型值（`type Status string`）按底层类型比较：`var x any = Status("a")` 时 `x == "a"` 为 `true`，Go 里动态类型不同为 `false`。与具名类型比较时先断言 `s, ok := x.(Status)`。

## 执行预算

执行步数按 Workflow Task 计：每个 Workflow Task 开始时清零，重放时每个已完成的 Workflow Task 重新计数，同一个 Workflow Task 在 live 与重放时于同一条语句触顶。长期运行的 Run 只要每个 Workflow Task 的计算有界，就不会因为累计步数失败。其余上限针对单个值或同时存在的协程：

| 预算 | 上限 |
| --- | --- |
| 执行步数（一个 Workflow Task 执行的语句，循环每轮执行的语句都计入；生成器给每个循环体插入一条语句，循环每轮至少计一步） | 1,000,000 |
| 协程（`workflow.Go`） | 1024 |
| 调用深度 | 128 |
| 数组长度 | 100,000 |
| map / 对象 key 数 | 100,000 |
| 单个字符串 | 16 MiB（16,777,216 字节） |

超出任一预算时解释器返回 `E_BUDGET` 错误，Workflow 代码不能捕获它。与 Go 运行时 panic 一样，Workflow Task 以 `workflow_panic` 失败（`WorkflowTaskFailed` 的 reason 以 `[E_BUDGET]` 开头，带超出的预算与 Program 位置），Run 保持 Running、显示 Blocked 并按退避重试，对标 Temporal Go SDK 的 deadlock detector 把单个 Workflow Task 运行过久按 panic 处理。同一段代码在重放时于同一位置触顶；修复代码（把计算拆到多个 Workflow Task 或放进 Activity）发布后，Run 从失败处继续。

大小上限在分配之前检查：

- 字符串 `+` / `+=` 按两边长度相加；`strings.Repeat`、`Replace`、`ReplaceAll`、`Join`、`Split`、`Fields`、`fmt.Sprintf`、`Sprint`、`Sprintln`、`Errorf`、`strconv.FormatFloat` 由参数算出结果大小或上界，超出时不调用函数。例如 `strings.Repeat("x", 1<<34)` 立即失败，不会分配 16 GiB。
- `m[k] = v` 新增 key 时检查 key 数；`append` 检查数组长度。
- 其余标准库与 Workflow API 函数在返回后检查结果最外层的字符串长度、数组长度与 key 数；`strings.ToUpper` 这类结果至多是参数几倍的函数走这条路径。

上限针对单个值，不限制所有值合计占用的内存；值的个数受执行步数、数组长度与 key 数约束。

Workflow Task 另有超时，Worker 默认 10 秒（`worker.DefaultWorkflowTaskTimeout`）。长循环在循环内等待 Activity、Timer 或 Signal，让执行跨越多个 Workflow Task，每个 Workflow Task 的步数分别计算；History 增长到上限前使用 [Continue-As-New](/develop/continue-as-new)。
