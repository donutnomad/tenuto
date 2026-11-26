package workflow

import (
	"reflect"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/sdk/apperror"
)

// Context 是 Workflow 函数里的协程作用域句柄：主函数、workflow.Go 的每条 lane、每个 handler
// 各有一个，由运行时传入；WithCancel 与 NewDisconnectedContext 从它派生可取消的子作用域。
// 作者只把它原样传给 workflow 包的函数，或读取 Done 与 Err。
//
// 取消：Run 收到取消请求时根 Context 被取消，Workflow 代码继续运行。在已取消的
// Context 上，ExecuteActivity、Sleep、ExecuteChildWorkflow 发起的操作被请求取消，对应的
// Future.Get 与 Sleep 返回取消错误（IsCanceledError 为 true），Await 返回取消错误。
type Context interface {
	// Done 返回 Context 被取消时关闭的 Channel，可放进 Selector.AddReceive 与其他 case 一起等待。
	Done() Channel
	// Err 在 Context 未取消时返回 nil，取消后返回 ErrCanceled。
	Err() error
}

// CancelFunc 取消它所属的 Context 及其派生的 Context；重复调用没有效果。
type CancelFunc func()

// WithCancel 返回 parent 的可取消副本：调用 cancel 或 parent 被取消时它被取消。
// 取消一个作用域会请求取消在它（及其派生作用域）上发起、尚未结束的 Activity、timer 与子 Workflow。
func WithCancel(parent Context) (ctx Context, cancel CancelFunc) {
	panic("workflow.WithCancel is a definition marker")
}

// NewDisconnectedContext 返回不继承 parent 取消状态的新 Context：parent 被取消时它不被取消。
// 用于取消之后的清理与补偿，例如在 Workflow 被取消后调度一个清理 Activity。
func NewDisconnectedContext(parent Context) (ctx Context, cancel CancelFunc) {
	panic("workflow.NewDisconnectedContext is a definition marker")
}

// ErrCanceled 是 Context.Err 在 Context 被取消后返回的错误。
var ErrCanceled = NewCanceledError()

// NewCanceledError 构造取消错误，details 随 WorkflowCancelled 事件或 Activity 的取消上报一起保存。
// Workflow 返回取消错误时 Run 以 Cancelled 关闭。
func NewCanceledError(details ...any) error {
	return apperror.NewCanceled(details...)
}

// IsCanceledError 报告 err（或它的 cause 链）是否为取消错误。
func IsCanceledError(err error) bool {
	return apperror.IsCanceled(err)
}

// ContinueAsNewReason 与 Info 定义在 protocol，这里保留 SDK 对外路径。
type ContinueAsNewReason = protocol.ContinueAsNewReason

const (
	// ContinueAsNewReasonHistoryEventCount 表示历史事件数超过了 Server 建议的上限。
	ContinueAsNewReasonHistoryEventCount = protocol.ContinueAsNewReasonHistoryEventCount
	// ContinueAsNewReasonHistorySizeBytes 表示历史大小超过了 Server 建议的上限。
	ContinueAsNewReasonHistorySizeBytes = protocol.ContinueAsNewReasonHistorySizeBytes
)

// Info is an immutable view of the committed history at activation start.
type Info = protocol.WorkflowInfo

// GetInfo returns the history snapshot captured at the start of the current activation.
func GetInfo(ctx Context) Info {
	panic("workflow.GetInfo is a definition marker")
}

// ActivityFuture 是 ExecuteActivity 返回的结果句柄。
type ActivityFuture struct{}

// ActivityOptions 是一次 ExecuteActivity 调用的选项，只作用于这一次调用。
//
// StartToCloseTimeout 与 ScheduleToCloseTimeout 至少设置一个。RetryPolicy 为零值时使用 Server 的
// 缺省重试策略：不限次数，间隔从 1s 起按 2 倍增长、最长 100s，直到成功、遇到不可重试的错误或
// ScheduleToCloseTimeout 到期。
type ActivityOptions struct {
	// ActivityID 是这次调用在 Run 内的业务 ID，出现在 ActivityScheduled 事件、activity.GetInfo(ctx).ActivityID、
	// pending Activity 与 Console 里，异步完成按它寻址。可以是运行期计算的值（如 fmt.Sprintf("charge-%d", i)），
	// 但必须确定：重放时同一调用算出的 ID 与历史不同属于非确定性错误。同一 Run 里与仍未关闭的 Activity
	// 重复时 Workflow Task 失败；前一个关闭之后可以再用。为空时取调用在协程内的位置（如 "#a1"、"/0#a2"）。
	ActivityID string
	TaskQueue  string
	// ScheduleToStartTimeout 是调度后等待 Worker 取走一次 attempt 的上限；到期不再重试。
	ScheduleToStartTimeout time.Duration
	// StartToCloseTimeout 是单次 attempt 从开始到结束的上限；到期按重试策略重试。
	StartToCloseTimeout time.Duration
	// ScheduleToCloseTimeout 是整个 Activity（含全部重试）从调度到结束的上限。
	ScheduleToCloseTimeout time.Duration
	// HeartbeatTimeout 是两次心跳之间的最长间隔；到期按重试策略重试。
	HeartbeatTimeout time.Duration
	RetryPolicy      RetryPolicy
	// WaitForCancellation 决定 ctx 被取消后 Future 何时返回：false（缺省）时取消请求一发出
	// Future 就以取消错误返回；true 时等 Activity 真正结束——确认取消（取消错误）、完成或失败。
	WaitForCancellation bool
}

// RetryPolicy 是 Activity 的重试策略。未设置的字段取 Server 的缺省值：InitialInterval 1s、
// BackoffCoefficient 2.0、MaxInterval 为 100 倍 InitialInterval、MaxAttempts 0（不限次数）。
type RetryPolicy struct {
	// MaxAttempts 是最多执行的次数（含首次）；0 表示不限，1 表示不重试。
	MaxAttempts int
	// InitialInterval 是第一次重试前的等待。
	InitialInterval time.Duration
	// BackoffCoefficient 是每次重试等待相对上一次的倍数，不小于 1。
	BackoffCoefficient float64
	// MaxInterval 是两次重试之间等待的上限。
	MaxInterval time.Duration
	// NonRetryableErrors 按错误 message 子串匹配，命中即不重试。
	NonRetryableErrors []string
	// NonRetryableErrorTypes 按错误 Type 完全相等匹配（大小写敏感），命中即不重试。
	NonRetryableErrorTypes []string
}

// SignalFuture 是 WaitSignal 返回的句柄。同名 Signal 是一个队列（同 Temporal 的 Signal Channel）：Future 第一次被
// 消费（Get 返回，或 Selector 选中它并执行回调）时取走下一条未消费的同名 Signal，之后 Get 一直返回这一条；
// 没被消费的 Future 不占 Signal。
type SignalFuture struct{}

// SideEffectFuture 是 SideEffect 与 MutableSideEffect 返回的结果句柄。
type SideEffectFuture struct{}

// ChildWorkflowFuture 是 ExecuteChildWorkflow 返回的结果句柄。
type ChildWorkflowFuture struct{}

// WaitGroup 等待一组并发执行线（workflow.Go 或 WaitGroup.Go 起的 lane）全部完成。
type WaitGroup struct{}

// Channel 是 Workflow 内并发执行线之间的通道，语义与 Go channel 相同（缓冲、阻塞与关闭），
// 由确定性调度器执行，重放时得到相同的交付顺序。
type Channel struct{}

// NewChannel 创建非缓冲 channel。
func NewChannel(ctx Context) Channel {
	panic("workflow.NewChannel is a definition marker")
}

// NewBufferedChannel 创建容量为 size 的缓冲 channel。
func NewBufferedChannel(ctx Context, size int) Channel {
	panic("workflow.NewBufferedChannel is a definition marker")
}

// Send 发送一个值，缓冲满或没有接收方时阻塞。向已关闭的 channel 发送会 panic。
func (Channel) Send(ctx Context, v any) {
	panic("workflow.Channel.Send is a definition marker")
}

// Receive 接收一个值写入 out；channel 已关闭且没有剩余值时返回 false。
func (Channel) Receive(ctx Context, out any) bool {
	panic("workflow.Channel.Receive is a definition marker")
}

// Close 关闭 channel，唤醒所有阻塞的接收方（Receive 返回 false）。
func (Channel) Close(ctx Context) {
	panic("workflow.Channel.Close is a definition marker")
}

// Selector 同时等待多个 channel 与 future：按声明顺序检查各 case，执行第一个 ready 的 case 的回调，
// 可以带 default。future case 按 future 类型分成 AddFuture（Activity）、AddSignalFuture 与 AddChildFuture。
type Selector struct{}

// NewSelector 创建一个空 Selector。
func NewSelector(ctx Context) Selector {
	panic("workflow.NewSelector is a definition marker")
}

// AddReceive 追加一个 receive case：ch 有值可取或已关闭时回调 f(ch, more)，回调里用 ch.Receive 取值。返回 Selector 以便链式调用。
func (Selector) AddReceive(ctx Context, ch Channel, f func(ch Channel, more bool)) Selector {
	panic("workflow.Selector.AddReceive is a definition marker")
}

// AddSend 追加一个 send case：ch 接受了 v 时回调 f。
func (Selector) AddSend(ctx Context, ch Channel, v any, f func()) Selector {
	panic("workflow.Selector.AddSend is a definition marker")
}

// AddFuture 追加一个 Activity future case：f 结束（完成或失败）时回调 cb(f)，回调里用 f.Get 取结果。
// Workflow 代码会被翻译成名义类型的 WLang 程序，所以 future case 按 future 类型分方法：
// Activity 用 AddFuture，signal 用 AddSignalFuture，子 Workflow 用 AddChildFuture。
// Update handler 里同样可以使用这些方法。
func (Selector) AddFuture(ctx Context, f ActivityFuture, cb func(f ActivityFuture)) Selector {
	panic("workflow.Selector.AddFuture is a definition marker")
}

// AddSignalFuture 追加一个 signal future case：有未消费的同名 Signal 时 f（workflow.WaitSignal 的返回值）ready；
// 选中时 f 取走这条 Signal 并回调 cb(f)。没被选中的 f 不占 Signal。
func (Selector) AddSignalFuture(ctx Context, f SignalFuture, cb func(f SignalFuture)) Selector {
	panic("workflow.Selector.AddSignalFuture is a definition marker")
}

// AddChildFuture 追加一个子 Workflow future case：f（workflow.ExecuteChildWorkflow 的返回值）对应的子 Workflow
// 结束（完成或失败）时回调 cb(f)。
func (Selector) AddChildFuture(ctx Context, f ChildWorkflowFuture, cb func(f ChildWorkflowFuture)) Selector {
	panic("workflow.Selector.AddChildFuture is a definition marker")
}

// AddDefault 注册 default 分支：Select 时没有 case ready 就立即执行 f。
func (Selector) AddDefault(ctx Context, f func()) Selector {
	panic("workflow.Selector.AddDefault is a definition marker")
}

// Select 阻塞直到某个 case ready 并执行它的回调（多个同时 ready 时取声明顺序第一个）；有 default 且没有
// case ready 时立即执行 default。每次 Select 只执行一个回调。
func (Selector) Select(ctx Context) {
	panic("workflow.Selector.Select is a definition marker")
}

// ExecuteActivity 仅用于 workflow 源码定义期，运行时由 wlang host API 接管。
func ExecuteActivity(ctx Context, activity any, input any, opts ...ActivityOptions) ActivityFuture {
	panic("workflow.ExecuteActivity is a definition marker")
}

// Get 等 Activity 结束：成功时把结果解码进 out（非 nil 指针），失败时返回 Activity 的错误。
func (ActivityFuture) Get(ctx Context, out any) error {
	panic("workflow.ActivityFuture.Get is a definition marker")
}

// WaitSignal 返回等待名为 name 的下一条 Signal 的 Future，调用本身不占 Signal，见 SignalFuture。
func WaitSignal(ctx Context, name string) SignalFuture {
	panic("workflow.WaitSignal is a definition marker")
}

// Get 等到 Signal 并把它的参数解码进 out。
func (SignalFuture) Get(ctx Context, out any) error {
	panic("workflow.SignalFuture.Get is a definition marker")
}

// SetUpdateHandler registers a deterministic update handler. Supported
// signatures are func(Context, ...Args) error and
// func(Context, ...Args) (SerializableResult, error).
//
// Register handlers before the workflow's first blocking call. An update whose
// name has no handler when the main coroutine first blocks or returns is
// completed with an "UnknownUpdate" failure.
func SetUpdateHandler(ctx Context, updateName string, handler any) error {
	panic("workflow.SetUpdateHandler is a definition marker")
}

// NewApplicationError 构造可重试的应用错误：type 与 details 随 Activity 失败、Update outcome
// 与 Workflow 终态一起持久化。运行期由解释器宿主实现，返回共享的 *apperror.Error。
func NewApplicationError(message, errorType string, details ...any) error {
	panic("workflow.NewApplicationError is a definition marker")
}

// NewApplicationErrorWithCause 构造带 cause 的可重试应用错误。
func NewApplicationErrorWithCause(message, errorType string, cause error, details ...any) error {
	panic("workflow.NewApplicationErrorWithCause is a definition marker")
}

// NewNonRetryableApplicationError 构造永久失败：重试判定优先看该 flag，不受
// NonRetryableErrors / NonRetryableErrorTypes 列表影响。
func NewNonRetryableApplicationError(message, errorType string, cause error, details ...any) error {
	panic("workflow.NewNonRetryableApplicationError is a definition marker")
}

// IsApplicationError 报告 err 是否为应用错误且其 Type 与 errorType 完全相等
// （大小写敏感）。普通 error 与旧数据（无 type）恒为 false。
func IsApplicationError(err error, errorType string) bool {
	panic("workflow.IsApplicationError is a definition marker")
}

// TimeoutType 是超时的种类，定义在 protocol，这里保留 SDK 对外路径。
type TimeoutType = protocol.TimeoutType

const (
	// TimeoutTypeScheduleToStart：Activity 调度后没有在 ScheduleToStartTimeout 内被 Worker 取走。
	TimeoutTypeScheduleToStart = protocol.TimeoutTypeScheduleToStart
	// TimeoutTypeStartToClose：单次 attempt 超过 StartToCloseTimeout，且重试已耗尽。
	TimeoutTypeStartToClose = protocol.TimeoutTypeStartToClose
	// TimeoutTypeScheduleToClose：整个 Activity 超过 ScheduleToCloseTimeout。
	TimeoutTypeScheduleToClose = protocol.TimeoutTypeScheduleToClose
	// TimeoutTypeHeartbeat：两次心跳之间超过 HeartbeatTimeout，且重试已耗尽。
	TimeoutTypeHeartbeat = protocol.TimeoutTypeHeartbeat
)

// TimeoutTypeOf 返回超时错误的超时种类；err 不是超时错误时返回空串。
// Activity 因超时失败（重试耗尽、ScheduleToClose 或 ScheduleToStart 到期）时，Future.Get 返回的
// error 是超时错误。
func TimeoutTypeOf(err error) TimeoutType {
	panic("workflow.TimeoutTypeOf is a definition marker")
}

// ApplicationErrorDetail 把第 index 个 detail 解码到 out（非 nil 指针，例如 *ChargeFailure）。
// 形态与 Future.Get 的 out 参数一致：宿主按 payload 解码。
//
// 失败都返回明确错误，不伪成功：非应用错误或没有 details 返回 apperror.ErrNoDetails；
// index 越界返回带范围的错误；out 为 nil 或非指针返回 apperror.ErrDetailTarget；
// 载荷与目标类型不匹配返回解码错误。out 只在成功时被改写。
func ApplicationErrorDetail(err error, index int, out any) error {
	panic("workflow.ApplicationErrorDetail is a definition marker")
}

// ApplicationErrorDetailCount 返回应用错误的 details 个数；非应用错误返回 0。
func ApplicationErrorDetailCount(err error) int {
	panic("workflow.ApplicationErrorDetailCount is a definition marker")
}

// SetQueryHandler 登记一个动态 query handler：name 唯一标识 query 类型，handler 在 QueryWorkflow
// 被调用时对「重放到最新态的只读现场」求值一次，返回 map 即 query 结果。
// handler 写成单返回值内联闭包，确保端到端可被 go2wlang 翻译。
//
// 只读约定：query 全程 isReplay=true，handler 内产出的任何 command 都被丢弃，不产事件、不落库、
// 不上报 command。因此 handler 应只读取工作流内存态（变量、闭包捕获），不应依赖副作用。
// handler 不能阻塞或起协程：调用 Await、Sleep、Future.Get、channel、Selector、WaitGroup.Wait 或
// workflow.Go 时本次 query 被拒。
//
// 失败约定：handler 返回的 map 若含非空字符串键 "__query_error"，视为本次 query 被拒
// （QueryRejected）。
//
// 返回值：name 为空或 handler 形状不对时返回 error，登记成功返回 nil。
func SetQueryHandler(ctx Context, name string,
	handler func(ctx Context, input map[string]any) map[string]any) error {
	panic("workflow.SetQueryHandler is a definition marker")
}

// Await 阻塞当前执行线直到 cond() 返回 true，返回 nil；ctx 先被取消时返回取消错误。
// cond 必须纯函数（只读 workflow 内存状态，不读 wall-clock/rand/map 序），每次协程被恢复时重新求值。
func Await(ctx Context, cond func() bool) error {
	panic("workflow.Await is a definition marker")
}

// Sleep 声明 workflow 等待 durable timer 触发，到期返回 nil。ctx 被取消时 timer 被取消
// （TimerCancelled），Sleep 立即返回取消错误。
func Sleep(ctx Context, delay time.Duration) error {
	panic("workflow.Sleep is a definition marker")
}

// Now 返回 event history 中的确定性时间。
func Now(ctx Context) time.Time {
	panic("workflow.Now is a definition marker")
}

// SideEffect 执行一次非确定性的 f 并把返回值记进 History 的 marker；重放时不再执行 f，直接取 marker 里的值。
// 返回值避免大于 2^53 的整数与精度敏感的浮点数：marker 以 JSON 保存会丢精度，大数用字符串表示。
func SideEffect(ctx Context, f func(ctx Context) any) SideEffectFuture {
	panic("workflow.SideEffect is a definition marker")
}

// Get 把记录的值解码进 out（*T，T 是 f 的返回类型）。
func (SideEffectFuture) Get(ctx Context, out any) error {
	panic("workflow.SideEffectFuture.Get is a definition marker")
}

// MutableSideEffect 每次执行 f，只在 equals 判定新值与上次记录的值不同时记一条新 marker；值不变时沿用上次的值，
// 不产生事件。id 标识同一个可变值，跨多次调用追踪它的历史。重放时取 marker 里的值。
func MutableSideEffect(ctx Context, id string, f func(ctx Context) any, equals func(a, b any) bool) SideEffectFuture {
	panic("workflow.MutableSideEffect is a definition marker")
}

// DefaultVersion 是 GetVersion 在历史里没有该 changeID 的记录时返回的版本：
// 这段历史由加入 GetVersion 之前的代码写出。
const DefaultVersion = -1

// GetVersion 返回 changeID 在本次 Run 里固定下来的版本，用它给改动前后的代码分支。
//
// 首次执行到这里时记录并返回 maxSupported；之后每次重放都返回当时记录的值。历史已经越过这一点
// 却没有记录时返回 DefaultVersion。返回值落在 [minSupported, maxSupported] 之外时本次
// Workflow Task 以非确定性错误失败，所以还要支持旧历史时 minSupported 写 DefaultVersion。
func GetVersion(ctx Context, changeID string, minSupported, maxSupported int) int {
	panic("workflow.GetVersion is a definition marker")
}

// RandomUUID 返回写入 marker 的 UUID 字符串。
func RandomUUID(ctx Context) string {
	panic("workflow.RandomUUID is a definition marker")
}

// RandomInt 返回写入 marker 的随机整数。
func RandomInt(ctx Context, max int) int {
	panic("workflow.RandomInt is a definition marker")
}

// ParentClosePolicy 与常量定义在 protocol，这里保留 SDK 对外路径。
type ParentClosePolicy = protocol.ParentClosePolicy

const (
	// ParentClosePolicyTerminate 在父 Run 关闭时终止子 Workflow（缺省）。
	ParentClosePolicyTerminate = protocol.ParentClosePolicyTerminate
	// ParentClosePolicyRequestCancel 在父 Run 关闭时请求取消子 Workflow。
	ParentClosePolicyRequestCancel = protocol.ParentClosePolicyRequestCancel
	// ParentClosePolicyAbandon 让子 Workflow 继续运行。
	ParentClosePolicyAbandon = protocol.ParentClosePolicyAbandon
)

// ChildWorkflowOptions 是 per-call 的子 workflow 选项。
type ChildWorkflowOptions struct {
	// ParentClosePolicy 决定父 Run 关闭（任何终态，含 continue-as-new）时仍在运行的子 workflow 如何处理：
	// 终止（默认）、请求取消或放任运行。
	ParentClosePolicy ParentClosePolicy
	// WaitForCancellation 决定 ctx 被取消后 Future 何时返回：false（缺省）时取消请求一发出
	// Future 就以取消错误返回；true 时等子 Workflow 结束，返回它的结果。
	WaitForCancellation bool
}

// ExecuteChildWorkflow 声明父 workflow 等待子 workflow。workflowType 是子 workflow 的类型；
// 子 Run 的 Workflow ID 由服务端按父 Run 与调用位置生成。
func ExecuteChildWorkflow(ctx Context, workflowType string, input any, opts ...ChildWorkflowOptions) ChildWorkflowFuture {
	panic("workflow.ExecuteChildWorkflow is a definition marker")
}

// Get 等子 Workflow 结束：成功时把结果解码进 out，失败时返回它的错误。
func (ChildWorkflowFuture) Get(ctx Context, out any) error {
	panic("workflow.ChildWorkflowFuture.Get is a definition marker")
}

// ContinueAsNew 声明当前 Run 关闭并创建下一代 Run。workflowType 是下一代的类型，空字符串沿用当前类型；
// 下一代沿用当前 Run 的 Workflow ID。
func ContinueAsNew(ctx Context, workflowType string, input any) error {
	panic("workflow.ContinueAsNew is a definition marker")
}

// Go 起一条并发执行线（lane）运行 f，立即返回，不等待 f 完成，也不收集 f 的结果与错误。
// 需要等待时用 WaitGroup：`wg := NewWaitGroup(ctx); wg.Go(ctx, f0); wg.Go(ctx, f1); wg.Wait(ctx)`；
// lane 把结果与错误写进共享变量，Wait 返回后由调用方检查。
//
// 何时用 Go、何时用裸 future（选型引导）：
//   - fan-out（每 lane 只跑单个 activity）：直接用裸 future 更简单，无需 Go——
//     `f1 := ExecuteActivity(ctx, A, in); f2 := ExecuteActivity(ctx, B, in); f1.Get(ctx,&r1); f2.Get(ctx,&r2)`
//     一次 WFT 并发调度 A、B，各自独立按需等待（ExecuteActivity 立即调度、Future.Get 只等结果）。
//   - 多步并发分支（每 lane 内是有依赖的顺序链，如 Exec(X1).Get(); Exec(X2).Get()）：用 Go——
//     lane 内 X2 必须等 X1 完成才发起，同时兄弟 lane 不被本 lane 的依赖链阻塞。这是 Go 独有的能力。
//     lane 内 Activity、timer、子 Workflow 与随机值的 ID 按 lane 路径与 lane 内序号生成，跨 Workflow Task 稳定，
//     与兄弟 lane 推进快慢无关，多步 lane 与其他并发调用混用时同一个调用不会被调度两次。
//
// future 是 Activity 级的并发（多个 Activity 同一轮调度）；Go 是执行线级的并发（多条内部顺序执行的 lane 并行推进，
// 各自独立挂起与恢复）。在主执行线上 Get 一个未完成的 future 会挂起主执行线，无法只挂起某一条分支等它的第一步，
// 所以多步并发分支需要 Go。
//
// f 是一个内联闭包 func(ctx)：lane 内可调 workflow.ExecuteActivity。
//
// 使用约束：
//   - f 必须写成内联函数字面量；同包函数不能作为值传入，生成期报 file:line。需要复用时在闭包里调用它。
func Go(ctx Context, f func(ctx Context)) {
	panic("workflow.Go is a definition marker")
}

// NewWaitGroup 创建一个计数为 0 的 WaitGroup。
func NewWaitGroup(ctx Context) WaitGroup {
	panic("workflow.NewWaitGroup is a definition marker")
}

// Add 令计数器增加 delta（可为负），计数变为负数时 panic。
func (WaitGroup) Add(ctx Context, delta int) {
	panic("workflow.WaitGroup.Add is a definition marker")
}

// Done 令计数器减 1。
func (WaitGroup) Done(ctx Context) {
	panic("workflow.WaitGroup.Done is a definition marker")
}

// Wait 阻塞直到计数器归零。
func (WaitGroup) Wait(ctx Context) {
	panic("workflow.WaitGroup.Wait is a definition marker")
}

// Go 令计数器加 1 并起一条 lane，lane 结束时自动 Done，等价于
// `wg.Add(1); workflow.Go(ctx, func(ctx){ defer wg.Done(); f(ctx) })`。
func (WaitGroup) Go(ctx Context, f func(ctx Context)) {
	panic("workflow.WaitGroup.Go is a definition marker")
}

// Equal 按 Go 的 == 比较 a 与 b：动态类型与值都相等时为 true，动态类型不可比较时 panic。
// 生成器把操作数含 any 的 == / != 与 any 上的 switch 改写成这个调用，Workflow 代码直接写运算符即可。
func Equal(a, b any) bool {
	return a == b
}

// SamePointer 报告指针 a 与 b 是否指向同一个对象，即 Go 的 a == b。
// 生成器把两侧都是指针的 == / != 改写成这个调用，Workflow 代码直接写运算符即可。
func SamePointer(a, b any) bool {
	return a == b
}

// IsNil 报告 v 是否为 nil 值：nil 接口，或 nil 的 slice、map、指针、函数。
// 生成器把 slice、map 与 nil 的比较改写成这个调用；空 slice（[]T{}、make([]T, 0)）与空 map 不是 nil。
func IsNil(v any) bool {
	if v == nil {
		return true
	}
	switch value := reflect.ValueOf(v); value.Kind() {
	case reflect.Slice, reflect.Map, reflect.Pointer, reflect.Func, reflect.Interface, reflect.Chan:
		return value.IsNil()
	}
	return false
}
