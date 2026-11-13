package taskq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/donutnomad/taskq/frontend"
	"github.com/donutnomad/tenuto/protocol"
	dispatch2 "github.com/donutnomad/tenuto/server/biz/domain/dispatch"
)

const (
	// activityPhysicalQueuePrefix 是 lazy-dial 出来的 activity 物理 queue 名前缀。
	// 对齐 Temporal 的 queue-type 维度隔离：activity 与 workflow 复用同一套物理
	// queue 基础设施，靠物理名区分——加此前缀后 activity 的物理名与 workflow queue
	// 名天然不可能相撞。未在 TASKQ_ACTIVITY_QUEUES_JSON 显式配置的 route 走
	// `prefix + ActivityTaskQueueKey(ns, tq)`；显式配置的 route 走配置的物理名。
	activityPhysicalQueuePrefix = "activity:"

	// activityFrontendIdleTTL 是 lazy-dial frontend 的空闲回收阈值：连续这么久没有
	// enqueue/dequeue/ack/nack 命中就会被 Close 并移出缓存。
	activityFrontendIdleTTL = 5 * time.Minute
	// activityFrontendGCInterval 是空闲 GC 的扫描周期。
	activityFrontendGCInterval = time.Minute
)

// activityFrontendDialer 按物理 queue 名 lazy 建一个 activity frontend。
type activityFrontendDialer func(ctx context.Context, physicalQueueName string) (*frontend.Frontend, error)

// queueDialer 是 Router 内部使用的 lazy-dial 缝：按物理名建一个 Queue 及其关闭器。
// 用它把「怎么建一个 frontend」与「Router 的缓存/GC 语义」解耦，便于测试注入 fake。
type queueDialer func(ctx context.Context, physicalQueueName string) (*Queue, interface{ Close() }, error)

// activityQueueEntry 是缓存中的一条 lazy-dial 出来的 activity queue，由 Router 负责 Close
// 与空闲回收；lazyCloser 在 Close 之后置 nil。
//
// inUse 是在途调用方的引用计数：queue() 交出 *Queue 前在锁内 +1，调用方用完经 release
// 闭包在锁内 -1。它是「调用方绝不会拿到已 Close 的 frontend」这一不变量的载体——见
// collectIdle 的说明。detached 表示条目已被 GC 或 Close 摘出缓存、只等最后一个持有者
// 归还后由其代为 Close（摘除时 inUse > 0 才会出现）。
type activityQueueEntry struct {
	queue      *Queue
	lazyCloser interface{ Close() }
	lastActive time.Time
	inUse      int
	detached   bool
}

// releaseLocked 归还一次引用。必须在持有 r.mu 时调用。返回需要在锁外 Close 的关闭器
// （仅当该条目已被摘除且这是最后一个持有者时非 nil）。
func (e *activityQueueEntry) releaseLocked(now time.Time) interface{ Close() } {
	e.inUse--
	// 用完的时刻才是空闲计时起点：Dequeue 的 long-poll 可能持有很久，若只在取用时刷新，
	// poll 一返回就会被判定早已超时并立刻收割。
	e.lastActive = now
	if e.inUse > 0 || !e.detached {
		return nil
	}
	closer := e.lazyCloser
	e.lazyCloser = nil
	return closer
}

type ActivityQueueRouter struct {
	// physicalOverrides 把 route 键（ActivityTaskQueueKey 格式或裸 route 名）映射到
	// 显式配置的物理 queue 名。缺省 route 走 activity: 前缀约定。
	physicalOverrides map[string]string
	dial              queueDialer

	mu      sync.Mutex
	entries map[string]*activityQueueEntry
	// closed 在 Close() 中置位：此后 miss 的 route 返回错误，Router 不再 lazy-dial 新的 frontend。
	closed bool

	stop     chan struct{}
	gcDone   chan struct{}
	stopOnce sync.Once
}

// NewLazyActivityQueueRouter 返回一个对齐 Temporal 隐式动态语义的 Router：任意 route
// miss 时用 dial 工厂 lazy 建 frontend 并缓存，空闲后由后台 GC 回收。physicalOverrides
// 里的 route 走配置的物理名，其余 route 走 activity: 前缀约定。
func NewLazyActivityQueueRouter(physicalOverrides map[string]string, dial activityFrontendDialer) *ActivityQueueRouter {
	return newLazyActivityQueueRouter(physicalOverrides, func(ctx context.Context, physicalQueueName string) (*Queue, interface{ Close() }, error) {
		fe, err := dial(ctx, physicalQueueName)
		if err != nil {
			return nil, nil, err
		}
		return NewQueue(fe), fe, nil
	})
}

func newLazyActivityQueueRouter(physicalOverrides map[string]string, dial queueDialer) *ActivityQueueRouter {
	r := &ActivityQueueRouter{
		physicalOverrides: physicalOverrides,
		dial:              dial,
		entries:           make(map[string]*activityQueueEntry),
		stop:              make(chan struct{}),
		gcDone:            make(chan struct{}),
	}
	go r.runGC()
	return r
}

func (r *ActivityQueueRouter) Enqueue(ctx context.Context, taskQueue string, task protocol.Task) error {
	queueName := normalizeTaskQueue(taskQueue)
	task.TaskQueue = queueName
	task.Namespace = protocol.NormalizeNamespace(task.Namespace)
	queue, release, err := r.queue(ctx, task.Namespace, queueName)
	if err != nil {
		return err
	}
	defer release()
	return queue.enqueueWithRoutingKey(ctx, task, taskActivityRoutingKey(task))
}

func (r *ActivityQueueRouter) Dequeue(ctx context.Context, taskQueue string) (protocol.TaskLease, error) {
	queueName := normalizeTaskQueue(taskQueue)
	namespace := protocol.NamespaceFromContext(ctx)
	queue, release, err := r.queue(ctx, namespace, queueName)
	if err != nil {
		return protocol.TaskLease{}, err
	}
	// 持有期覆盖整个 long-poll：有活跃 poller 的条目按定义就不空闲。
	defer release()
	worker, _ := protocol.WorkerRoutingInfoFrom(ctx)
	lease, err := queue.dequeueWithRoutingKeys(ctx, activityRoutingKeysForWorker(namespace, queueName, worker.Identity))
	if err != nil {
		return protocol.TaskLease{}, err
	}
	if lease.Task.TaskQueue == "" {
		lease.Task.TaskQueue = queueName
	}
	if lease.Task.Namespace == "" {
		lease.Task.Namespace = namespace
	}
	return lease, nil
}

func (r *ActivityQueueRouter) Ack(ctx context.Context, lease protocol.TaskLease) error {
	queue, release, err := r.queue(ctx, lease.Task.Namespace, lease.Task.TaskQueue)
	if err != nil {
		return err
	}
	defer release()
	return queue.Ack(ctx, lease)
}

func (r *ActivityQueueRouter) Nack(ctx context.Context, lease protocol.TaskLease) error {
	queue, release, err := r.queue(ctx, lease.Task.Namespace, lease.Task.TaskQueue)
	if err != nil {
		return err
	}
	defer release()
	return queue.Nack(ctx, lease)
}

// Durable 返回 true：任务持久化在 TaskQ 的存储里。
func (r *ActivityQueueRouter) Durable() bool { return true }

// queue 返回给定 namespace/taskQueue 的 Queue，以及一个必须在用完后调用的 release 闭包：
// 先查缓存（命中即取一次引用并刷新 last-active），miss 时 lazy-dial 一个并缓存。
//
// 调用方约定：拿到 (queue, release, nil) 后必须 `defer release()`——在 release 之前该条目
// 不会被空闲 GC 或 Router.Close() 关掉。出错时返回的 release 为 no-op，可无条件 defer。
func (r *ActivityQueueRouter) queue(ctx context.Context, namespace, taskQueue string) (*Queue, func(), error) {
	queueName := normalizeTaskQueue(taskQueue)
	scopedKey := dispatch2.ActivityTaskQueueKey(namespace, queueName)

	r.mu.Lock()
	if entry := r.entries[scopedKey]; entry != nil {
		return r.acquireLocked(entry)
	}
	closed := r.closed
	r.mu.Unlock()

	if closed {
		return nil, noopRelease, fmt.Errorf("workflow activity task queue %s: %w", scopedKey, errActivityRouterClosed)
	}

	physicalName := PhysicalQueueName(r.physicalOverrides, namespace, queueName)
	dialedQueue, closer, err := r.dial(ctx, physicalName)
	if err != nil {
		return nil, noopRelease, fmt.Errorf("dial activity task queue %s (physical %s): %w", scopedKey, physicalName, err)
	}

	r.mu.Lock()
	// 拨号期间 Router 被 Close：这条 frontend 已无人回收，当场关掉。
	if r.closed {
		r.mu.Unlock()
		closer.Close()
		return nil, noopRelease, fmt.Errorf("workflow activity task queue %s: %w", scopedKey, errActivityRouterClosed)
	}
	// 双检：可能有并发调用刚刚建好同一条，回收多余的 frontend，返回既有的。
	if entry := r.entries[scopedKey]; entry != nil {
		queue, release, retErr := r.acquireLocked(entry)
		closer.Close()
		return queue, release, retErr
	}
	entry := &activityQueueEntry{
		queue:      dialedQueue,
		lazyCloser: closer,
		lastActive: time.Now(),
	}
	r.entries[scopedKey] = entry
	return r.acquireLocked(entry)
}

// acquireLocked 对 entry 取一次引用并返回配套的 release 闭包。必须在持有 r.mu 时调用，
// 返回前会解锁 r.mu（把「判定可用」与「登记引用」压在同一个临界区内，见 collectIdle）。
func (r *ActivityQueueRouter) acquireLocked(entry *activityQueueEntry) (*Queue, func(), error) {
	entry.inUse++
	entry.lastActive = time.Now()
	queue := entry.queue
	r.mu.Unlock()

	var once sync.Once
	return queue, func() {
		once.Do(func() {
			r.mu.Lock()
			closer := entry.releaseLocked(time.Now())
			r.mu.Unlock()
			if closer != nil {
				closer.Close()
			}
		})
	}, nil
}

func noopRelease() {}

var errActivityRouterClosed = errors.New("activity queue router is closed")

// PhysicalQueueName 决定给定 namespace/taskQueue 的 activity route 落到哪个物理 queue：
// 显式配置优先，其次走 activity: 前缀约定。查 override 时同时试 namespace
// 作用域键（ActivityTaskQueueKey 格式）与裸 route 名。
//
// 这是 activity 物理名的单一事实源：Router 的运行时路由与 tenuto 启动期分区初始化都调它，
// 保证两侧对同一 route 算出完全一致的物理名，杜绝漂移。
func PhysicalQueueName(overrides map[string]string, namespace, taskQueue string) string {
	scopedKey := dispatch2.ActivityTaskQueueKey(namespace, taskQueue)
	if physical := overrides[scopedKey]; physical != "" {
		return physical
	}
	if physical := overrides[taskQueue]; physical != "" {
		return physical
	}
	return activityPhysicalQueuePrefix + scopedKey
}

// runGC 周期性回收空闲超过 TTL 且当前无在途调用方的 lazy-dial frontend。取用与归还都会
// 刷新 lastActive，引用计数则保证在途调用方持有的条目不会被回收（见 collectIdle）。
func (r *ActivityQueueRouter) runGC() {
	defer close(r.gcDone)
	ticker := time.NewTicker(activityFrontendGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.collectIdle(time.Now())
		}
	}
}

// collectIdle 回收空闲超过 TTL 的 lazy 条目。
//
// 关键不变量：**只有 inUse == 0 的条目才会被 Close**。「条目可回收」的判定与「调用方登记
// 引用」（queue()/acquireLocked 里的 inUse++）发生在同一把锁的同一临界区内，因此不存在
// 「GC 已判定 idle、调用方又刚拿到指针」的交错——调用方一旦持有 *Queue，计数就已 ≥1，本轮
// 及后续 GC 都会跳过它。这是不变量层面的排除，不是靠缩小时间窗口。
//
// 注意：仅仅「从 map 里摘除」并不足以保证安全。摘除只挡住**之后**才来的命中，挡不住**摘除
// 之前**已拿到指针、尚未使用的调用方——那正是本函数早先版本的 use-after-close 缺陷。
func (r *ActivityQueueRouter) collectIdle(now time.Time) {
	var reaped []interface{ Close() }
	r.mu.Lock()
	for key, entry := range r.entries {
		if now.Sub(entry.lastActive) < activityFrontendIdleTTL {
			continue
		}
		if entry.inUse > 0 {
			// 有在途调用方：这条按定义就不空闲，留到下一轮再看。
			continue
		}
		reaped = append(reaped, entry.lazyCloser)
		entry.lazyCloser = nil
		delete(r.entries, key)
	}
	r.mu.Unlock()
	// 此处被 Close 的条目引用计数均为 0，锁外 Close 不会碰到任何在途调用方持有的 frontend。
	for _, closer := range reaped {
		closer.Close()
	}
}

// Close 停掉空闲 GC 并 Close 所有 lazy-dial 出来的 frontend。可重复调用。
//
// 与 collectIdle 同样遵守「不 Close 在途调用方持有的 frontend」：仍有引用的条目只标记
// detached 并摘出缓存，实际 Close 交给最后一个 release 的持有者完成——否则这个窗口只是
// 从 GC 路径搬到了关停路径。
func (r *ActivityQueueRouter) Close() {
	r.stopOnce.Do(func() {
		close(r.stop)
		<-r.gcDone
		var reaped []interface{ Close() }
		r.mu.Lock()
		r.closed = true
		for key, entry := range r.entries {
			delete(r.entries, key)
			if entry.inUse > 0 {
				entry.detached = true
				continue
			}
			reaped = append(reaped, entry.lazyCloser)
			entry.lazyCloser = nil
		}
		r.mu.Unlock()
		for _, closer := range reaped {
			closer.Close()
		}
	})
}

func normalizeTaskQueue(taskQueue string) string {
	if taskQueue == "" {
		return protocol.DefaultActivityTaskQueue
	}
	return taskQueue
}
