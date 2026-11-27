package engine

import (
	"context"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/clock"
)

// closedPollSessionRetention 是已关闭的 Worker session 在本副本上至少保留的时长：保留期间到达的领取请求立即以超时结束。
// 它覆盖 Worker 停止时已经发出、晚于关闭请求到达的 poll。超过保留期的记录在下一次 shutdown 时清理；session 每个
// Worker 实例只用一次，记录在清理之前一直拒绝该 session 的 poll 不影响其他实例。
const closedPollSessionRetention = 2 * time.Minute

// workerPollSessions 记录本副本上各 Worker 实例（session）在途的领取长轮询（Workflow Task 与 Activity Task）。
// Worker 停止时经 ShutdownWorkerPolls 结束它们：长轮询在连接仍然连着时结束，已出队的租约照常交给 Worker，
// 不再因 Worker 先断开连接而写进死连接。没有 session 的请求不登记。
type workerPollSessions struct {
	clock    clock.Clock
	mu       sync.Mutex
	sessions map[workerPollSessionKey]*workerPollSession
}

type workerPollSessionKey struct {
	namespace, identity, session string
}

type workerPollSession struct {
	polls map[*context.CancelFunc]struct{}
	// closedAt 非零表示 session 已关闭。
	closedAt time.Time
}

func newWorkerPollSessions(c clock.Clock) *workerPollSessions {
	return &workerPollSessions{clock: c, sessions: make(map[workerPollSessionKey]*workerPollSession)}
}

func pollSessionKey(namespace, identity, session string) workerPollSessionKey {
	return workerPollSessionKey{namespace: protocol.NormalizeNamespace(namespace), identity: identity, session: session}
}

// track 登记 ctx 所属 Worker session 的一次长轮询，返回在 session 关闭时取消的 context。session 已关闭时返回
// protocol.ErrTaskPollTimeout。
func (p *workerPollSessions) track(ctx context.Context) (context.Context, context.CancelFunc, error) {
	info, _ := protocol.WorkerInfoFrom(ctx)
	if info.Session == "" {
		return ctx, func() {}, nil
	}
	key := pollSessionKey(info.Namespace, info.Identity, info.Session)
	p.mu.Lock()
	defer p.mu.Unlock()
	session := p.sessions[key]
	if session == nil {
		session = &workerPollSession{polls: make(map[*context.CancelFunc]struct{})}
		p.sessions[key] = session
	}
	if !session.closedAt.IsZero() {
		return nil, nil, protocol.ErrTaskPollTimeout
	}
	ctx, cancel := context.WithCancel(ctx)
	handle := &cancel
	session.polls[handle] = struct{}{}
	return ctx, func() {
		cancel()
		p.mu.Lock()
		defer p.mu.Unlock()
		delete(session.polls, handle)
		if len(session.polls) == 0 && session.closedAt.IsZero() && p.sessions[key] == session {
			delete(p.sessions, key)
		}
	}, nil
}

// shutdown 关闭 session：结束它在途的长轮询，之后 closedPollSessionRetention 内的领取请求立即以超时结束。
func (p *workerPollSessions) shutdown(namespace, identity, session string) {
	now := p.clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, s := range p.sessions {
		if !s.closedAt.IsZero() && now.Sub(s.closedAt) > closedPollSessionRetention {
			delete(p.sessions, key)
		}
	}
	key := pollSessionKey(namespace, identity, session)
	s := p.sessions[key]
	if s == nil {
		s = &workerPollSession{polls: make(map[*context.CancelFunc]struct{})}
		p.sessions[key] = s
	}
	s.closedAt = now
	for cancel := range s.polls {
		(*cancel)()
	}
}

// ShutdownWorkerPolls 结束一个 Worker 实例在本副本上在途的领取长轮询（应答为超时，已出队的租约照常返回），并在
// 一段时间内拒绝它的新领取请求。Worker 停止时先调用它、再断开连接。多副本时只影响接到这个请求的副本。
func (s *Server) ShutdownWorkerPolls(ctx context.Context, namespace, identity, session string) error {
	if identity == "" || session == "" {
		return protocol.InvalidRequestf("worker identity and session are required")
	}
	s.workerPolls.shutdown(namespace, identity, session)
	return nil
}

// taskPollContext 返回领取 Task 的长轮询 context：在 ctx 结束、StopLongPolls 或 Worker session 关闭时取消。
// session 已关闭时返回 protocol.ErrTaskPollTimeout。
func (s *Server) taskPollContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	ctx, release, err := s.workerPolls.track(ctx)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := s.longPollContext(ctx)
	return ctx, func() {
		cancel()
		release()
	}, nil
}
