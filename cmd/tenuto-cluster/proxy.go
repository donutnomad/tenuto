package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// 本文件是故障注入用的进程内 TCP 代理：副本到 MySQL、副本之间的 matching gRPC、调用端到副本的 HTTP
// 都经过它。代理只在本机转发字节，按规则断开连接、让流量停滞（不发 RST，对端只看到没有响应）或给每个
// 数据块加延迟；MySQL 服务端与副本进程本身不受影响。

// stallPoll 是停滞中的连接重新检查规则的间隔。
const stallPoll = 20 * time.Millisecond

// netRule 是一条有时限的网络故障规则。From 非空时只作用于来源是 From 的连接，NotFrom 非空时只作用于
// 来源不是 NotFrom 的连接（来源未识别的连接算作「不是」）。
type netRule struct {
	Until   time.Time
	Stall   bool
	Latency time.Duration
	From    string
	NotFrom string
}

func (r netRule) matches(source string) bool {
	return (r.From == "" || r.From == source) && (r.NotFrom == "" || r.NotFrom != source)
}

// netRules 是一组规则；过期的规则在读取时移除。
type netRules struct {
	mu    sync.Mutex
	rules []netRule
}

func (n *netRules) add(rule netRule) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rules = append(n.rules, rule)
}

// state 返回来源为 source 的连接此刻是否停滞、要加多少延迟（多条延迟规则取最大值）。
func (n *netRules) state(source string, now time.Time) (stall bool, latency time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	kept := n.rules[:0]
	for _, rule := range n.rules {
		if !now.Before(rule.Until) {
			continue
		}
		kept = append(kept, rule)
		if rule.matches(source) {
			stall = stall || rule.Stall
			latency = max(latency, rule.Latency)
		}
	}
	clear(n.rules[len(kept):])
	n.rules = kept
	return stall, latency
}

// relayConn 是代理转发的一对连接。source 是发起方的标识（副本名或负载均衡器的后端名），可能在转发开始后
// 才识别出来。
type relayConn struct {
	client net.Conn
	rules  *netRules
	source atomic.Pointer[string]

	mu       sync.Mutex
	upstream net.Conn
	done     chan struct{}
	once     sync.Once
}

func newRelayConn(client net.Conn, rules *netRules, source string) *relayConn {
	c := &relayConn{client: client, rules: rules, done: make(chan struct{})}
	c.setSource(source)
	return c
}

func (c *relayConn) setSource(source string) { c.source.Store(&source) }

func (c *relayConn) sourceName() string { return *c.source.Load() }

func (c *relayConn) state() (bool, time.Duration) { return c.rules.state(c.sourceName(), time.Now()) }

func (c *relayConn) setUpstream(upstream net.Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return false
	default:
	}
	c.upstream = upstream
	return true
}

// close 关闭两端；重复调用无副作用。
func (c *relayConn) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.client.Close()
		c.mu.Lock()
		if c.upstream != nil {
			_ = c.upstream.Close()
		}
		c.mu.Unlock()
	})
}

// waitOpen 等到规则不再让这条连接停滞；连接关闭时返回 false。
func (c *relayConn) waitOpen() bool {
	for {
		if stall, _ := c.state(); !stall {
			return true
		}
		select {
		case <-c.done:
			return false
		case <-time.After(stallPoll):
		}
	}
}

// run 先等停滞结束再拨号上游（停滞期间新连接被接受但没有任何回应），然后双向转发，任一方向结束即关闭两端。
func (c *relayConn) run(dial func() (net.Conn, error)) {
	defer c.close()
	if !c.waitOpen() {
		return
	}
	upstream, err := dial()
	if err != nil {
		return
	}
	if !c.setUpstream(upstream) {
		_ = upstream.Close()
		return
	}
	finished := make(chan struct{}, 2)
	go func() { c.relay(upstream, c.client); finished <- struct{}{} }()
	go func() { c.relay(c.client, upstream); finished <- struct{}{} }()
	<-finished
	c.close()
	<-finished
}

type chunk struct {
	data []byte
	at   time.Time
}

// relay 把 src 读到的数据块写到 dst：每块在到达后 latency 才写出（延迟线，不降低吞吐），停滞期间不写出；
// 缓冲满后不再读 src，对端的 TCP 窗口随之关闭。
func (c *relayConn) relay(dst, src net.Conn) {
	chunks := make(chan chunk, 64)
	go func() {
		defer close(chunks)
		buf := make([]byte, 32<<10)
		for {
			n, err := src.Read(buf)
			if n > 0 {
				select {
				case chunks <- chunk{data: bytes.Clone(buf[:n]), at: time.Now()}:
				case <-c.done:
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	for item := range chunks {
		for {
			stall, latency := c.state()
			wait := stallPoll
			if !stall {
				if wait = time.Until(item.at.Add(latency)); wait <= 0 {
					break
				}
			}
			select {
			case <-c.done:
				return
			case <-time.After(min(wait, stallPoll*5)):
			}
		}
		if _, err := dst.Write(item.data); err != nil {
			return
		}
	}
}

// faultProxy 把 listener 上的连接转发到 target，规则见 netRules。identify 非 nil 时在后台识别每条连接的
// 发起方（结果写入连接的 source），识别前 source 是空串。
type faultProxy struct {
	name     string
	listener net.Listener
	network  string
	target   string
	identify func(remote net.Addr) string
	netRules

	wg     sync.WaitGroup
	mu     sync.Mutex
	conns  map[*relayConn]struct{}
	closed bool
}

func newFaultProxy(name, listen, network, target string, identify func(net.Addr) string) (*faultProxy, error) {
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("%s listen %s: %w", name, listen, err)
	}
	p := &faultProxy{name: name, listener: listener, network: network, target: target, identify: identify, conns: map[*relayConn]struct{}{}}
	p.wg.Add(1)
	go p.serve()
	return p, nil
}

func (p *faultProxy) addr() string { return p.listener.Addr().String() }

func (p *faultProxy) serve() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		conn := newRelayConn(client, &p.netRules, "")
		if !p.track(conn, true) {
			conn.close()
			return
		}
		if p.identify != nil {
			go func() {
				if source := p.identify(client.RemoteAddr()); source != "" {
					conn.setSource(source)
				}
			}()
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.track(conn, false)
			conn.run(func() (net.Conn, error) { return net.DialTimeout(p.network, p.target, 5*time.Second) })
		}()
	}
}

func (p *faultProxy) track(conn *relayConn, add bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !add {
		delete(p.conns, conn)
		return true
	}
	if p.closed {
		return false
	}
	p.conns[conn] = struct{}{}
	return true
}

// dropAll 关闭当前全部连接（客户端看到连接被对端关闭，如同 MySQL 重启或主从切换），返回关闭的条数。
// 之后的新连接照常转发。
func (p *faultProxy) dropAll() int {
	p.mu.Lock()
	conns := make([]*relayConn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()
	for _, conn := range conns {
		conn.close()
	}
	return len(conns)
}

func (p *faultProxy) connections() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

func (p *faultProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	p.dropAll()
	p.wg.Wait()
}

// ── 识别连接的发起进程 ──

// lsofAvailable 报告本机能否用 lsof 识别连接的发起进程。
func lsofAvailable() bool {
	_, err := exec.LookPath("lsof")
	return err == nil
}

// connectingPID 用 lsof 找出本机上从 remote（发起方的地址）连进来的进程：两端都在本机，lsof 按端口列出
// 两个进程，去掉代理自己就是发起方。找不到时返回 0。
func connectingPID(ctx context.Context, remote net.Addr) int {
	tcp, ok := remote.(*net.TCPAddr)
	if !ok {
		return 0
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "lsof", "-nP", "-iTCP:"+strconv.Itoa(tcp.Port), "-sTCP:ESTABLISHED", "-Fp").Output()
	if err != nil && len(out) == 0 {
		return 0
	}
	return parseLsofPIDs(bytes.NewReader(out), os.Getpid())
}

// parseLsofPIDs 从 lsof -Fp 的输出里取第一个不是 self 的 PID。
func parseLsofPIDs(r io.Reader, self int) int {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 2 || line[0] != 'p' {
			continue
		}
		if pid, err := strconv.Atoi(line[1:]); err == nil && pid != self {
			return pid
		}
	}
	return 0
}
