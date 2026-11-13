package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// 故障动作。进程故障：kill-* 是 kill -9，pause-replica 是 SIGSTOP，停机时长过后重启（resume 发 SIGCONT）。
// 网络故障经进程内代理注入（需要 -proxies）：db-* 作用于副本到 MySQL 的连接，partition 隔断副本之间的
// matching 流量，isolate 同时隔断两者，api-blackhole 让负载均衡器转给这个副本的 HTTP 流量停滞。
const (
	actionKillLeader   = "kill-leader"
	actionKillReplica  = "kill-replica"
	actionKillWorker   = "kill-worker"
	actionPauseReplica = "pause-replica"
	actionDBDrop       = "db-drop"
	actionDBBlackhole  = "db-blackhole"
	actionDBLatency    = "db-latency"
	actionPartition    = "partition"
	actionIsolate      = "isolate"
	actionAPIBlackhole = "api-blackhole"

	actionRestart      = "restart"
	actionResume       = "resume"
	actionHeal         = "heal"
	actionUpgradeStop  = "upgrade-stop"
	actionUpgradeStart = "upgrade-start"
)

// 目标：随机副本、reconciler leader、全部副本（只用于 db-*）。另可写副本序号或 replica-<i>。
const (
	targetRandom = "random"
	targetLeader = "leader"
	targetAll    = "all"
)

// defaultFaults 覆盖每种进程故障至少一次，落在 failover 默认负载的运行窗口内。
const defaultFaults = "10s:kill-leader:10s,20s:kill-worker:5s,30s:pause-replica:10s,45s:kill-replica:10s"

// defaultSoakFaults 是 soak 缺省的故障集合：每种故障一项。
const defaultSoakFaults = "kill-leader:10s,kill-replica:10s,kill-worker:5s,pause-replica:10s," +
	"db-drop:random,db-drop:all,db-blackhole:random:20s,db-latency:all:50:30s," +
	"partition:random:20s,isolate:random:20s,api-blackhole:random:20s"

// faultStep 是一次故障：开始后 At 执行 Action，Down 之后恢复。Target 是网络故障的目标；Latency 只用于 db-latency。
type faultStep struct {
	At      time.Duration
	Action  string
	Down    time.Duration
	Target  string
	Latency time.Duration
}

func (s faultStep) String() string {
	parts := []string{s.Action}
	if s.Target != "" {
		parts = append(parts, s.Target)
	}
	if s.Latency > 0 {
		parts = append(parts, strconv.FormatInt(s.Latency.Milliseconds(), 10))
	}
	if s.Down > 0 {
		parts = append(parts, s.Down.String())
	}
	return strings.Join(parts, ":")
}

// parseFaultSchedule 解析 failover 的故障计划 "at:fault,..."，例如 "10s:kill-leader:15s,20s:db-blackhole:all:5s"；
// fault 的写法见 parseFault。空串表示不注入故障。结果按 At 排序。
func parseFaultSchedule(spec string, replicas int) ([]faultStep, error) {
	var steps []faultStep
	for item := range strings.SplitSeq(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		atText, rest, ok := strings.Cut(item, ":")
		if !ok {
			return nil, fmt.Errorf("fault %q: want at:action[:args]", item)
		}
		at, err := time.ParseDuration(atText)
		if err != nil || at < 0 {
			return nil, fmt.Errorf("fault %q: bad time %q", item, atText)
		}
		step, err := parseFault(rest, replicas)
		if err != nil {
			return nil, fmt.Errorf("fault %q: %w", item, err)
		}
		step.At = at
		steps = append(steps, step)
	}
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].At < steps[j].At })
	return steps, nil
}

// parseFaultSet 解析 soak 的故障集合 "fault,..."（没有 at）。集合不能为空。
func parseFaultSet(spec string, replicas int) ([]faultStep, error) {
	var steps []faultStep
	for item := range strings.SplitSeq(spec, ",") {
		if item = strings.TrimSpace(item); item == "" {
			continue
		}
		step, err := parseFault(item, replicas)
		if err != nil {
			return nil, fmt.Errorf("fault %q: %w", item, err)
		}
		steps = append(steps, step)
	}
	if len(steps) == 0 {
		return nil, errors.New("the fault set is empty")
	}
	return steps, nil
}

// parseFault 解析一个故障：
//
//	kill-leader|kill-replica|kill-worker|pause-replica[:down]   down 缺省 10s
//	db-drop:<target>
//	db-blackhole:<target>:<down>
//	db-latency:<target>:<ms>:<down>
//	partition|isolate|api-blackhole:<replica>:<down>
//
// target 是 random、leader、all、副本序号或 replica-<i>；replica 同 target 但不能是 all。
func parseFault(spec string, replicas int) (faultStep, error) {
	parts := strings.Split(spec, ":")
	step := faultStep{Action: parts[0]}
	args := parts[1:]
	duration := func(text string) (time.Duration, error) {
		d, err := time.ParseDuration(text)
		if err != nil || d <= 0 {
			return 0, fmt.Errorf("bad duration %q", text)
		}
		return d, nil
	}
	var err error
	switch step.Action {
	case actionKillLeader, actionKillReplica, actionKillWorker, actionPauseReplica:
		step.Down = 10 * time.Second
		switch len(args) {
		case 0:
		case 1:
			if step.Down, err = duration(args[0]); err != nil {
				return step, err
			}
		default:
			return step, fmt.Errorf("want %s[:down]", step.Action)
		}
		return step, nil
	case actionDBDrop:
		if len(args) != 1 {
			return step, errors.New("want db-drop:<target>")
		}
	case actionDBBlackhole, actionPartition, actionIsolate, actionAPIBlackhole:
		if len(args) != 2 {
			return step, fmt.Errorf("want %s:<target>:<duration>", step.Action)
		}
		if step.Down, err = duration(args[1]); err != nil {
			return step, err
		}
	case actionDBLatency:
		if len(args) != 3 {
			return step, errors.New("want db-latency:<target>:<ms>:<duration>")
		}
		ms, convErr := strconv.Atoi(args[1])
		if convErr != nil || ms <= 0 {
			return step, fmt.Errorf("bad latency %q (milliseconds)", args[1])
		}
		step.Latency = time.Duration(ms) * time.Millisecond
		if step.Down, err = duration(args[2]); err != nil {
			return step, err
		}
	default:
		return step, fmt.Errorf("unknown action %q (kill-leader, kill-replica, kill-worker, pause-replica, db-drop, db-blackhole, db-latency, partition, isolate, api-blackhole)", step.Action)
	}
	step.Target = args[0]
	allowAll := strings.HasPrefix(step.Action, "db-")
	if _, err := parseTarget(step.Target, replicas, allowAll); err != nil {
		return step, err
	}
	return step, nil
}

// parseTarget 返回固定的副本序号；random、leader、all 返回 -1。
func parseTarget(target string, replicas int, allowAll bool) (int, error) {
	switch target {
	case targetRandom, targetLeader:
		return -1, nil
	case targetAll:
		if !allowAll {
			return 0, errors.New("target all is only valid for db-* faults")
		}
		return -1, nil
	}
	i, err := strconv.Atoi(strings.TrimPrefix(target, "replica-"))
	if err != nil || i < 0 || i >= replicas {
		return 0, fmt.Errorf("bad target %q (random, leader, all, or a replica index below %d)", target, replicas)
	}
	return i, nil
}

// needsProxies 报告故障是否要经代理注入。
func (s faultStep) needsProxies() bool {
	switch s.Action {
	case actionDBDrop, actionDBBlackhole, actionDBLatency, actionPartition, actionIsolate:
		return true
	}
	return false
}

// isInjectedFault 报告时间线里的动作是不是一次故障（而不是恢复）。
func isInjectedFault(action string) bool {
	name, _, _ := strings.Cut(action, " ")
	switch name {
	case actionKillLeader, actionKillReplica, actionKillWorker, actionPauseReplica, actionDBDrop, actionDBBlackhole,
		actionDBLatency, actionPartition, actionIsolate, actionAPIBlackhole, actionUpgradeStop:
		return true
	}
	return false
}

// faultEnd 是一次故障结束（恢复完成）。
type faultEnd struct {
	Action string
	Target string
	Start  time.Time
	End    time.Time
}

// injector 注入故障。一个进程同一时刻至多处于一次故障中（target all 的 db-* 故障不占用副本）。
type injector struct {
	cluster *cluster
	rng     *rand.Rand
	// ended 非 nil 时在每次故障恢复后调用（db-drop 立即调用）。
	ended func(faultEnd)
	mu    sync.Mutex
	busy  map[string]bool
	wg    sync.WaitGroup
}

func newInjector(c *cluster, seed uint64) *injector {
	return &injector{cluster: c, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), busy: map[string]bool{}}
}

// run 从 start 起按计划执行，等全部故障恢复后返回。
func (in *injector) run(ctx context.Context, start time.Time, steps []faultStep) {
	for _, step := range steps {
		if err := sleepCtx(ctx, time.Until(start.Add(step.At))); err != nil {
			break
		}
		if err := in.apply(ctx, step); err != nil {
			fmt.Printf("%s fault %s skipped: %v\n", time.Now().Format("15:04:05.000"), step, err)
		}
	}
	in.wait()
}

func (in *injector) wait() { in.wg.Wait() }

// randomInt 在 [0, n) 里取一个数（rng 不是并发安全的）。
func (in *injector) randomInt(n int) int {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.rng.IntN(n)
}

func (in *injector) apply(ctx context.Context, step faultStep) error {
	c := in.cluster
	if step.needsProxies() && len(c.dbProxy) == 0 {
		return fmt.Errorf("%s needs -proxies", step.Action)
	}
	switch step.Action {
	case actionKillWorker:
		i, err := in.pick(c.workers)
		if err != nil {
			return err
		}
		worker := c.workers[i]
		start := c.recordFault(step.Action, worker.name)
		if err := worker.kill(); err != nil {
			in.release(worker.name)
			return err
		}
		in.after(ctx, step, worker.name, worker.name, start, func() error {
			c.recordFault(actionRestart, worker.name)
			return c.startWorker(ctx, i)
		})
		return nil
	case actionKillLeader, actionKillReplica, actionPauseReplica:
		i, note, err := in.pickReplica(ctx, step.Action == actionKillLeader)
		if err != nil {
			return err
		}
		replica := c.replicas[i]
		c.lb.setUp(i, false)
		start := c.recordFault(step.Action+note, replica.name)
		if step.Action == actionPauseReplica {
			if err := replica.signal(syscall.SIGSTOP); err != nil {
				in.release(replica.name)
				return err
			}
			in.after(ctx, step, replica.name, replica.name, start, func() error {
				c.recordFault(actionResume, replica.name)
				if err := replica.signal(syscall.SIGCONT); err != nil {
					return err
				}
				c.lb.setUp(i, true)
				return nil
			})
			return nil
		}
		if err := replica.kill(); err != nil {
			in.release(replica.name)
			return err
		}
		in.after(ctx, step, replica.name, replica.name, start, func() error {
			c.recordFault(actionRestart, replica.name)
			return c.startReplica(ctx, i)
		})
		return nil
	}
	return in.applyNetwork(ctx, step)
}

// applyNetwork 注入经代理的故障。规则带截止时间，到期自动失效；after 只记录恢复并释放目标。
func (in *injector) applyNetwork(ctx context.Context, step faultStep) error {
	c := in.cluster
	targets, claimed, note, err := in.resolveTargets(ctx, step.Target, strings.HasPrefix(step.Action, "db-"))
	if err != nil {
		return err
	}
	label := targetLabel(targets, len(c.replicas))
	until := time.Now().Add(step.Down)
	action := step.Action + note
	switch step.Action {
	case actionDBDrop:
		dropped := 0
		for _, i := range targets {
			dropped += c.dbProxy[i].dropAll()
		}
		start := c.recordFault(fmt.Sprintf("%s (%d connections)%s", step.Action, dropped, note), label)
		in.release(claimed)
		if in.ended != nil {
			in.ended(faultEnd{Action: step.Action, Target: label, Start: start, End: start})
		}
		return nil
	case actionDBBlackhole, actionDBLatency:
		rule := netRule{Until: until, Stall: step.Action == actionDBBlackhole, Latency: step.Latency}
		for _, i := range targets {
			c.dbProxy[i].add(rule)
		}
		if step.Latency > 0 {
			action = fmt.Sprintf("%s %dms%s", step.Action, step.Latency.Milliseconds(), note)
		}
	case actionPartition, actionIsolate:
		i := targets[0]
		c.partition(i, until)
		if step.Action == actionIsolate {
			c.dbProxy[i].add(netRule{Until: until, Stall: true})
		}
	case actionAPIBlackhole:
		c.lb.add(netRule{Until: until, Stall: true, From: replicaName(targets[0])})
	}
	start := c.recordFault(action, label)
	in.after(ctx, step, claimed, label, start, func() error {
		c.recordFault(actionHeal, label)
		return nil
	})
	return nil
}

// partition 让副本 i 与其他副本之间的 matching 流量停滞到 until：进入 i 的 matching 代理、不是来自 i 自己的
// 连接，以及来自 i、进入其他副本 matching 代理的连接（需要识别发起方）。i 连自己不受影响。
func (c *cluster) partition(i int, until time.Time) {
	name := replicaName(i)
	for j, proxy := range c.peer {
		if j == i {
			proxy.add(netRule{Until: until, Stall: true, NotFrom: name})
		} else {
			proxy.add(netRule{Until: until, Stall: true, From: name})
		}
	}
}

// resolveTargets 把目标解析成副本序号。随机、leader 与固定序号的目标占用这个副本（claimed 是占用的名字），
// all 不占用。
func (in *injector) resolveTargets(ctx context.Context, target string, allowAll bool) (targets []int, claimed, note string, err error) {
	c := in.cluster
	switch target {
	case targetAll:
		if !allowAll {
			return nil, "", "", errors.New("target all is only valid for db-* faults")
		}
		for i := range c.replicas {
			targets = append(targets, i)
		}
		return targets, "", "", nil
	case targetRandom, targetLeader:
		i, note, err := in.pickReplica(ctx, target == targetLeader)
		if err != nil {
			return nil, "", "", err
		}
		return []int{i}, c.replicas[i].name, note, nil
	}
	i, err := parseTarget(target, len(c.replicas), allowAll)
	if err != nil {
		return nil, "", "", err
	}
	if !in.claim(c.replicas[i]) {
		return nil, "", "", fmt.Errorf("%s is not running or already in a fault", c.replicas[i].name)
	}
	return []int{i}, c.replicas[i].name, "", nil
}

func targetLabel(targets []int, replicas int) string {
	if len(targets) == replicas && replicas > 1 {
		return "all replicas"
	}
	names := make([]string, len(targets))
	for k, i := range targets {
		names[k] = replicaName(i)
	}
	return strings.Join(names, ",")
}

// pickReplica 选一个可以注入故障的副本。leader 为 true 时选 workflow:reconciler 锁的持有者，
// 查不到（锁刚过期、持有者正在故障中）时退回随机副本，note 说明这一点。
func (in *injector) pickReplica(ctx context.Context, leader bool) (int, string, error) {
	c := in.cluster
	if leader {
		nodeID, err := c.db.reconcilerLeader(ctx)
		for i := range c.replicas {
			if err == nil && nodeID == replicaNodeID(i) && in.claim(c.replicas[i]) {
				return i, "", nil
			}
		}
		i, pickErr := in.pick(c.replicas)
		if pickErr != nil {
			return 0, "", pickErr
		}
		return i, fmt.Sprintf(" (leader %q unavailable, random replica)", nodeID), nil
	}
	i, err := in.pick(c.replicas)
	return i, "", err
}

// pick 随机选一个正在运行、没有处于故障中的进程并占用它。
func (in *injector) pick(processes []*process) (int, error) {
	in.mu.Lock()
	var candidates []int
	for i, p := range processes {
		if !in.busy[p.name] && p.running() {
			candidates = append(candidates, i)
		}
	}
	in.mu.Unlock()
	if len(candidates) == 0 {
		return 0, fmt.Errorf("no running process is free for a fault")
	}
	i := candidates[in.randomInt(len(candidates))]
	if !in.claim(processes[i]) {
		return 0, fmt.Errorf("%s is already in a fault", processes[i].name)
	}
	return i, nil
}

func (in *injector) claim(p *process) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.busy[p.name] || !p.running() {
		return false
	}
	in.busy[p.name] = true
	return true
}

func (in *injector) release(name string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	delete(in.busy, name)
}

// inFault 报告进程是否处于一次故障中（被 kill 后等待重启也算）。
func (in *injector) inFault(name string) bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.busy[name]
}

// busyNames 返回处于故障中的进程名。
func (in *injector) busyNames() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	names := make([]string, 0, len(in.busy))
	for name := range in.busy {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// after 在 step.Down 之后执行恢复，然后释放 name 并报告 label 上的故障结束。ctx 取消（Ctrl-C）时不再恢复：收尾的
// shutdown 停止仍在运行的进程，暂停中的进程先收到 SIGCONT。
func (in *injector) after(ctx context.Context, step faultStep, name, label string, start time.Time, restore func() error) {
	in.wg.Add(1)
	go func() {
		defer in.wg.Done()
		defer in.release(name)
		if sleepCtx(ctx, step.Down) != nil {
			return
		}
		if err := restore(); err != nil {
			fmt.Printf("%s restore %s failed: %v\n", time.Now().Format("15:04:05.000"), name, err)
		}
		if in.ended != nil {
			in.ended(faultEnd{Action: step.Action, Target: label, Start: start, End: time.Now()})
		}
	}()
}
