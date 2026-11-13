package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParseFaultSchedule(t *testing.T) {
	steps, err := parseFaultSchedule("30s:pause-replica:5s, 10s:kill-leader:15s,20s:kill-worker,"+
		"40s:db-drop:all,50s:db-blackhole:replica-1:5s,60s:db-latency:leader:80:20s,70s:partition:2:10s,80s:isolate:random:10s,90s:api-blackhole:0:3s", 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []faultStep{
		{At: 10 * time.Second, Action: actionKillLeader, Down: 15 * time.Second},
		{At: 20 * time.Second, Action: actionKillWorker, Down: 10 * time.Second},
		{At: 30 * time.Second, Action: actionPauseReplica, Down: 5 * time.Second},
		{At: 40 * time.Second, Action: actionDBDrop, Target: "all"},
		{At: 50 * time.Second, Action: actionDBBlackhole, Target: "replica-1", Down: 5 * time.Second},
		{At: 60 * time.Second, Action: actionDBLatency, Target: "leader", Latency: 80 * time.Millisecond, Down: 20 * time.Second},
		{At: 70 * time.Second, Action: actionPartition, Target: "2", Down: 10 * time.Second},
		{At: 80 * time.Second, Action: actionIsolate, Target: "random", Down: 10 * time.Second},
		{At: 90 * time.Second, Action: actionAPIBlackhole, Target: "0", Down: 3 * time.Second},
	}
	if len(steps) != len(want) {
		t.Fatalf("steps = %+v", steps)
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("step %d = %+v, want %+v", i, steps[i], want[i])
		}
	}
	if got := steps[5].String(); got != "db-latency:leader:80:20s" {
		t.Fatalf("String() = %q", got)
	}
	if steps, err := parseFaultSchedule("", 3); err != nil || len(steps) != 0 {
		t.Fatalf("empty schedule = %v, %v", steps, err)
	}
	for _, bad := range []string{
		"10s", "10s:reboot", "x:kill-worker", "10s:kill-worker:0s", "10s:kill-worker:5s:1", "-1s:kill-worker",
		"10s:db-drop", "10s:db-drop:all:5s", "10s:db-blackhole:all", "10s:db-latency:all:0:5s", "10s:db-latency:all:x:5s",
		"10s:partition:all:5s", "10s:isolate:3:5s", "10s:api-blackhole:replica-x:5s", "10s:db-blackhole:-1:5s",
	} {
		if _, err := parseFaultSchedule(bad, 3); err == nil {
			t.Errorf("parseFaultSchedule(%q) accepted", bad)
		}
	}
}

func TestParseFaultSet(t *testing.T) {
	steps, err := parseFaultSet(defaultSoakFaults, 3)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, step := range steps {
		if step.At != 0 {
			t.Errorf("%s has a time", step)
		}
		seen[step.Action] = true
	}
	for _, action := range []string{actionKillLeader, actionKillReplica, actionKillWorker, actionPauseReplica, actionDBDrop,
		actionDBBlackhole, actionDBLatency, actionPartition, actionIsolate, actionAPIBlackhole} {
		if !seen[action] {
			t.Errorf("default soak set misses %s", action)
		}
	}
	if _, err := parseFaultSet(" , ", 3); err == nil {
		t.Error("an empty fault set was accepted")
	}
	if _, err := parseFaultSet("10s:kill-worker", 3); err == nil {
		t.Error("a fault with a time was accepted in a fault set")
	}
}

// 默认计划覆盖每种进程故障至少一次。
func TestDefaultFaultsCoverEveryAction(t *testing.T) {
	steps, err := parseFaultSchedule(defaultFaults, 3)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, step := range steps {
		seen[step.Action] = true
	}
	for _, action := range []string{actionKillLeader, actionKillReplica, actionKillWorker, actionPauseReplica} {
		if !seen[action] {
			t.Errorf("default schedule misses %s", action)
		}
	}
	for _, action := range []string{actionKillLeader + " (leader unavailable)", actionPauseReplica, "db-drop (3 connections)", actionPartition, actionIsolate, actionAPIBlackhole, actionUpgradeStop} {
		if !isInjectedFault(action) {
			t.Errorf("isInjectedFault(%q) = false", action)
		}
	}
	for _, action := range []string{actionRestart, actionResume, actionHeal, actionUpgradeStart, "migrate"} {
		if isInjectedFault(action) {
			t.Errorf("isInjectedFault(%q) = true", action)
		}
	}
}

// 网络故障：规则装到对应的代理上；目标被占用期间不能再注入；恢复后报告故障结束；db-drop 关掉现有连接。
func TestInjectorNetworkFaults(t *testing.T) {
	c := testCluster(t, 3)
	var ended []faultEnd
	endedCh := make(chan faultEnd, 8)
	in := newInjector(c, 1)
	in.ended = func(end faultEnd) { endedCh <- end }
	ctx := context.Background()

	if err := in.apply(ctx, faultStep{Action: actionPartition, Target: "1", Down: 150 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if stall, _ := c.peer[1].state("", now); !stall {
		t.Error("partition: unidentified peer reaches replica-1")
	}
	if stall, _ := c.peer[1].state("replica-1", now); stall {
		t.Error("partition: replica-1 cannot reach itself")
	}
	if stall, _ := c.peer[0].state("replica-1", now); !stall {
		t.Error("partition: replica-1 reaches replica-0")
	}
	if stall, _ := c.peer[0].state("replica-2", now); stall {
		t.Error("partition: replica-2 cannot reach replica-0")
	}
	if stall, _ := c.dbProxy[1].state("", now); stall {
		t.Error("partition stalls the database")
	}
	if err := in.apply(ctx, faultStep{Action: actionIsolate, Target: "replica-1", Down: time.Second}); err == nil {
		t.Error("a second fault on a busy replica was accepted")
	}
	if err := in.apply(ctx, faultStep{Action: actionDBLatency, Target: "all", Latency: 30 * time.Millisecond, Down: 150 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, latency := c.dbProxy[i].state("", time.Now()); latency != 30*time.Millisecond {
			t.Errorf("db proxy %d latency = %s", i, latency)
		}
	}
	if err := in.apply(ctx, faultStep{Action: actionAPIBlackhole, Target: "2", Down: 150 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if stall, _ := c.lb.state(replicaName(2), time.Now()); !stall {
		t.Error("api-blackhole does not stall replica-2's backend")
	}
	if stall, _ := c.lb.state(replicaName(0), time.Now()); stall {
		t.Error("api-blackhole stalls replica-0's backend")
	}
	for range 3 {
		select {
		case end := <-endedCh:
			ended = append(ended, end)
		case <-time.After(5 * time.Second):
			t.Fatalf("faults did not end: %+v", ended)
		}
	}
	in.wait()
	if names := in.busyNames(); len(names) != 0 {
		t.Fatalf("busy after recovery: %v", names)
	}
	if stall, _ := c.peer[0].state("replica-1", time.Now()); stall {
		t.Error("partition outlived its duration")
	}
	if err := in.apply(ctx, faultStep{Action: actionIsolate, Target: "1", Down: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if stall, _ := c.dbProxy[1].state("", time.Now()); !stall {
		t.Error("isolate does not stall replica-1's database")
	}
	<-endedCh
	in.wait()

	if err := in.apply(ctx, faultStep{Action: actionDBDrop, Target: "all"}); err != nil {
		t.Fatal(err)
	}
	if end := <-endedCh; end.Action != actionDBDrop || !end.Start.Equal(end.End) {
		t.Fatalf("db-drop end = %+v", end)
	}
	timeline := c.faultLog()
	joined := ""
	for _, event := range timeline {
		joined += event.Action + " " + event.Target + "\n"
	}
	for _, want := range []string{"partition replica-1", "db-latency 30ms all replicas", "api-blackhole replica-2", "heal replica-1", "isolate replica-1", "db-drop (0 connections) all replicas"} {
		if !strings.Contains(joined, want) {
			t.Errorf("timeline misses %q:\n%s", want, joined)
		}
	}
}

// 没有代理时拒绝经代理的故障。
func TestInjectorNeedsProxies(t *testing.T) {
	c := testCluster(t, 2)
	c.dbProxy, c.peer = nil, nil
	if err := newInjector(c, 1).apply(context.Background(), faultStep{Action: actionDBDrop, Target: "all"}); err == nil || !strings.Contains(err.Error(), "-proxies") {
		t.Fatalf("err = %v", err)
	}
}
