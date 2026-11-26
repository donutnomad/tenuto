package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/protocol"
)

// registryStub 记录登记与心跳；heartbeatErrs 依次给出前几次心跳的错误，registerErrs 依次给出第一次心跳之后
// 前几次登记的错误。
type registryStub struct {
	mu            sync.Mutex
	registers     int
	heartbeats    int
	registerErrs  []error
	heartbeatErrs []error
	reregistered  chan struct{}
}

func (r *registryStub) RegisterActivityWorkerVersion(context.Context, protocol.ActivityWorkerVersion) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registers++
	if r.heartbeats > 0 && len(r.registerErrs) > 0 {
		err := r.registerErrs[0]
		r.registerErrs = r.registerErrs[1:]
		return err
	}
	if r.heartbeats > 0 {
		select {
		case r.reregistered <- struct{}{}:
		default:
		}
	}
	return nil
}

func (r *registryStub) HeartbeatActivityWorkerVersionInNamespace(context.Context, string, string, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.heartbeats++
	if len(r.heartbeatErrs) > 0 {
		err := r.heartbeatErrs[0]
		r.heartbeatErrs = r.heartbeatErrs[1:]
		return err
	}
	return nil
}

func (r *registryStub) DeregisterActivityWorkerVersionInNamespace(context.Context, string, string, string) error {
	return nil
}

// 心跳得到「未登记」（登记在长时间失联后被 Server 清理）时 Worker 重新登记，登记失败按退避重试直到成功；
// 其他心跳错误只记日志，不重新登记。
func TestActivityWorkerVersionRegistryRegistersAgainWhenHeartbeatFindsNoRegistration(t *testing.T) {
	registry := &registryStub{
		heartbeatErrs: []error{
			errors.New("connection refused"),
			fmt.Errorf("%w: default/pod-1/v1", protocol.ErrActivityWorkerVersionNotRegistered),
		},
		// 重新登记先失败两次，退避后成功。
		registerErrs: []error{errors.New("503"), errors.New("503")},
		reregistered: make(chan struct{}, 1),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunActivityWorkerVersionRegistry(ctx, slog.New(slog.DiscardHandler), protocol.ActivityWorkerVersion{Identity: "pod-1", BuildID: "v1"},
			registry, 10*time.Millisecond, func(err error) { ready <- err })
	}()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	select {
	case <-registry.reregistered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not register again after the heartbeat found no registration")
	}
	cancel()
	<-done
	registry.mu.Lock()
	defer registry.mu.Unlock()
	// 初次登记 1 次，重新登记失败 2 次、成功 1 次；连接错误那次心跳不触发登记。
	if registry.registers != 4 {
		t.Fatalf("registers = %d, want 4", registry.registers)
	}
}
