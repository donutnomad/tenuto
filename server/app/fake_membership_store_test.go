package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/taskq/membership"
)

// fakeMemberStore 必须能直接接进 Monitor：调用方只需要两个方法即可替换服务发现后端。
func TestFakeMemberStoreSatisfiesMemberStore(t *testing.T) {
	var store membership.MemberStore = &fakeMemberStore{}
	ctx := t.Context()
	if err := store.UpsertMember(ctx, membership.UpsertMemberRequest{ServiceName: "matching"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetActiveMembers(ctx, 0); err != nil {
		t.Fatal(err)
	}
}

// fakeMemberStore 冒充调用方自带的服务发现后端（如 nacos 的适配器），
// 只实现 membership.MemberStore 的两个方法，并记录被调用的次数。
// 生产代码不使用；作为导出类型供同包测试与外部包测试共用。
type fakeMemberStore struct {
	mu          sync.Mutex
	members     []membership.MemberRecord
	getCalls    int
	upsertCalls int
}

func (f *fakeMemberStore) UpsertMember(context.Context, membership.UpsertMemberRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.upsertCalls++
	return nil
}

// SetMembers 替换 GetActiveMembers 返回的成员。
func (f *fakeMemberStore) SetMembers(members []membership.MemberRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = members
}

func (f *fakeMemberStore) GetActiveMembers(context.Context, time.Duration) ([]membership.MemberRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return f.members, nil
}

// Counts 返回 GetActiveMembers 与 UpsertMember 的调用次数。
func (f *fakeMemberStore) Counts() (get, upsert int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getCalls, f.upsertCalls
}
