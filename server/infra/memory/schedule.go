// Package memory 提供领域仓储的内存实现。
package memory

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/samber/mo"
)

type ScheduleRepository struct {
	mu        sync.RWMutex
	schedules map[string]*schedule.Schedule // key = "namespace/id"
}

var _ schedule.Repository = (*ScheduleRepository)(nil)

func NewScheduleRepository() *ScheduleRepository {
	return &ScheduleRepository{
		schedules: make(map[string]*schedule.Schedule),
	}
}

func scheduleKey(namespace, id string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + id
}

// cloneSchedule 返回与 entity 不共享 Input 与时间指针的副本：namespace 归一，待保存变更清空。
// 存入与读出都经过它，调用方改动自己手里的实体不影响仓储里的记录。
func cloneSchedule(entity *schedule.Schedule) *schedule.Schedule {
	clone := *entity
	clone.Namespace = protocol.NormalizeNamespace(entity.Namespace)
	clone.Input = cloneAnyMap(entity.Input)
	clone.LastRunAt = cloneTime(entity.LastRunAt)
	clone.NextRunAt = cloneTime(entity.NextRunAt)
	clone.BufferDropFrom = cloneTime(entity.BufferDropFrom)
	clone.BufferDropUntil = cloneTime(entity.BufferDropUntil)
	clone.ClearPatch()
	return &clone
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	clone := *t
	return &clone
}

func (s *ScheduleRepository) CreateSchedule(entity *schedule.Schedule) error {
	key := scheduleKey(entity.Namespace, entity.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.schedules[key]; exists {
		return fmt.Errorf("%w: %s", schedule.ErrAlreadyExists, key)
	}
	s.schedules[key] = cloneSchedule(entity)
	entity.ClearPatch()
	return nil
}

func (s *ScheduleRepository) GetSchedule(namespace, id string) (*schedule.Schedule, error) {
	key := scheduleKey(namespace, id)
	s.mu.RLock()
	defer s.mu.RUnlock()
	entity, ok := s.schedules[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", schedule.ErrNotFound, key)
	}
	return cloneSchedule(entity), nil
}

// UpdateSchedule 只写入经领域方法改过的字段，与 gorm 仓储的 ToPatch 更新一致。
func (s *ScheduleRepository) UpdateSchedule(entity *schedule.Schedule) error {
	key := scheduleKey(entity.Namespace, entity.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.schedules[key]
	if !ok {
		return fmt.Errorf("%w: %s", schedule.ErrNotFound, key)
	}
	updated := *stored
	applyPatch(&updated, entity, entity.ExportPatch())
	s.schedules[key] = cloneSchedule(&updated)
	entity.ClearPatch()
	return nil
}

func (s *ScheduleRepository) UpdateScheduleNextRunAtCAS(entity *schedule.Schedule, oldNextRunAt *time.Time) (bool, error) {
	key := scheduleKey(entity.Namespace, entity.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.schedules[key]
	if !ok {
		return false, nil
	}
	current := stored.NextRunAt
	if (current == nil) != (oldNextRunAt == nil) || (current != nil && !current.Equal(*oldNextRunAt)) {
		return false, nil
	}
	stored.NextRunAt = cloneTime(entity.NextRunAt)
	stored.UpdatedAt = entity.UpdatedAt
	// CAS 只消费实际写入的两个字段，其余待保存变更继续保留。
	patch := entity.ExportPatch()
	patch.NextRunAt = mo.None[*time.Time]()
	patch.UpdatedAt = mo.None[time.Time]()
	return true, nil
}

func (s *ScheduleRepository) DeleteSchedule(namespace, id string) error {
	key := scheduleKey(namespace, id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.schedules[key]; !ok {
		return fmt.Errorf("%w: %s", schedule.ErrNotFound, key)
	}
	delete(s.schedules, key)
	return nil
}

func (s *ScheduleRepository) ListSchedules(namespace string) ([]*schedule.Schedule, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*schedule.Schedule
	for _, entity := range s.schedules {
		if entity.Namespace == namespace {
			result = append(result, cloneSchedule(entity))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].CreatedAt.Before(result[j].CreatedAt)
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}

func (s *ScheduleRepository) ListDueSchedules(now time.Time) ([]*schedule.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*schedule.Schedule
	for _, entity := range s.schedules {
		if entity.Paused || entity.NextRunAt == nil || entity.NextRunAt.After(now) {
			continue
		}
		result = append(result, cloneSchedule(entity))
	}
	sort.Slice(result, func(i, j int) bool {
		if !result[i].NextRunAt.Equal(*result[j].NextRunAt) {
			return result[i].NextRunAt.Before(*result[j].NextRunAt)
		}
		return result[i].ID < result[j].ID
	})
	return result, nil
}
