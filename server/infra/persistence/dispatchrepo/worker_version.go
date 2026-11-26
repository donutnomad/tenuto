package dispatchrepo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/dispatch"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type activityWorkerVersionKey struct {
	Namespace    string
	ActivityType string
	Identity     string
	BuildID      string
	TaskQueue    string
}

func (s *Repository) UpsertActivityWorkerVersionInNamespace(namespace string, worker protocol.ActivityWorkerVersion, now time.Time, ttl time.Duration) (protocol.ActivityWorkerVersion, error) {
	if len(worker.ActivityTypes) == 0 {
		return protocol.ActivityWorkerVersion{}, protocol.InvalidRequestf("activity worker version has no activity types")
	}
	worker.Namespace = protocol.NormalizeNamespace(namespace)
	saved := worker
	saved.LastHeartbeatAt = now
	saved.ExpiresAt = now.Add(ttl)
	taskQueues := dispatch.WorkerRouteTaskQueues(worker)
	desired := make(map[activityWorkerVersionKey]bool)
	for _, activityType := range worker.ActivityTypes {
		if activityType == "" {
			continue
		}
		for _, taskQueue := range taskQueues {
			desired[activityWorkerVersionKey{Namespace: worker.Namespace, ActivityType: activityType, Identity: worker.Identity, BuildID: worker.BuildID, TaskQueue: taskQueue}] = true
		}
	}
	if len(desired) == 0 {
		return protocol.ActivityWorkerVersion{}, protocol.InvalidRequestf("activity worker version has no activity types")
	}
	contractsJSON, err := json.Marshal(worker.Contracts)
	if err != nil {
		return protocol.ActivityWorkerVersion{}, fmt.Errorf("marshal activity worker contracts: %w", err)
	}
	err = s.db.Transaction(func(tx *gorm.DB) error {
		existing, err := infradb.Find(tx, gsql.From[ActivityWorkerVersionPO](ActivityWorkerVersionSchema).Where(
			ActivityWorkerVersionSchema.Namespace.Eq(worker.Namespace),
			ActivityWorkerVersionSchema.Identity.Eq(worker.Identity),
			ActivityWorkerVersionSchema.BuildID.Eq(worker.BuildID),
		))
		if err != nil {
			return err
		}
		for _, row := range existing {
			key := activityWorkerVersionKey{
				Namespace:    row.Namespace,
				ActivityType: row.ActivityType,
				Identity:     row.Identity,
				BuildID:      row.BuildID,
				TaskQueue:    row.TaskQueue,
			}
			if desired[key] {
				continue
			}
			if err := gsql.From[ActivityWorkerVersionPO](ActivityWorkerVersionSchema).Where(
				ActivityWorkerVersionSchema.Namespace.Eq(row.Namespace),
				ActivityWorkerVersionSchema.ActivityType.Eq(row.ActivityType),
				ActivityWorkerVersionSchema.Identity.Eq(row.Identity),
				ActivityWorkerVersionSchema.BuildID.Eq(row.BuildID),
				ActivityWorkerVersionSchema.TaskQueue.Eq(row.TaskQueue),
			).Delete(tx).Error; err != nil {
				return err
			}
		}
		for _, activityType := range worker.ActivityTypes {
			if activityType == "" {
				continue
			}
			for _, taskQueue := range taskQueues {
				row := &ActivityWorkerVersionPO{
					Namespace:       worker.Namespace,
					ActivityType:    activityType,
					App:             worker.App,
					Identity:        worker.Identity,
					BuildID:         worker.BuildID,
					TaskQueue:       taskQueue,
					IsDefault:       worker.Default,
					Weight:          worker.Weight,
					HeartbeatTTL:    worker.HeartbeatTTL,
					ContractsJSON:   contractsJSON,
					LastHeartbeatAt: saved.LastHeartbeatAt,
					ExpiresAt:       saved.ExpiresAt,
					CreatedAt:       now,
					UpdatedAt:       now,
				}
				// 撞主键 (namespace, activity_type, identity, build_id, task_queue) 时改写路由属性与心跳时间。
				if err := tx.Clauses(clause.OnConflict{
					Columns: []clause.Column{
						{Name: "namespace"},
						{Name: "activity_type"},
						{Name: "identity"},
						{Name: "build_id"},
						{Name: "task_queue"},
					},
					DoUpdates: clause.AssignmentColumns([]string{
						"app",
						"is_default",
						"weight",
						"heartbeat_ttl",
						"contracts_json",
						"last_heartbeat_at",
						"expires_at",
						"updated_at",
					}),
				}).Create(row).Error; err != nil {
					return err
				}
			}
		}
		return nil
	})
	return saved, err
}

func (s *Repository) HeartbeatActivityWorkerVersionInNamespace(namespace, identity, buildID string, now time.Time, ttl time.Duration) error {
	namespace = protocol.NormalizeNamespace(namespace)
	if identity == "" {
		return protocol.InvalidRequestf("activity worker identity is empty")
	}
	// 同一 worker（namespace/identity/build）的全部行由一次注册写入，heartbeat_ttl 相同：
	// 读出 TTL 的去重值，再按 TTL 整批续期，正常情况下是一条 SELECT 加一条 UPDATE。
	worker := []gsql.Expression{
		ActivityWorkerVersionSchema.Namespace.Eq(namespace),
		ActivityWorkerVersionSchema.Identity.Eq(identity),
		ActivityWorkerVersionSchema.BuildID.Eq(buildID),
	}
	return s.db.Transaction(func(tx *gorm.DB) error {
		ttls, err := infradb.Find(tx, gsql.PluckG[string](ActivityWorkerVersionSchema.HeartbeatTTL).
			From(ActivityWorkerVersionSchema).
			Where(worker...).
			Distinct())
		if err != nil {
			return err
		}
		if len(ttls) == 0 {
			return protocol.InvalidRequestf("activity worker version %s/%s/%s is not registered", namespace, identity, buildID)
		}
		for _, rowTTL := range ttls {
			effectiveTTL, err := dispatch.ResolveHeartbeatTTL(*rowTTL, ttl)
			if err != nil {
				return err
			}
			if err := gsql.From[ActivityWorkerVersionPO](ActivityWorkerVersionSchema).
				Where(worker...).
				Where(ActivityWorkerVersionSchema.HeartbeatTTL.Eq(*rowTTL)).
				Update(tx, map[string]any{
					"last_heartbeat_at": now,
					"expires_at":        now.Add(effectiveTTL),
					"updated_at":        now,
				}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Repository) DeregisterActivityWorkerVersionInNamespace(namespace, identity, buildID string, now time.Time) error {
	namespace = protocol.NormalizeNamespace(namespace)
	if identity == "" {
		return protocol.InvalidRequestf("activity worker identity is empty")
	}
	return gsql.From[ActivityWorkerVersionPO](ActivityWorkerVersionSchema).
		Where(
			ActivityWorkerVersionSchema.Namespace.Eq(namespace),
			ActivityWorkerVersionSchema.Identity.Eq(identity),
			ActivityWorkerVersionSchema.BuildID.Eq(buildID),
		).
		Update(s.db, map[string]any{
			"expires_at": now,
			"updated_at": now,
		}).Error
}

func (s *Repository) DeleteExpiredActivityWorkerVersions(before time.Time) (int64, error) {
	result := gsql.From[ActivityWorkerVersionPO](ActivityWorkerVersionSchema).
		Where(ActivityWorkerVersionSchema.ExpiresAt.Lt(before)).
		Delete(s.db)
	return result.RowsAffected, result.Error
}

func (s *Repository) ListActiveActivityWorkerVersions(now time.Time) ([]protocol.ActivityWorkerVersion, error) {
	return s.listRoutes(ActivityWorkerVersionSchema.ExpiresAt.Gt(now))
}

func (s *Repository) ListActivityWorkerVersionsOfWorker(namespace, identity string) ([]protocol.ActivityWorkerVersion, error) {
	return s.listRoutes(
		ActivityWorkerVersionSchema.Namespace.Eq(protocol.NormalizeNamespace(namespace)),
		ActivityWorkerVersionSchema.Identity.Eq(identity),
	)
}

func (s *Repository) listRoutes(conditions ...gsql.Expression) ([]protocol.ActivityWorkerVersion, error) {
	rows, err := infradb.Find(s.db, gsql.From[ActivityWorkerVersionPO](ActivityWorkerVersionSchema).
		Where(conditions...).
		Order(ActivityWorkerVersionSchema.Namespace).
		Order(ActivityWorkerVersionSchema.ActivityType).
		Order(ActivityWorkerVersionSchema.BuildID).
		Order(ActivityWorkerVersionSchema.TaskQueue).
		Order(ActivityWorkerVersionSchema.Identity))
	if err != nil {
		return nil, err
	}
	workers := make([]protocol.ActivityWorkerVersion, 0, len(rows))
	for _, row := range rows {
		worker, err := activityWorkerVersionRowToWorker(row)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}
	return workers, nil
}
