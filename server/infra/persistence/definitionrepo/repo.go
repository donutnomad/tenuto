package definitionrepo

import (
	"fmt"
	"sync"
	"time"

	"github.com/donutnomad/gsql"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
	infradb "github.com/donutnomad/tenuto/server/infra/db"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository 是配置内容版本的 gorm 仓储。
type Repository struct {
	db infradb.DB
	// contents 缓存解码后的内容版本。内容版本的 version 由内容计算（program.WorkflowConfigVersion），行写入后
	// 不再修改、也没有删除路径：同一个 (namespace, workflow_type, version) 永远对应同一份内容，缓存不会过期。
	// 缓存的配置由调用方共享、只读（见 definition.ConfigRepository）。latest/active 指针会变，不缓存。
	contents configContents
}

var _ definition.ConfigRepository = (*Repository)(nil)

func New(database infradb.DB) *Repository {
	return &Repository{db: database}
}

// configContentsLimit 是缓存的内容版本数上限；满了以后按写入顺序淘汰最早的一个。
const configContentsLimit = 256

type configContents struct {
	mu      sync.Mutex
	entries map[definition.WorkflowConfigRef]*program.WorkflowConfig
	order   []definition.WorkflowConfigRef
}

func (c *configContents) get(ref definition.WorkflowConfigRef) (*program.WorkflowConfig, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.entries[ref]
	return data, ok
}

func (c *configContents) put(ref definition.WorkflowConfigRef, wf *program.WorkflowConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[definition.WorkflowConfigRef]*program.WorkflowConfig)
	}
	if _, ok := c.entries[ref]; ok {
		return
	}
	if len(c.order) >= configContentsLimit {
		delete(c.entries, c.order[0])
		c.order = c.order[1:]
	}
	c.entries[ref] = wf
	c.order = append(c.order, ref)
}

func (s *Repository) SaveWorkflowConfigInNamespace(namespace string, wf *program.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	ref, _, err := s.saveWorkflowConfigInNamespace(namespace, wf, activateAlways)
	return ref, err
}

// SaveWorkflowConfigArtifactInNamespace 登记内容版本但不切 active；旧 Worker 重启不会把 latest 拉回旧版本。
func (s *Repository) SaveWorkflowConfigArtifactInNamespace(namespace string, wf *program.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	ref, _, err := s.saveWorkflowConfigInNamespace(namespace, wf, activateNever)
	return ref, err
}

// WorkflowConfigEnrollStateInNamespace 报告登记是否会产生激活动作：内容首次出现或尚无 active 时 true。
func (s *Repository) WorkflowConfigEnrollStateInNamespace(namespace string, ref definition.WorkflowConfigRef) (bool, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	row, err := gsql.From[WorkflowConfigPO](WorkflowConfigSchema).
		Where(
			WorkflowConfigSchema.Namespace.Eq(namespace),
			WorkflowConfigSchema.WorkflowType.Eq(ref.WorkflowType),
			WorkflowConfigSchema.Version.Eq(ref.Version),
		).Take(s.db)
	if err != nil {
		return false, err
	}
	if row == nil {
		return true, nil
	}
	active, err := gsql.From[WorkflowConfigActivePO](WorkflowConfigActiveSchema).
		Where(
			WorkflowConfigActiveSchema.Namespace.Eq(namespace),
			WorkflowConfigActiveSchema.WorkflowType.Eq(ref.WorkflowType),
		).Take(s.db)
	if err != nil {
		return false, err
	}
	return active == nil, nil
}

// EnrollWorkflowConfigInNamespace 登记内容版本，并在内容首次出现或尚无 active 行时激活。
func (s *Repository) EnrollWorkflowConfigInNamespace(namespace string, wf *program.WorkflowConfig) (definition.WorkflowConfigRef, bool, error) {
	return s.saveWorkflowConfigInNamespace(namespace, wf, activateIfNew)
}

// activationMode 决定保存内容版本时如何维护 active/latest。
type activationMode int

const (
	activateNever  activationMode = iota // 只登记内容版本
	activateAlways                       // 总是把 latest 指向本次内容（显式注册/回滚）
	activateIfNew                        // 新内容首次出现，或还没有 latest 时才激活
)

func (s *Repository) saveWorkflowConfigInNamespace(namespace string, wf *program.WorkflowConfig, mode activationMode) (definition.WorkflowConfigRef, bool, error) {
	version, err := program.WorkflowConfigVersion(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, false, err
	}
	data, err := marshalWorkflowConfig(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, false, err
	}
	namespace = protocol.NormalizeNamespace(namespace)
	ref := definition.WorkflowConfigRef{Namespace: namespace, WorkflowType: wf.Type, Version: version}
	activated := false
	err = s.db.Transaction(func(tx *gorm.DB) error {
		row := &WorkflowConfigPO{
			Namespace:    namespace,
			WorkflowType: wf.Type,
			Version:      version,
			ConfigJSON:   data,
			CreatedAt:    time.Now(),
		}
		// 内容版本已存在时不改任何列：撞主键 (namespace, workflow_type, version) 时执行
		// ON DUPLICATE KEY UPDATE namespace = namespace，RowsAffected 为 0。
		// 不用 INSERT IGNORE（gsql.InsertIgnore）：它会把截断、NOT NULL 等写入错误一并降成警告。
		namespaceColumn := clause.Column{Name: "namespace"}
		inserted := tx.Clauses(clause.OnConflict{DoUpdates: clause.Set{{Column: namespaceColumn, Value: namespaceColumn}}}).Create(row)
		if inserted.Error != nil {
			return inserted.Error
		}
		if mode == activateNever {
			return nil
		}
		if mode == activateIfNew && inserted.RowsAffected == 0 {
			// 内容已存在（旧二进制重启/同 hash 重复启动）：只有在该 workflow 还没有
			// active 版本时才补激活，否则保持现状。
			active, err := gsql.From[WorkflowConfigActivePO](WorkflowConfigActiveSchema).
				Where(
					WorkflowConfigActiveSchema.Namespace.Eq(namespace),
					WorkflowConfigActiveSchema.WorkflowType.Eq(wf.Type),
				).Take(tx)
			if err != nil {
				return err
			}
			if active != nil {
				return nil
			}
		}
		active := &WorkflowConfigActivePO{
			Namespace:    namespace,
			WorkflowType: wf.Type,
			Version:      version,
			UpdatedAt:    time.Now(),
		}
		// 撞主键 (namespace, workflow_type) 时改写 version 与 updated_at（ON DUPLICATE KEY UPDATE）。
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "namespace"}, {Name: "workflow_type"}},
			DoUpdates: clause.AssignmentColumns([]string{"version", "updated_at"}),
		}).Create(active).Error; err != nil {
			return err
		}
		activated = true
		return nil
	})
	if err != nil {
		return definition.WorkflowConfigRef{}, false, err
	}
	return ref, activated, nil
}

func (s *Repository) GetWorkflowConfig(ref definition.WorkflowConfigRef) (*program.WorkflowConfig, error) {
	ref.Namespace = protocol.NormalizeNamespace(ref.Namespace)
	if wf, ok := s.contents.get(ref); ok {
		return wf, nil
	}
	row, err := gsql.From[WorkflowConfigPO](WorkflowConfigSchema).
		Where(
			WorkflowConfigSchema.Namespace.Eq(ref.Namespace),
			WorkflowConfigSchema.WorkflowType.Eq(ref.WorkflowType),
			WorkflowConfigSchema.Version.Eq(ref.Version),
		).Take(s.db)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("%w: %s/%s@%s", definition.ErrWorkflowConfigNotFound, ref.Namespace, ref.WorkflowType, ref.Version)
	}
	wf, err := unmarshalWorkflowConfig(row.ConfigJSON)
	if err != nil {
		return nil, err
	}
	if wf != nil {
		s.contents.put(ref, wf)
	}
	return wf, nil
}

func (s *Repository) LatestWorkflowConfigRefInNamespace(namespace, workflowType string) (definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	row, err := gsql.From[WorkflowConfigActivePO](WorkflowConfigActiveSchema).
		Where(
			WorkflowConfigActiveSchema.Namespace.Eq(namespace),
			WorkflowConfigActiveSchema.WorkflowType.Eq(workflowType),
		).Take(s.db)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	if row == nil {
		return definition.WorkflowConfigRef{}, fmt.Errorf("%w: %s/%s latest version", definition.ErrWorkflowConfigNotFound, namespace, workflowType)
	}
	return definition.WorkflowConfigRef{Namespace: row.Namespace, WorkflowType: row.WorkflowType, Version: row.Version}, nil
}

// ListWorkflowConfigRefsInNamespace 返回该 namespace 全部已存内容版本（历史 + active），
// 按 (workflow_type, created_at, version) 排序。Console definitions 页依赖它展示版本列表。
func (s *Repository) ListWorkflowConfigRefsInNamespace(namespace string) ([]definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	rows, err := gsql.From[WorkflowConfigPO](WorkflowConfigSchema).
		Where(WorkflowConfigSchema.Namespace.Eq(namespace)).
		Order(WorkflowConfigSchema.WorkflowType).
		Order(WorkflowConfigSchema.CreatedAt).
		Order(WorkflowConfigSchema.Version).
		Find(s.db)
	if err != nil {
		return nil, err
	}
	refs := make([]definition.WorkflowConfigRef, 0, len(rows))
	for _, row := range rows {
		refs = append(refs, definition.WorkflowConfigRef{Namespace: row.Namespace, WorkflowType: row.WorkflowType, Version: row.Version})
	}
	return refs, nil
}

// ActiveWorkflowConfigRefsInNamespace 返回持久化的 active 引用，按 workflow_type 排序；进程重启后
// Console 的 Active 标记与 latest 版本仍可展示。
func (s *Repository) ActiveWorkflowConfigRefsInNamespace(namespace string) ([]definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	rows, err := gsql.From[WorkflowConfigActivePO](WorkflowConfigActiveSchema).
		Where(WorkflowConfigActiveSchema.Namespace.Eq(namespace)).
		Order(WorkflowConfigActiveSchema.WorkflowType).
		Find(s.db)
	if err != nil {
		return nil, err
	}
	refs := make([]definition.WorkflowConfigRef, 0, len(rows))
	for _, row := range rows {
		refs = append(refs, definition.WorkflowConfigRef{Namespace: row.Namespace, WorkflowType: row.WorkflowType, Version: row.Version})
	}
	return refs, nil
}
