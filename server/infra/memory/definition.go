package memory

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	programpkg "github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/server/biz/domain/definition"
)

// DefinitionRepository 是配置内容版本的内存仓储。
type DefinitionRepository struct {
	mu      sync.RWMutex
	configs map[definition.WorkflowConfigRef]storedConfig
	latest  map[string]definition.WorkflowConfigRef
}

// storedConfig 是一个配置内容版本与它首次保存的时间；列表按首次保存的先后排列。
type storedConfig struct {
	config    *programpkg.WorkflowConfig
	createdAt time.Time
}

func NewDefinitionRepository() *DefinitionRepository {
	return &DefinitionRepository{
		configs: make(map[definition.WorkflowConfigRef]storedConfig),
		latest:  make(map[string]definition.WorkflowConfigRef),
	}
}

// putConfigLocked 保存内容版本；同一版本再次保存时保留首次保存的时间。调用方持有 s.mu。
func (s *DefinitionRepository) putConfigLocked(ref definition.WorkflowConfigRef, wf *programpkg.WorkflowConfig) {
	createdAt := time.Now()
	if existing, ok := s.configs[ref]; ok {
		createdAt = existing.createdAt
	}
	s.configs[ref] = storedConfig{config: wf, createdAt: createdAt}
}

var _ definition.ConfigRepository = (*DefinitionRepository)(nil)

func definitionKey(namespace, workflowID string) string {
	return protocol.NormalizeNamespace(namespace) + "/" + workflowID
}

func (s *DefinitionRepository) SaveWorkflowConfigInNamespace(namespace string, wf *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	version, err := programpkg.WorkflowConfigVersion(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	cloned, err := definition.CloneWorkflowConfig(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	namespace = protocol.NormalizeNamespace(namespace)
	ref := definition.WorkflowConfigRef{Namespace: namespace, WorkflowID: wf.ID, Version: version}
	s.mu.Lock()
	s.putConfigLocked(ref, cloned)
	s.latest[definitionKey(namespace, wf.ID)] = ref
	s.mu.Unlock()
	return ref, nil
}

func (s *DefinitionRepository) SaveWorkflowConfigArtifactInNamespace(namespace string, wf *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, error) {
	version, err := programpkg.WorkflowConfigVersion(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	cloned, err := definition.CloneWorkflowConfig(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, err
	}
	namespace = protocol.NormalizeNamespace(namespace)
	ref := definition.WorkflowConfigRef{Namespace: namespace, WorkflowID: wf.ID, Version: version}
	s.mu.Lock()
	s.putConfigLocked(ref, cloned)
	s.mu.Unlock()
	return ref, nil
}

// WorkflowConfigEnrollStateInNamespace 报告登记是否会产生激活动作：内容首次出现或尚无 latest 时 true。
func (s *DefinitionRepository) WorkflowConfigEnrollStateInNamespace(namespace string, ref definition.WorkflowConfigRef) (bool, error) {
	ref.Namespace = protocol.NormalizeNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, exists := s.configs[ref]; !exists {
		return true, nil
	}
	_, hasLatest := s.latest[definitionKey(ref.Namespace, ref.WorkflowID)]
	return !hasLatest, nil
}

// EnrollWorkflowConfigInNamespace 登记内容版本；内容首次出现或还没有 latest 时激活。
func (s *DefinitionRepository) EnrollWorkflowConfigInNamespace(namespace string, wf *programpkg.WorkflowConfig) (definition.WorkflowConfigRef, bool, error) {
	version, err := programpkg.WorkflowConfigVersion(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, false, err
	}
	cloned, err := definition.CloneWorkflowConfig(wf)
	if err != nil {
		return definition.WorkflowConfigRef{}, false, err
	}
	namespace = protocol.NormalizeNamespace(namespace)
	ref := definition.WorkflowConfigRef{Namespace: namespace, WorkflowID: wf.ID, Version: version}
	key := definitionKey(namespace, wf.ID)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.configs[ref]
	s.putConfigLocked(ref, cloned)
	_, hasLatest := s.latest[key]
	if exists && hasLatest {
		return ref, false, nil
	}
	s.latest[key] = ref
	return ref, true, nil
}

func (s *DefinitionRepository) GetWorkflowConfig(ref definition.WorkflowConfigRef) (*programpkg.WorkflowConfig, error) {
	ref.Namespace = protocol.NormalizeNamespace(ref.Namespace)
	s.mu.RLock()
	wf := s.configs[ref].config
	s.mu.RUnlock()
	if wf == nil {
		return nil, fmt.Errorf("%w: %s/%s@%s", definition.ErrWorkflowConfigNotFound, ref.Namespace, ref.WorkflowID, ref.Version)
	}
	// 保存时已复制一份，内容版本不可变：调用方共享这一份只读配置。
	return wf, nil
}

func (s *DefinitionRepository) LatestWorkflowConfigRefInNamespace(namespace, workflowID string) (definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	s.mu.RLock()
	ref, ok := s.latest[definitionKey(namespace, workflowID)]
	s.mu.RUnlock()
	if !ok {
		return definition.WorkflowConfigRef{}, fmt.Errorf("%w: %s/%s latest version", definition.ErrWorkflowConfigNotFound, namespace, workflowID)
	}
	return ref, nil
}

// ActiveWorkflowConfigRefsInNamespace 按 workflow ID 升序返回 active 引用，与 gorm 实现一致。
func (s *DefinitionRepository) ActiveWorkflowConfigRefsInNamespace(namespace string) ([]definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	refs := []definition.WorkflowConfigRef{}
	for _, ref := range s.latest {
		if ref.Namespace == namespace {
			refs = append(refs, ref)
		}
	}
	slices.SortFunc(refs, func(a, b definition.WorkflowConfigRef) int { return strings.Compare(a.WorkflowID, b.WorkflowID) })
	return refs, nil
}

// ListWorkflowConfigRefsInNamespace 按 (workflow ID, 首次保存时间, 版本) 排序，与 gorm 实现一致。
func (s *DefinitionRepository) ListWorkflowConfigRefsInNamespace(namespace string) ([]definition.WorkflowConfigRef, error) {
	namespace = protocol.NormalizeNamespace(namespace)
	s.mu.RLock()
	defer s.mu.RUnlock()
	refs := []definition.WorkflowConfigRef{}
	for ref := range s.configs {
		if ref.Namespace == namespace {
			refs = append(refs, ref)
		}
	}
	slices.SortFunc(refs, func(a, b definition.WorkflowConfigRef) int {
		return cmp.Or(
			strings.Compare(a.WorkflowID, b.WorkflowID),
			s.configs[a].createdAt.Compare(s.configs[b].createdAt),
			strings.Compare(a.Version, b.Version),
		)
	})
	return refs, nil
}
