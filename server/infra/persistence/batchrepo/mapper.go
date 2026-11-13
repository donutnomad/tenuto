package batchrepo

import "github.com/donutnomad/tenuto/server/biz/domain/batch"

func (p *BatchOperationPO) ToPO(entity *batch.Operation) *BatchOperationPO {
	if entity == nil {
		return nil
	}
	return &BatchOperationPO{
		ID:             entity.ID,
		Namespace:      entity.Namespace,
		Type:           string(entity.Type),
		ResetType:      string(entity.ResetType),
		Reason:         entity.Reason,
		Status:         string(entity.Status),
		TotalCount:     entity.TotalCount,
		SucceededCount: entity.SucceededCount,
		FailedCount:    entity.FailedCount,
		SkippedCount:   entity.SkippedCount,
		Version:        entity.Version,
		CreatedAt:      entity.CreatedAt,
		StartedAt:      entity.StartedAt,
		FinishedAt:     entity.FinishedAt,
		UpdatedAt:      entity.UpdatedAt,
	}
}
