package schedulerepo

import "github.com/donutnomad/tenuto/server/biz/domain/schedule"

func (p *SchedulePO) ToPO(entity *schedule.Schedule) *SchedulePO {
	if entity == nil {
		return nil
	}
	return &SchedulePO{
		ID:             entity.ID,
		Namespace:      entity.Namespace,
		WorkflowType:   entity.WorkflowType,
		Description:    entity.Description,
		CronExpression: entity.CronExpression,
		Timezone:       entity.Timezone,
		Paused:         entity.Paused,
		OverlapPolicy:  entity.OverlapPolicy,
		LastRunAt:      entity.LastRunAt,
		NextRunAt:      entity.NextRunAt,
		LastRunID:      entity.LastRunID,
		RunCount:       entity.RunCount,
		CreatedAt:      entity.CreatedAt,
		UpdatedAt:      entity.UpdatedAt,
	}
}
