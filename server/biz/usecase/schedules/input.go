package schedules

import (
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
)

type CreateRequest struct {
	Namespace      string         `json:"namespace,omitempty"`
	WorkflowType   string         `json:"workflow_type"`
	Description    string         `json:"description,omitempty"`
	CronExpression string         `json:"cron_expression"`
	Timezone       string         `json:"timezone,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	OverlapPolicy  string         `json:"overlap_policy,omitempty"`
	Paused         bool           `json:"paused,omitempty"`
	// CatchupWindow 为 0 时取 schedule.DefaultCatchupWindow。
	CatchupWindow time.Duration `json:"catchup_window,omitempty"`
}

type UpdateRequest struct {
	Description    *string         `json:"description,omitempty"`
	CronExpression *string         `json:"cron_expression,omitempty"`
	Timezone       *string         `json:"timezone,omitempty"`
	Input          *map[string]any `json:"input,omitempty"`
	OverlapPolicy  *string         `json:"overlap_policy,omitempty"`
	CatchupWindow  *time.Duration  `json:"catchup_window,omitempty"`
}

type ListFilter struct {
	Namespace    string `json:"namespace,omitempty"`
	WorkflowType string `json:"workflow_type,omitempty"`
	Paused       *bool  `json:"paused,omitempty"`
	Offset       int    `json:"offset,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

type ListResult struct {
	Items []schedule.Schedule `json:"items"`
	Total int                 `json:"total"`
}
