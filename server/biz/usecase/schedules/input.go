package schedules

import "github.com/donutnomad/tenuto/server/biz/domain/schedule"

type CreateRequest struct {
	Namespace      string         `json:"namespace,omitempty"`
	WorkflowType   string         `json:"workflow_type"`
	Description    string         `json:"description,omitempty"`
	CronExpression string         `json:"cron_expression"`
	Timezone       string         `json:"timezone,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	OverlapPolicy  string         `json:"overlap_policy,omitempty"`
	Paused         bool           `json:"paused,omitempty"`
}

type UpdateRequest struct {
	Description    *string         `json:"description,omitempty"`
	CronExpression *string         `json:"cron_expression,omitempty"`
	Timezone       *string         `json:"timezone,omitempty"`
	Input          *map[string]any `json:"input,omitempty"`
	OverlapPolicy  *string         `json:"overlap_policy,omitempty"`
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
