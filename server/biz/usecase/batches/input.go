package batches

import (
	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
)

type StartRequest struct {
	Namespace string                  `json:"namespace,omitempty"`
	Type      batch.Type              `json:"type"`
	Filter    execution.RunListFilter `json:"filter"`
	Reason    string                  `json:"reason,omitempty"`
	ResetType execution.ResetType     `json:"reset_type,omitempty"`
}

type ListFilter struct {
	Namespace string `json:"namespace,omitempty"`
	Offset    int    `json:"offset,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type ListResult struct {
	Items []batch.Operation `json:"items"`
	Total int               `json:"total"`
}
