package definitionrepo

import (
	"encoding/json"
	"fmt"

	"github.com/donutnomad/tenuto/program"
)

func marshalWorkflowConfig(value *program.WorkflowConfig) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow config: %w", err)
	}
	return data, nil
}

func unmarshalWorkflowConfig(data []byte) (*program.WorkflowConfig, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var value program.WorkflowConfig
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("unmarshal workflow config: %w", err)
	}
	return &value, nil
}
