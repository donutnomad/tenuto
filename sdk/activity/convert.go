package activity

import (
	"encoding/json"

	"github.com/donutnomad/tenuto/internal/jsonvalue"
)

// mapToStruct 通过 JSON 桥接将 map[string]any 转为 struct
func mapToStruct(m map[string]any, out any) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// structToMap 通过 JSON 桥接将 struct 转为 map[string]any
func structToMap(v any) (map[string]any, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return jsonvalue.DecodeMap(data)
}
