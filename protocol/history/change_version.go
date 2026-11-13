package history

import "encoding/json"

// ChangeVersionSearchAttribute 是 GetVersion 维护的保留搜索属性，值为 JSON 数组编码的
// `changeID-version` 列表，与 Temporal 的 TemporalChangeVersion 同名同义。
const ChangeVersionSearchAttribute = "TemporalChangeVersion"

// EncodeChangeVersions 编码 ChangeVersionSearchAttribute 的属性值。
func EncodeChangeVersions(values []string) string {
	encoded, _ := json.Marshal(values) // []string 的编码恒成功
	return string(encoded)
}

// IsChangeVersionUpsert 识别 GetVersion 决策写入的事件。保留键拒绝外部写入，
// 所以带该键的 SearchAttributesUpserted 全部来自 Workflow 决策。
func IsChangeVersionUpsert(event Event) bool {
	if event.SearchAttributesUpserted == nil {
		return false
	}
	_, ok := event.SearchAttributesUpserted.Attributes[ChangeVersionSearchAttribute]
	return ok
}
