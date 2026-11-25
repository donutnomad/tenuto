package protocol

// ActivityContract 是一个 Activity 的 wire 契约：类型名、输入与输出的结构指纹。
//
// Worker 为它登记的每个 Activity 上报完整契约。Workflow 侧声明的维度逐字段与上报值对照，
// 值相同才算匹配；Workflow 侧留空的维度不参与对照。
type ActivityContract struct {
	Type   string `json:"type,omitempty"`
	Input  string `json:"input,omitempty"`
	Output string `json:"output,omitempty"`
}

// HasDeclaration 报告契约是否声明了至少一个可对照的维度。
func (c ActivityContract) HasDeclaration() bool {
	return c.Input != "" || c.Output != ""
}

// ActivityContractFieldReport 是一个维度对照结果：Field 为 input/output。
type ActivityContractFieldReport struct {
	Field    string
	Expected string
	Actual   string
}

// CompareActivityContracts 用声明的契约逐字段对照 Worker 上报的契约，返回不匹配的维度。
//
// 声明的维度在任一份上报里取到相同的值即匹配。Worker 没有上报该维度时 Actual 为空，
// 同样算不匹配。
func CompareActivityContracts(declared ActivityContract, reported []ActivityContract) []ActivityContractFieldReport {
	fields := []struct {
		name     string
		expected string
		actual   func(ActivityContract) string
	}{
		{"input", declared.Input, func(c ActivityContract) string { return c.Input }},
		{"output", declared.Output, func(c ActivityContract) string { return c.Output }},
	}
	var mismatched []ActivityContractFieldReport
	for _, field := range fields {
		if field.expected == "" {
			continue
		}
		matched := false
		actual := ""
		for _, contract := range reported {
			value := field.actual(contract)
			if value == field.expected {
				matched = true
				break
			}
			if actual == "" {
				actual = value
			}
		}
		if !matched {
			mismatched = append(mismatched, ActivityContractFieldReport{Field: field.name, Expected: field.expected, Actual: actual})
		}
	}
	return mismatched
}
