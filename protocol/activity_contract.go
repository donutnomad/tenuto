package protocol

// ActivityContract 是一个 Activity 的 wire 契约：类型名、输入与输出的结构指纹，以及指纹对应的 wire 结构。
//
// Worker 为它登记的每个 Activity 上报完整契约。Workflow 侧声明的维度逐个与上报对照：指纹相同即匹配；
// 指纹不同而两侧都带 wire 结构时按数据流向判兼容（见 CompareActivityContracts）。Workflow 侧留空的维度不参与对照。
type ActivityContract struct {
	Type   string `json:"type,omitempty"`
	Input  string `json:"input,omitempty"`
	Output string `json:"output,omitempty"`
	// InputWire、OutputWire 是 Input、Output 指纹对应的 wire 结构。
	InputWire  *WireType `json:"input_wire,omitempty"`
	OutputWire *WireType `json:"output_wire,omitempty"`
}

// HasDeclaration 报告契约是否声明了至少一个可对照的维度。
func (c ActivityContract) HasDeclaration() bool {
	return c.Input != "" || c.Output != ""
}

// ActivityContractFieldReport 是一个维度对照结果：Field 为 input/output。Issues 是两侧都带 wire 结构时
// 按数据流向找到的不兼容位置。
type ActivityContractFieldReport struct {
	Field    string
	Expected string
	Actual   string
	Issues   []WireIncompatibility
}

// CompareActivityContracts 用声明的契约逐维度对照 Worker 上报的契约，返回不匹配的维度。
//
// 声明的维度在任一份上报里兼容即匹配：指纹相同；或者两侧都带 wire 结构，且按数据流向写出的 JSON
// 能被读方无损读回（CompareWireTypes）。input 由 Workflow 写、Worker 读；output 由 Worker 写、Workflow 读。
// 例如 Worker 的入参新增字段兼容（旧调用方的载荷里取零值），删字段、改类型不兼容。
// Worker 没有上报该维度时 Actual 为空，同样算不匹配。
func CompareActivityContracts(declared ActivityContract, reported []ActivityContract) []ActivityContractFieldReport {
	fields := []struct {
		name     string
		expected string
		actual   func(ActivityContract) string
		// compare 返回按数据流向对照两侧 wire 结构的不兼容位置；任一侧没有结构时 ok 为 false。
		compare func(ActivityContract) (issues []WireIncompatibility, ok bool)
	}{
		{"input", declared.Input, func(c ActivityContract) string { return c.Input }, func(c ActivityContract) ([]WireIncompatibility, bool) {
			if declared.InputWire == nil || c.InputWire == nil {
				return nil, false
			}
			return CompareWireTypes(*declared.InputWire, *c.InputWire), true
		}},
		{"output", declared.Output, func(c ActivityContract) string { return c.Output }, func(c ActivityContract) ([]WireIncompatibility, bool) {
			if declared.OutputWire == nil || c.OutputWire == nil {
				return nil, false
			}
			return CompareWireTypes(*c.OutputWire, *declared.OutputWire), true
		}},
	}
	var mismatched []ActivityContractFieldReport
	for _, field := range fields {
		if field.expected == "" {
			continue
		}
		var first *ActivityContractFieldReport
		matched := false
		for _, contract := range reported {
			value := field.actual(contract)
			if value == field.expected {
				matched = true
				break
			}
			issues, ok := field.compare(contract)
			if ok && len(issues) == 0 {
				matched = true
				break
			}
			if first == nil {
				first = &ActivityContractFieldReport{Field: field.name, Expected: field.expected, Actual: value, Issues: issues}
			}
		}
		if matched {
			continue
		}
		if first == nil {
			first = &ActivityContractFieldReport{Field: field.name, Expected: field.expected}
		}
		mismatched = append(mismatched, *first)
	}
	return mismatched
}
