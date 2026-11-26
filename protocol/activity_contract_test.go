package protocol

import (
	"reflect"
	"testing"
)

func TestCompareActivityContractsMatchesDeclaredDimensions(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Input: "ChargeInput.v2", Output: "Receipt.v2"}
	mismatched := CompareActivityContracts(declared, []ActivityContract{{
		Type: "payments.charge", Input: "ChargeInput.v2", Output: "Receipt.v2",
	}})
	if len(mismatched) != 0 {
		t.Fatalf("expected compatible, mismatched=%+v", mismatched)
	}
}

// Worker 没有上报 Workflow 声明的维度时，该维度不匹配，Actual 为空。
func TestCompareActivityContractsReportsUnreportedDimensionAsMismatch(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Input: "ChargeInput.v2"}
	for name, reported := range map[string][]ActivityContract{
		"dimension left empty": {{Type: "payments.charge", Output: "Receipt.v2"}},
		"no contract at all":   nil,
	} {
		t.Run(name, func(t *testing.T) {
			mismatched := CompareActivityContracts(declared, reported)
			var input []ActivityContractFieldReport
			for _, report := range mismatched {
				if report.Field == "input" {
					input = append(input, report)
				}
			}
			if !reflect.DeepEqual(input, []ActivityContractFieldReport{{Field: "input", Expected: "ChargeInput.v2"}}) {
				t.Fatalf("unexpected mismatch: %+v", mismatched)
			}
		})
	}
}

func TestCompareActivityContractsReportsMismatchedDimension(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Input: "ChargeInput.v2"}
	mismatched := CompareActivityContracts(declared, []ActivityContract{{
		Type: "payments.charge", Input: "ChargeInput.v1",
	}})
	if !reflect.DeepEqual(mismatched, []ActivityContractFieldReport{{Field: "input", Expected: "ChargeInput.v2", Actual: "ChargeInput.v1"}}) {
		t.Fatalf("unexpected mismatch: %+v", mismatched)
	}
}

func TestCompareActivityContractsIgnoresUndeclaredDimensions(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Input: "ChargeInput.v2"}
	mismatched := CompareActivityContracts(declared, []ActivityContract{{
		Type: "payments.charge", Input: "ChargeInput.v2", Output: "anything",
	}})
	if len(mismatched) != 0 {
		t.Fatalf("undeclared dimensions must not be compared: %+v", mismatched)
	}
}

func contractRecord(fields map[string]string) *WireType {
	record := WireType{Kind: WireKindRecord}
	for name, scalar := range fields {
		record.Fields = append(record.Fields, WireField{JSON: name, Type: WireType{Kind: WireKindScalar, Name: scalar}})
	}
	return &record
}

func wireContract(input, output *WireType) ActivityContract {
	return ActivityContract{Type: "payments.charge", Input: WireFingerprint(*input), Output: WireFingerprint(*output), InputWire: input, OutputWire: output}
}

// 指纹不同时按数据流向判兼容：input 由 Workflow 写、Worker 读；output 由 Worker 写、Workflow 读。读方新增字段兼容
// （写方的载荷里取零值）；读方缺少写方的字段（删除或改名）、字段改类型都不兼容。
func TestCompareActivityContractsChecksWireStructureByDirection(t *testing.T) {
	base := contractRecord(map[string]string{"order_id": "string", "amount": "int64"})
	added := contractRecord(map[string]string{"order_id": "string", "amount": "int64", "warehouse": "string"})
	removed := contractRecord(map[string]string{"order_id": "string"})
	retyped := contractRecord(map[string]string{"order_id": "string", "amount": "string"})
	receipt := contractRecord(map[string]string{"receipt": "string"})
	for _, test := range []struct {
		name     string
		declared ActivityContract
		reported ActivityContract
		want     string // 不兼容的维度；空表示兼容
		issue    string
	}{
		{name: "input: worker adds a field", declared: wireContract(base, receipt), reported: wireContract(added, receipt)},
		{name: "input: worker drops a field the workflow sends", declared: wireContract(base, receipt), reported: wireContract(removed, receipt), want: "input", issue: "amount: field is missing in the new type"},
		{name: "input: worker changes a field type", declared: wireContract(base, receipt), reported: wireContract(retyped, receipt), want: "input", issue: "amount: type changed"},
		{name: "output: workflow reads an added field", declared: wireContract(receipt, added), reported: wireContract(receipt, base)},
		{name: "output: workflow lacks a field the worker returns", declared: wireContract(receipt, removed), reported: wireContract(receipt, base), want: "output", issue: "amount: field is missing in the new type"},
		{name: "output: worker changes a field type", declared: wireContract(receipt, base), reported: wireContract(receipt, retyped), want: "output", issue: "amount: type changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			mismatched := CompareActivityContracts(test.declared, []ActivityContract{test.reported})
			if test.want == "" {
				if len(mismatched) != 0 {
					t.Fatalf("expected compatible, mismatched=%+v", mismatched)
				}
				return
			}
			if len(mismatched) != 1 || mismatched[0].Field != test.want || len(mismatched[0].Issues) == 0 {
				t.Fatalf("mismatched=%+v, want one %s mismatch with issues", mismatched, test.want)
			}
			if got := mismatched[0].Issues[0].String(); len(got) < len(test.issue) || got[:len(test.issue)] != test.issue {
				t.Fatalf("issue = %q, want prefix %q", got, test.issue)
			}
		})
	}
}

// 任一侧没有 wire 结构时只能按指纹对照：指纹不同即不兼容。
func TestCompareActivityContractsWithoutWireStructureFallsBackToFingerprints(t *testing.T) {
	declared := wireContract(contractRecord(map[string]string{"order_id": "string"}), contractRecord(map[string]string{"receipt": "string"}))
	reported := wireContract(contractRecord(map[string]string{"order_id": "string", "note": "string"}), contractRecord(map[string]string{"receipt": "string"}))
	reported.InputWire = nil
	mismatched := CompareActivityContracts(declared, []ActivityContract{reported})
	if len(mismatched) != 1 || mismatched[0].Field != "input" || len(mismatched[0].Issues) != 0 {
		t.Fatalf("mismatched=%+v", mismatched)
	}
}
