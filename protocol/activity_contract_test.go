package protocol

import (
	"reflect"
	"testing"
)

func TestCompareActivityContractsMatchesDeclaredDimensions(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Version: "v2", Input: "ChargeInput.v2", Output: "Receipt.v2"}
	mismatched := CompareActivityContracts(declared, []ActivityContract{{
		Type: "payments.charge", Version: "v2", Input: "ChargeInput.v2", Output: "Receipt.v2",
	}})
	if len(mismatched) != 0 {
		t.Fatalf("expected compatible, mismatched=%+v", mismatched)
	}
}

// Worker 没有上报 Workflow 声明的维度时，该维度不匹配，Actual 为空。
func TestCompareActivityContractsReportsUnreportedDimensionAsMismatch(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Version: "v2", Input: "ChargeInput.v2"}
	for name, reported := range map[string][]ActivityContract{
		"dimension left empty": {{Type: "payments.charge", Version: "v2"}},
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
	declared := ActivityContract{Type: "payments.charge", Version: "v2", Input: "ChargeInput.v2"}
	mismatched := CompareActivityContracts(declared, []ActivityContract{{
		Type: "payments.charge", Version: "v2", Input: "ChargeInput.v1",
	}})
	if !reflect.DeepEqual(mismatched, []ActivityContractFieldReport{{Field: "input", Expected: "ChargeInput.v2", Actual: "ChargeInput.v1"}}) {
		t.Fatalf("unexpected mismatch: %+v", mismatched)
	}
}

func TestCompareActivityContractsIgnoresUndeclaredDimensions(t *testing.T) {
	declared := ActivityContract{Type: "payments.charge", Version: "v2"}
	mismatched := CompareActivityContracts(declared, []ActivityContract{{
		Type: "payments.charge", Version: "v2", Input: "anything", Output: "anything",
	}})
	if len(mismatched) != 0 {
		t.Fatalf("undeclared dimensions must not be compared: %+v", mismatched)
	}
}
