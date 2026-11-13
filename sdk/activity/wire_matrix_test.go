package activity

import (
	"testing"

	"github.com/donutnomad/tenuto/protocol"
)

// DTO/JSON 变更的 wire 契约矩阵。
//
// 对照规则（见 WireFingerprint）：字段按 JSON 名排序后比较 JSON 名、wire 类型、
// omitempty 与指针可选性；Go 类型名、包名、字段声明顺序不参与比较。
// 因此“缺必填字段”不是本层能表达的维度：契约只比较结构，运行期是否缺字段
// 由业务 JSON 解码决定。

type wireBase struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount"`
}

func TestWireFingerprintDTOChangeMatrix(t *testing.T) {
	type addOptional struct {
		OrderID string  `json:"order_id"`
		Amount  int64   `json:"amount"`
		Note    *string `json:"note,omitempty"`
	}
	type addRequired struct {
		OrderID  string `json:"order_id"`
		Amount   int64  `json:"amount"`
		Currency string `json:"currency"`
	}
	type renameJSON struct {
		OrderID string `json:"order_id"`
		Total   int64  `json:"total"`
	}
	type changeType struct {
		OrderID string `json:"order_id"`
		Amount  string `json:"amount"`
	}
	type deleteField struct {
		OrderID string `json:"order_id"`
	}
	type reorderFields struct {
		Amount  int64  `json:"amount"`
		OrderID string `json:"order_id"`
	}
	// 只有 Go 标识符不同、JSON 结构完全一致。
	type renamedGoFields struct {
		Order string `json:"order_id"`
		Total int64  `json:"amount"`
	}
	type valueToPointer struct {
		OrderID string `json:"order_id"`
		Amount  *int64 `json:"amount"`
	}
	type dropOmitEmpty struct {
		OrderID string `json:"order_id"`
		Amount  int64  `json:"amount,omitempty"`
	}

	base, err := WireFingerprintOf[wireBase]()
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		fp       func() (string, error)
		wantSame bool
	}{
		{name: "add-optional-field", fp: WireFingerprintOf[addOptional]},
		{name: "add-required-field", fp: WireFingerprintOf[addRequired]},
		{name: "rename-json-field", fp: WireFingerprintOf[renameJSON]},
		{name: "change-field-type-int-to-string", fp: WireFingerprintOf[changeType]},
		{name: "delete-field", fp: WireFingerprintOf[deleteField]},
		{name: "reorder-fields", fp: WireFingerprintOf[reorderFields], wantSame: true},
		{name: "rename-go-fields", fp: WireFingerprintOf[renamedGoFields], wantSame: true},
		{name: "value-to-pointer", fp: WireFingerprintOf[valueToPointer]},
		{name: "add-omitempty", fp: WireFingerprintOf[dropOmitEmpty]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fp()
			if err != nil {
				t.Fatal(err)
			}
			if same := got == base; same != tc.wantSame {
				t.Fatalf("fingerprint same=%v, want %v (base %s got %s)", same, tc.wantSame, base, got)
			}
		})
	}
}

// 契约对照只拒绝“已上报且值不同”的维度：未上报（旧 worker）与只上报部分维度
// 都保持放行。缺必填字段属于运行期解码，不在契约对照范围内。
func TestContractComparisonDoesNotEncodeRequiredness(t *testing.T) {
	type required struct {
		OrderID string `json:"order_id"`
	}
	type optional struct {
		OrderID string `json:"order_id,omitempty"`
	}
	requiredFP, err := WireFingerprintOf[required]()
	if err != nil {
		t.Fatal(err)
	}
	optionalFP, err := WireFingerprintOf[optional]()
	if err != nil {
		t.Fatal(err)
	}
	if requiredFP == optionalFP {
		t.Fatal("omitempty must change the wire fingerprint")
	}
	declared := Contract{Type: "payments.charge", Version: "v1", Input: requiredFP}
	mismatched := protocol.CompareActivityContracts(declared, []Contract{{
		Type: "payments.charge", Version: "v1",
	}})
	if len(mismatched) != 1 || mismatched[0].Field != "input" || mismatched[0].Actual != "" {
		t.Fatalf("unreported input must be a mismatch with an empty actual value, got %+v", mismatched)
	}
}
