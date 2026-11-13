package program

import (
	"testing"

	"github.com/donutnomad/tenuto/sdk/activity"
	wlangtypes "github.com/donutnomad/wlang/types"
)

type wireSample struct {
	OrderID string  `json:"order_id"`
	Amount  int     `json:"amount,omitempty"`
	Note    *string `json:"note,omitempty"`
	Ignored string  `json:"-"`
}

// TestWLangWireFingerprintMatchesGo 锁定 descriptor 侧与 Go 反射侧使用同一
// wire 规范：Go int→int64、忽略 json:"-"、指针可选、omitempty 参与比较。
func TestWLangWireFingerprintMatchesGo(t *testing.T) {
	types := map[string]WLangTypeDescriptor{
		"flows.WireSample": {
			ID: "flows.WireSample", Kind: "record",
			Fields: []wlangtypes.FieldDescriptor{
				{GoName: "OrderID", JSONName: "order_id", TypeID: "string"},
				{GoName: "Amount", JSONName: "amount", TypeID: "int64", OmitEmpty: true},
				{GoName: "Note", JSONName: "note", TypeID: "pointer:string", OmitEmpty: true},
				{GoName: "Ignored", JSONName: "-", TypeID: "string"},
			},
		},
		"pointer:string": {ID: "pointer:string", Kind: "pointer", Elem: "string"},
	}
	descriptorFingerprint, err := WLangWireFingerprint("flows.WireSample", types)
	if err != nil {
		t.Fatal(err)
	}
	goFingerprint, err := activity.WireFingerprintOf[wireSample]()
	if err != nil {
		t.Fatal(err)
	}
	if descriptorFingerprint != goFingerprint {
		t.Fatalf("descriptor wire = %s, go wire = %s", descriptorFingerprint, goFingerprint)
	}
}
