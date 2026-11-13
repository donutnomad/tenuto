package activity

import (
	"testing"
	"time"
)

type localChargeInput struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount,omitempty"`
}

// remotePayRequest 故意使用不同的包内类型名，wire 结构相同。
type remotePayRequest struct {
	OrderID string `json:"order_id"`
	Amount  int64  `json:"amount,omitempty"`
}

type brokenPayRequest struct {
	OrderID string `json:"order_id"`
	Amount  string `json:"amount,omitempty"`
}

type localChargeOutput struct {
	ReceiptID string `json:"receipt_id"`
}

func TestWireFingerprintIgnoresGoIdentity(t *testing.T) {
	local, err := WireFingerprintOf[localChargeInput]()
	if err != nil {
		t.Fatal(err)
	}
	remote, err := WireFingerprintOf[remotePayRequest]()
	if err != nil {
		t.Fatal(err)
	}
	if local != remote {
		t.Fatalf("same JSON structure must match: %s vs %s", local, remote)
	}
	broken, err := WireFingerprintOf[brokenPayRequest]()
	if err != nil {
		t.Fatal(err)
	}
	if broken == local {
		t.Fatal("field type change must change the wire fingerprint")
	}
}

func TestWireFingerprintNestedAndOptional(t *testing.T) {
	type item struct {
		SKU string `json:"sku"`
	}
	type cart struct {
		Items []item  `json:"items"`
		Note  *string `json:"note,omitempty"`
	}
	first, err := WireFingerprintOf[cart]()
	if err != nil {
		t.Fatal(err)
	}
	type otherItem struct {
		SKU string `json:"sku"`
	}
	type otherCart struct {
		Items []otherItem `json:"items"`
		Note  *string     `json:"note,omitempty"`
	}
	second, err := WireFingerprintOf[otherCart]()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("nested independent DTOs must match: %s vs %s", first, second)
	}
	type requiredNote struct {
		Items []item `json:"items"`
		Note  string `json:"note"`
	}
	third, err := WireFingerprintOf[requiredNote]()
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("optionality change must change the wire fingerprint")
	}
}

func TestContractOfUsesWireFingerprints(t *testing.T) {
	contract, err := ContractOf[localChargeInput, localChargeOutput]("payments.charge.v1", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if contract.Type != "payments.charge.v1" || contract.Version != "v1" {
		t.Fatalf("contract = %+v", contract)
	}
	input, err := WireFingerprintOf[localChargeInput]()
	if err != nil {
		t.Fatal(err)
	}
	if contract.Input != input {
		t.Fatalf("contract input = %s, want %s", contract.Input, input)
	}
}

type stringTagged struct {
	Amount int64 `json:"amount,string"`
}

type EmbeddedInner struct {
	Inner string `json:"inner"`
}

type embeddedOuter struct {
	EmbeddedInner
	Other string `json:"other"`
}

type customCodec struct{}

func (customCodec) MarshalJSON() ([]byte, error) { return []byte("{}"), nil }

type unsupportedSamples struct {
	Bytes  []byte            `json:"bytes"`
	Time   time.Time         `json:"time"`
	Custom customCodec       `json:"custom"`
	Any    any               `json:"any"`
	Map    map[string]string `json:"map"`
}

// TestWireTypeOfGoMatchesGeneratorSubset 锁定 Go 侧与 generator descriptor 侧
// 同一个 json/plain-v1 子集：无法安全描述的类型必须显式报错，而不是产出
// 永不匹配或假兼容的指纹。
func TestWireTypeOfGoMatchesGeneratorSubset(t *testing.T) {
	if _, err := WireFingerprintOf[stringTagged](); err == nil {
		t.Fatal("json:\",string\" must be rejected")
	}
	if _, err := WireFingerprintOf[embeddedOuter](); err == nil {
		t.Fatal("embedded fields must be rejected")
	}
	if _, err := WireFingerprintOf[struct {
		Value time.Time `json:"value"`
	}](); err == nil {
		t.Fatal("time.Time must be rejected")
	}
	if _, err := WireFingerprintOf[struct {
		Value []byte `json:"value"`
	}](); err == nil {
		t.Fatal("[]byte must be rejected")
	}
	if _, err := WireFingerprintOf[struct {
		Value uint `json:"value"`
	}](); err == nil {
		t.Fatal("platform uint must be rejected")
	}
	if _, err := WireFingerprintOf[customCodec](); err == nil {
		t.Fatal("custom JSON codec must be rejected")
	}
	if _, err := WireFingerprintOf[struct {
		Value any               `json:"value"`
		Map   map[string]string `json:"map"`
	}](); err != nil {
		t.Fatalf("any + string map should be supported: %v", err)
	}
}
