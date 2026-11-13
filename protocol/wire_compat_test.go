package protocol

import (
	"reflect"
	"testing"
)

func wireScalar(name string) WireType { return WireType{Kind: WireKindScalar, Name: name} }

func wireRecord(fields ...WireField) WireType { return WireType{Kind: WireKindRecord, Fields: fields} }

func wireField(json string, schema WireType) WireField { return WireField{JSON: json, Type: schema} }

func wirePtr(elem WireType) WireType {
	return WireType{Kind: WireKindPointer, Elem: &elem, Optional: true}
}

func wireSlice(elem WireType) WireType { return WireType{Kind: WireKindSlice, Elem: &elem} }

func wireArray(length int64, elem WireType) WireType {
	return WireType{Kind: WireKindArray, Elem: &elem, Length: length}
}

func wireMap(key, elem WireType) WireType { return WireType{Kind: WireKindMap, Key: &key, Elem: &elem} }

// CompareWireTypes 判断按旧结构写出的 JSON 能否被新结构无损读回：新增字段兼容，删除、改名、改类型（含嵌套、
// 切片元素、map 值）不兼容；标量只接受无损放宽；T 改成 *T 兼容，反之不兼容。
func TestCompareWireTypes(t *testing.T) {
	order := wireRecord(wireField("order_id", wireScalar("string")), wireField("amount", wireScalar("int64")))
	for _, test := range []struct {
		name     string
		old, new WireType
		want     []WireIncompatibility
	}{
		{name: "Happy Path: identical records", old: order, new: order},
		{
			name: "Happy Path: an added field is compatible",
			old:  order,
			new:  wireRecord(wireField("order_id", wireScalar("string")), wireField("amount", wireScalar("int64")), wireField("note", wireScalar("string"))),
		},
		{
			name: "Happy Path: field order does not matter",
			old:  order,
			new:  wireRecord(wireField("amount", wireScalar("int64")), wireField("order_id", wireScalar("string"))),
		},
		{
			name: "Error Case: a removed field",
			old:  order,
			new:  wireRecord(wireField("amount", wireScalar("int64"))),
			want: []WireIncompatibility{{Path: "order_id", Reason: "field is missing in the new type (removed or renamed)"}},
		},
		{
			name: "Error Case: a renamed field",
			old:  order,
			new:  wireRecord(wireField("orderId", wireScalar("string")), wireField("amount", wireScalar("int64"))),
			want: []WireIncompatibility{{Path: "order_id", Reason: "field is missing in the new type (removed or renamed)"}},
		},
		{
			name: "Error Case: a retyped field",
			old:  order,
			new:  wireRecord(wireField("order_id", wireScalar("int64")), wireField("amount", wireScalar("int64"))),
			want: []WireIncompatibility{{Path: "order_id", Reason: "type changed from string to int64"}},
		},
		{
			name: "Error Case: a nested field change reports the full path",
			old:  wireRecord(wireField("customer", wireRecord(wireField("email", wireScalar("string"))))),
			new:  wireRecord(wireField("customer", wireRecord(wireField("mail", wireScalar("string"))))),
			want: []WireIncompatibility{{Path: "customer.email", Reason: "field is missing in the new type (removed or renamed)"}},
		},
		{
			name: "Error Case: a field of a slice element",
			old:  wireRecord(wireField("items", wireSlice(wireRecord(wireField("sku", wireScalar("string")))))),
			new:  wireRecord(wireField("items", wireSlice(wireRecord(wireField("code", wireScalar("string")))))),
			want: []WireIncompatibility{{Path: "items[].sku", Reason: "field is missing in the new type (removed or renamed)"}},
		},
		{
			name: "Error Case: a map value type",
			old:  wireRecord(wireField("labels", wireMap(wireScalar("string"), wireScalar("string")))),
			new:  wireRecord(wireField("labels", wireMap(wireScalar("string"), wireScalar("int64")))),
			want: []WireIncompatibility{{Path: "labels[*]", Reason: "type changed from string to int64"}},
		},
		{
			name: "Error Case: a slice became a scalar",
			old:  wireRecord(wireField("tags", wireSlice(wireScalar("string")))),
			new:  wireRecord(wireField("tags", wireScalar("string"))),
			want: []WireIncompatibility{{Path: "tags", Reason: "type changed from []string to string"}},
		},
		{
			name: "Happy Path: a value field becoming a pointer is compatible",
			old:  wireRecord(wireField("note", wireScalar("string"))),
			new:  wireRecord(wireField("note", wirePtr(wireScalar("string")))),
		},
		{
			name: "Happy Path: pointer elements are compared",
			old:  wireRecord(wireField("customer", wirePtr(wireRecord(wireField("email", wireScalar("string")))))),
			new:  wireRecord(wireField("customer", wirePtr(wireRecord(wireField("email", wireScalar("string")), wireField("name", wireScalar("string")))))),
		},
		{
			name: "Error Case: a pointer becoming a value loses null",
			old:  wireRecord(wireField("note", wirePtr(wireScalar("string")))),
			new:  wireRecord(wireField("note", wireScalar("string"))),
			want: []WireIncompatibility{{Path: "note", Reason: "optional value became required (null can no longer be told apart from the zero value)"}},
		},
		{
			name: "Happy Path: lossless numeric widening",
			old:  wireRecord(wireField("a", wireScalar("int32")), wireField("b", wireScalar("uint32")), wireField("c", wireScalar("float32")), wireField("d", wireScalar("int16"))),
			new:  wireRecord(wireField("a", wireScalar("int64")), wireField("b", wireScalar("int64")), wireField("c", wireScalar("float64")), wireField("d", wireScalar("float32"))),
		},
		{
			name: "Error Case: int64 and float64 are not interchangeable",
			old:  wireRecord(wireField("a", wireScalar("int64")), wireField("b", wireScalar("float64"))),
			new:  wireRecord(wireField("a", wireScalar("float64")), wireField("b", wireScalar("int64"))),
			want: []WireIncompatibility{
				{Path: "a", Reason: "type changed from int64 to float64"},
				{Path: "b", Reason: "type changed from float64 to int64"},
			},
		},
		{
			name: "Error Case: narrowing and signed to unsigned",
			old:  wireRecord(wireField("a", wireScalar("int64")), wireField("b", wireScalar("int32"))),
			new:  wireRecord(wireField("a", wireScalar("int32")), wireField("b", wireScalar("uint64"))),
			want: []WireIncompatibility{
				{Path: "a", Reason: "type changed from int64 to int32"},
				{Path: "b", Reason: "type changed from int32 to uint64"},
			},
		},
		{
			name: "Happy Path: any on either side is not checked",
			old:  wireRecord(wireField("a", wireScalar("any")), wireField("b", order)),
			new:  wireRecord(wireField("a", order), wireField("b", wireScalar("any"))),
		},
		{
			name: "Happy Path: a record read into a string-keyed map of a compatible value",
			old:  order,
			new:  wireMap(wireScalar("string"), wireScalar("any")),
		},
		{
			name: "Happy Path: an untyped map read into a record is not checked",
			old:  wireMap(wireScalar("string"), wireScalar("any")),
			new:  order,
		},
		{
			name: "Happy Path: an array read into a slice",
			old:  wireArray(3, wireScalar("string")),
			new:  wireSlice(wireScalar("string")),
		},
		{
			name: "Error Case: an array length change",
			old:  wireArray(3, wireScalar("string")),
			new:  wireArray(2, wireScalar("string")),
			want: []WireIncompatibility{{Reason: "array length changed from 3 to 2"}},
		},
		{
			name: "Happy Path: the same recursive back reference",
			old:  wireRecord(wireField("next", wirePtr(WireType{Kind: WireKindRecursive, Length: 1}))),
			new:  wireRecord(wireField("next", wirePtr(WireType{Kind: WireKindRecursive, Length: 1}))),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := CompareWireTypes(test.old, test.new); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("CompareWireTypes() = %+v, want %+v", got, test.want)
			}
		})
	}
}
