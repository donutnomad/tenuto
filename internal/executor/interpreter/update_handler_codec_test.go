package interpreter

import (
	"testing"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/protocol"
	wlregistry "github.com/donutnomad/wlang/registry"
	wltypes "github.com/donutnomad/wlang/types"
)

func TestUpdateHandlerPayloadCodecRoundTripsRecord(t *testing.T) {
	descriptor := wltypes.TypeDescriptor{
		ID: "example.CreateResult", LanguageName: "example.CreateResult", Kind: wltypes.TypeKindRecord,
		Fields: []wltypes.FieldDescriptor{
			{GoName: "RecordID", JSONName: "record_id", TypeID: wltypes.TString},
			{GoName: "Count", JSONName: "count", TypeID: wltypes.TUint64},
			{GoName: "Optional", JSONName: "optional", TypeID: wltypes.TString, OmitEmpty: true},
		},
	}
	descriptor.Fingerprint = wltypes.DescriptorFingerprint(descriptor)
	pointerDescriptor := wltypes.TypeDescriptor{
		ID: "*example.CreateResult", LanguageName: "*example.CreateResult", Kind: wltypes.TypeKindPointer, Elem: descriptor.ID,
	}
	pointerDescriptor.Fingerprint = wltypes.DescriptorFingerprint(pointerDescriptor)
	reg := wlregistry.New()
	if err := reg.BindDescriptors(map[string]wltypes.TypeDescriptor{
		descriptor.ID: descriptor, pointerDescriptor.ID: pointerDescriptor,
	}); err != nil {
		t.Fatalf("bind descriptors: %v", err)
	}
	payload := protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"record_id":"rec-1","count":18446744073709551615}`)}

	value, err := executor.DecodeUpdatePayloadValue(reg, payload, descriptor.LanguageName)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	record, ok := wltypes.AsRecord(value)
	if !ok {
		t.Fatalf("decoded carrier = %T", value.Go())
	}
	count, ok := record.Field("Count")
	if !ok || count.Go() != uint64(18446744073709551615) {
		t.Fatalf("count = %#v", count.Go())
	}

	roundTrip, err := executor.EncodeUpdatePayloadValue(reg, value)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got, want := string(roundTrip.Data), `{"count":18446744073709551615,"record_id":"rec-1"}`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}
	pointerValue, err := executor.DecodeUpdatePayloadValue(reg, payload, pointerDescriptor.LanguageName)
	if err != nil {
		t.Fatalf("decode pointer: %v", err)
	}
	pointerRoundTrip, err := executor.EncodeUpdatePayloadValue(reg, pointerValue)
	if err != nil {
		t.Fatalf("encode pointer: %v", err)
	}
	if got, want := string(pointerRoundTrip.Data), `{"count":18446744073709551615,"record_id":"rec-1"}`; got != want {
		t.Fatalf("pointer payload = %s, want %s", got, want)
	}
}

func TestDecodeUpdateHandlerArgumentsEnforcesArity(t *testing.T) {
	reg := wlregistry.New()
	arg, err := protocol.NewJSONPayload("rec-1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.DecodeUpdateHandlerArguments(reg, []string{"string", "int64"}, &protocol.Payloads{arg})
	if err == nil || !stringContains(err.Error(), "expects 2 arguments, got 1") {
		t.Fatalf("error = %v", err)
	}
}

func TestUpdateHandlerPayloadCodecSupportsCollections(t *testing.T) {
	reg := wlregistry.New()
	bindUpdateCodecCollectionDescriptors(t, reg)
	payload := protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`[1,2,18446744073709551615]`)}
	value, err := executor.DecodeUpdatePayloadValue(reg, payload, "array<uint64>")
	if err != nil {
		t.Fatalf("decode array: %v", err)
	}
	roundTrip, err := executor.EncodeUpdatePayloadValue(reg, value)
	if err != nil {
		t.Fatalf("encode array: %v", err)
	}
	if got, want := string(roundTrip.Data), `[1,2,18446744073709551615]`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}

	mapPayload := protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"left":"L","right":"R"}`)}
	mapValue, err := executor.DecodeUpdatePayloadValue(reg, mapPayload, "map<string,string>")
	if err != nil {
		t.Fatalf("decode map: %v", err)
	}
	mapRoundTrip, err := executor.EncodeUpdatePayloadValue(reg, mapValue)
	if err != nil {
		t.Fatalf("encode map: %v", err)
	}
	if got, want := string(mapRoundTrip.Data), `{"left":"L","right":"R"}`; got != want {
		t.Fatalf("payload = %s, want %s", got, want)
	}
}

func TestUpdateHandlerPayloadCodecUsesRegistryDescriptorSemantics(t *testing.T) {
	reg := wlregistry.New()
	bindUpdateCodecCollectionDescriptors(t, reg)

	for _, typeName := range []string{"array<string>", "flows.Names", "map<string,string>", "flows.Labels"} {
		value, err := executor.DecodeUpdatePayloadValue(reg, protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`null`)}, typeName)
		if err != nil {
			t.Fatalf("decode null %s: %v", typeName, err)
		}
		if value.Go() != nil {
			t.Fatalf("decode null %s = %#v", typeName, value.Go())
		}
		payload, err := executor.EncodeUpdatePayloadValue(reg, value)
		if err != nil {
			t.Fatalf("encode null %s: %v", typeName, err)
		}
		if string(payload.Data) != "null" {
			t.Fatalf("encode null %s = %s", typeName, payload.Data)
		}
	}

	labels, err := executor.DecodeUpdatePayloadValue(reg, protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"primary":"ready"}`)}, "flows.Labels")
	if err != nil {
		t.Fatalf("decode named-string map: %v", err)
	}
	encoded, err := executor.EncodeUpdatePayloadValue(reg, labels)
	if err != nil {
		t.Fatalf("encode named-string map: %v", err)
	}
	if string(encoded.Data) != `{"primary":"ready"}` {
		t.Fatalf("named-string map = %s", encoded.Data)
	}

	recordDescriptor := wltypes.TypeDescriptor{
		ID: "flows.Record", LanguageName: "flows.Record", Kind: wltypes.TypeKindRecord,
		Fields: []wltypes.FieldDescriptor{
			{GoName: "Visible", JSONName: "visible", TypeID: wltypes.TString},
			{GoName: "Ignored", JSONName: "-", TypeID: wltypes.TString},
		},
	}
	recordDescriptor.Fingerprint = wltypes.DescriptorFingerprint(recordDescriptor)
	if err := reg.BindDescriptors(map[string]wltypes.TypeDescriptor{recordDescriptor.ID: recordDescriptor}); err != nil {
		t.Fatal(err)
	}
	record, err := executor.DecodeUpdatePayloadValue(reg, protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: []byte(`{"visible":"yes","-":"discard"}`)}, "flows.Record")
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	roundTrip, err := executor.EncodeUpdatePayloadValue(reg, record)
	if err != nil {
		t.Fatalf("encode record: %v", err)
	}
	if string(roundTrip.Data) != `{"visible":"yes"}` {
		t.Fatalf("json:- field leaked: %s", roundTrip.Data)
	}
}

func bindUpdateCodecCollectionDescriptors(t *testing.T, reg *wlregistry.Registry) {
	t.Helper()
	descriptors := map[string]wltypes.TypeDescriptor{
		"flows.Names": {
			ID: "flows.Names", LanguageName: "flows.Names", Kind: wltypes.TypeKindSlice, Elem: wltypes.TString,
		},
		"flows.Key": {
			ID: "flows.Key", LanguageName: "flows.Key", Kind: wltypes.TypeKindAlias, Underlying: wltypes.TString,
		},
		"flows.Labels": {
			ID: "flows.Labels", LanguageName: "flows.Labels", Kind: wltypes.TypeKindMap, Key: "flows.Key", Elem: wltypes.TString,
		},
	}
	for id, descriptor := range descriptors {
		descriptor.Fingerprint = wltypes.DescriptorFingerprint(descriptor)
		descriptors[id] = descriptor
	}
	if err := reg.BindDescriptors(descriptors); err != nil {
		t.Fatalf("bind collection descriptors: %v", err)
	}
}
