package executor

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/donutnomad/tenuto/protocol"
	wlregistry "github.com/donutnomad/wlang/registry"
	wlruntime "github.com/donutnomad/wlang/runtime"
	wltypes "github.com/donutnomad/wlang/types"
)

type UpdateHandlerABIKind string

const (
	UpdateHandlerABITemporalError       UpdateHandlerABIKind = "temporal-error-v1"
	UpdateHandlerABITemporalResultError UpdateHandlerABIKind = "temporal-result-error-v1"
)

type UpdateHandlerABI struct {
	Kind               UpdateHandlerABIKind
	BusinessParamTypes []string
	ResultType         string
}

type UpdateHandlerTypeResolver interface {
	Descriptor(name string) (wltypes.TypeDescriptor, bool)
}

func ClassifyUpdateHandlerSignature(sig wlruntime.FunctionSignature, resolver UpdateHandlerTypeResolver) (UpdateHandlerABI, error) {
	if len(sig.Params) == 0 || sig.Params[0] != "workflow.Context" {
		return UpdateHandlerABI{}, fmt.Errorf("update handler first parameter must be workflow.Context")
	}
	businessParams := append([]string(nil), sig.Params[1:]...)
	for i, typeName := range businessParams {
		if !isSerializableUpdateType(typeName, resolver) {
			return UpdateHandlerABI{}, fmt.Errorf("update handler parameter %d has unsupported type %q", i+1, typeName)
		}
	}
	switch {
	case len(sig.Returns) == 1 && sig.Returns[0] == wltypes.TError:
		return UpdateHandlerABI{Kind: UpdateHandlerABITemporalError, BusinessParamTypes: businessParams}, nil
	case len(sig.Returns) == 2 && sig.Returns[1] == wltypes.TError:
		if !isSerializableUpdateType(sig.Returns[0], resolver) {
			return UpdateHandlerABI{}, fmt.Errorf("update handler result has unsupported type %q", sig.Returns[0])
		}
		return UpdateHandlerABI{Kind: UpdateHandlerABITemporalResultError, BusinessParamTypes: businessParams, ResultType: sig.Returns[0]}, nil
	default:
		return UpdateHandlerABI{}, fmt.Errorf("update handler must return error or (result, error)")
	}
}

func isSerializableUpdateType(typeName string, resolver UpdateHandlerTypeResolver) bool {
	switch typeName {
	case wltypes.TString, wltypes.TBoolean,
		wltypes.TInt8, wltypes.TInt16, wltypes.TInt32, wltypes.TInt64,
		wltypes.TUint8, wltypes.TUint16, wltypes.TUint32, wltypes.TUint64,
		wltypes.TFloat32, wltypes.TFloat64, wltypes.TAny, wltypes.TNull:
		return true
	}
	if elemType, ok := splitGenericType(typeName, "array"); ok {
		return isSerializableUpdateType(elemType, resolver)
	}
	if keyType, elemType, ok := splitMapType(typeName); ok {
		return keyType == wltypes.TString && isSerializableUpdateType(elemType, resolver)
	}
	if resolver != nil {
		_, ok := resolver.Descriptor(typeName)
		return ok
	}
	return false
}

func DecodeUpdateHandlerArguments(reg *wlregistry.Registry, paramTypes []string, args *protocol.Payloads) ([]wltypes.Value, error) {
	var payloads protocol.Payloads
	if args != nil {
		payloads = *args
	}
	if len(payloads) != len(paramTypes) {
		return nil, fmt.Errorf("update handler expects %d arguments, got %d", len(paramTypes), len(payloads))
	}
	values := make([]wltypes.Value, len(paramTypes))
	for i, typeName := range paramTypes {
		value, err := DecodeUpdatePayloadValue(reg, payloads[i], typeName)
		if err != nil {
			return nil, fmt.Errorf("decode update argument %d as %s: %w", i+1, typeName, err)
		}
		values[i] = value
	}
	return values, nil
}

func DecodeUpdatePayloadValue(reg *wlregistry.Registry, payload protocol.Payload, typeName string) (wltypes.Value, error) {
	if err := payload.Validate(); err != nil {
		return wltypes.Value{}, err
	}
	return DecodeTypedJSON(reg, typeName, json.RawMessage(payload.Data))
}

func EncodeUpdatePayloadValue(reg *wlregistry.Registry, value wltypes.Value) (protocol.Payload, error) {
	raw, err := reg.EncodeJSONValue(value)
	if err != nil {
		return protocol.Payload{}, err
	}
	payload := protocol.Payload{Encoding: protocol.PayloadEncodingJSONPlainV1, Data: append([]byte(nil), raw...)}
	return protocol.CanonicalPayload(payload)
}

func splitGenericType(typeName, prefix string) (string, bool) {
	want := prefix + "<"
	if !strings.HasPrefix(typeName, want) || !strings.HasSuffix(typeName, ">") {
		return "", false
	}
	return typeName[len(want) : len(typeName)-1], true
}

func splitMapType(typeName string) (string, string, bool) {
	body, ok := splitGenericType(typeName, "map")
	if !ok {
		return "", "", false
	}
	parts := strings.SplitN(body, ",", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}
