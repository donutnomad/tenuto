package program

import (
	"errors"
	"testing"

	wlangtypes "github.com/donutnomad/wlang/types"
)

func TestWLangProgramRejectsMissingSchema(t *testing.T) {
	program := WLangProgram{Program: []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}}}}
	if err := program.ValidateArtifact(); err == nil {
		t.Fatal("program without schema must be rejected")
	}
	if err := program.ValidateCapabilities(CurrentWLangCapabilities()); err == nil {
		t.Fatal("program without schema must not match worker capabilities")
	}
}

func TestWLangProgramV2CapabilitiesAndValidation(t *testing.T) {
	program := WLangProgram{
		Schema:           WLangProgramSchemaV2,
		Lang:             WLangLanguageV1,
		Program:          []any{map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}}},
		Imports:          map[string]string{"workflow": "github.com/donutnomad/tenuto/sdk/workflow"},
		HandlerABISchema: WLangHandlerABISchemaV1,
	}
	if err := program.ValidateArtifact(); err != nil {
		t.Fatal(err)
	}
	if err := program.ValidateCapabilities(CurrentWLangCapabilities()); err != nil {
		t.Fatal(err)
	}
	var unsupported *UnsupportedWLangCapabilityError
	if err := program.ValidateCapabilities([]string{}); !errors.As(err, &unsupported) {
		t.Fatalf("expected capability error, got %v", err)
	}
	program.Schema = "wlang/program/v999"
	unsupported = nil
	if err := program.ValidateCapabilities(CurrentWLangCapabilities()); !errors.As(err, &unsupported) {
		t.Fatalf("expected future schema capability error, got %v", err)
	}
	program.Schema = WLangProgramSchemaV2
	program.HandlerABISchema = "workflow/update-handler-abi/v999"
	if err := program.ValidateArtifact(); err == nil {
		t.Fatal("unknown handler ABI schema accepted")
	}
	program.HandlerABISchema = WLangHandlerABISchemaV1
	program.Types = map[string]WLangTypeDescriptor{
		"example.Bad": {
			ID:           "example.Bad",
			LanguageName: "example.Bad",
			Kind:         wlangtypes.TypeKindRecord,
		},
	}
	if err := program.ValidateArtifact(); err == nil {
		t.Fatal("descriptor without fingerprint accepted")
	}
}

func TestWLangProgramContentVersionCoversWholeArtifact(t *testing.T) {
	base := WLangProgram{
		Schema:  WLangProgramSchemaV2,
		Lang:    WLangLanguageV1,
		Program: []any{map[string]any{"return": map[string]any{"var": "result"}}},
		Imports: map[string]string{"b": "example.com/b", "a": "example.com/a"},
		Types: map[string]WLangTypeDescriptor{
			"example.Result": descriptorWithFingerprint(wlangtypes.TypeDescriptor{
				ID:           "example.Result",
				LanguageName: "example.Result",
				Kind:         wlangtypes.TypeKindRecord,
				Fields:       []wlangtypes.FieldDescriptor{{GoName: "Value", JSONName: "value", TypeID: "string"}},
			}),
		},
		HandlerABISchema: WLangHandlerABISchemaV1,
	}
	version, err := programVersion(base)
	if err != nil {
		t.Fatal(err)
	}
	reordered := base
	reordered.Imports = map[string]string{"a": "example.com/a", "b": "example.com/b"}
	same, err := programVersion(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if same != version {
		t.Fatalf("map order changed version: %s != %s", same, version)
	}

	for name, mutate := range map[string]func(*WLangProgram){
		"program": func(p *WLangProgram) {
			p.Program = append(p.Program, map[string]any{"expr": map[string]any{"var": "x"}})
		},
		"imports": func(p *WLangProgram) { p.Imports = map[string]string{"a": "example.com/changed"} },
		"types": func(p *WLangProgram) {
			p.Types = map[string]WLangTypeDescriptor{
				"example.Result": descriptorWithFingerprint(wlangtypes.TypeDescriptor{
					ID:           "example.Result",
					LanguageName: "example.Result",
					Kind:         wlangtypes.TypeKindRecord,
					Fields:       []wlangtypes.FieldDescriptor{{GoName: "Value", JSONName: "renamed", TypeID: "string"}},
				}),
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			got, versionErr := programVersion(changed)
			if versionErr != nil {
				t.Fatal(versionErr)
			}
			if got == version {
				t.Fatalf("%s change did not affect version", name)
			}
		})
	}
}

func descriptorWithFingerprint(descriptor wlangtypes.TypeDescriptor) wlangtypes.TypeDescriptor {
	descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
	return descriptor
}

// programVersion 是只含 p 的配置的内容版本；p 必须是有效产物。
func programVersion(p WLangProgram) (string, error) {
	if err := p.ValidateArtifact(); err != nil {
		return "", err
	}
	return WorkflowConfigVersion(&WorkflowConfig{Program: p})
}
