package registration

import (
	"testing"
	"testing/fstest"
)

func TestDefinitionLoad(t *testing.T) {
	definition := Definition{FS: fstest.MapFS{
		ArtifactFile: &fstest.MapFile{Data: []byte(`{"id":"startup-greeting","name":"Greeting"}`)},
	}}
	id, data, err := definition.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if id != "startup-greeting" {
		t.Fatalf("workflow id = %q", id)
	}
	if len(data) == 0 {
		t.Fatal("workflow json is empty")
	}
}

func TestDefinitionLoadBad(t *testing.T) {
	cases := map[string]Definition{
		"missing filesystem": {},
		"missing workflow.json": {FS: fstest.MapFS{
			"wlang.json": &fstest.MapFile{Data: []byte(`{}`)},
		}},
		"invalid json": {FS: fstest.MapFS{
			ArtifactFile: &fstest.MapFile{Data: []byte(`{`)},
		}},
		"empty id": {FS: fstest.MapFS{
			ArtifactFile: &fstest.MapFile{Data: []byte(`{"id":""}`)},
		}},
	}
	for name, definition := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, err := definition.Load(); err == nil {
				t.Fatal("Load should fail")
			}
		})
	}
}
