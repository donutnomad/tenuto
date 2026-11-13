package interpreter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/executor"
	"github.com/donutnomad/tenuto/program"
	"github.com/donutnomad/tenuto/protocol/history"
	wlruntime "github.com/donutnomad/wlang/runtime"
	wlangtypes "github.com/donutnomad/wlang/types"
)

func TestReplayWLangForDebug_UsesExecutorWaitGroupLanes(t *testing.T) {
	nodes := parallelGoProgram(
		map[string]program.ActivityRef{
			"activities.A": {Symbol: "activities.A", Type: "activities.A"},
			"activities.B": {Symbol: "activities.B", Type: "activities.B"},
			"activities.C": {Symbol: "activities.C", Type: "activities.C"},
		},
		laneActivityFn("activities.A", "a"),
		laneActivityFn("activities.B", "b"),
		laneActivityFn("activities.C", "c"),
	)
	node := nodes
	raw, err := json.Marshal(node.Program)
	if err != nil {
		t.Fatal(err)
	}

	events := []history.Event{
		history.NewWorkflowStartedEvent(nil, "", "", "", "", 0),
		{ID: 2, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/0#a1", ActivityType: "activities.A"}},
		{ID: 3, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/1#a1", ActivityType: "activities.B"}},
		{ID: 4, Timestamp: time.Now(), ActivityScheduled: &history.ActivityScheduledEvent{ActivityID: "/2#a1", ActivityType: "activities.C"}},
		{ID: 5, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 2, Output: map[string]any{"lane": "a"}}},
		{ID: 6, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 3, Output: map[string]any{"lane": "b"}}},
		{ID: 7, Timestamp: time.Now(), ActivityCompleted: &history.ActivityCompletedEvent{ScheduledEventID: 4, Output: map[string]any{"lane": "c"}}},
	}

	routineIDs := map[uint64]bool{}
	effects := make([]executor.DebugEffect, 0)
	_, err = ReplayWLangForDebug(context.Background(), raw, events, func(info wlruntime.StepInfo) error {
		routineIDs[info.RoutineID] = true
		return nil
	}, func(effect executor.DebugEffect) {
		effects = append(effects, effect)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{0, 1, 2, 3} {
		if !routineIDs[id] {
			t.Fatalf("routine %d missing from debug steps: %v", id, routineIDs)
		}
	}
	phaseByStep := make(map[string]map[string]executor.DebugEffect)
	for _, effect := range effects {
		if phaseByStep[effect.ActivityID] == nil {
			phaseByStep[effect.ActivityID] = make(map[string]executor.DebugEffect)
		}
		phaseByStep[effect.ActivityID][effect.Phase] = effect
	}
	for stepID, scheduledEventID := range map[string]int64{"/0#a1": 2, "/1#a1": 3, "/2#a1": 4} {
		for _, phase := range []string{"scheduled", "awaited", "resolved"} {
			effect, ok := phaseByStep[stepID][phase]
			if !ok {
				t.Fatalf("effect %s/%s missing: %+v", stepID, phase, effects)
			}
			if effect.Kind != "activity" || effect.ScheduledEventID != scheduledEventID {
				t.Fatalf("effect %s/%s = %+v", stepID, phase, effect)
			}
			if effect.RoutineID == 0 {
				t.Fatalf("effect %s/%s lost routine ID: %+v", stepID, phase, effect)
			}
		}
	}
}

func TestReplayWLangForDebugBindsTypeDescriptorsAndChecksHandlerABI(t *testing.T) {
	descriptor := wlangtypes.TypeDescriptor{
		ID:           "example.Result",
		LanguageName: "example.Result",
		Kind:         wlangtypes.TypeKindRecord,
	}
	descriptor.Fingerprint = wlangtypes.DescriptorFingerprint(descriptor)
	artifact := program.WLangProgram{
		Schema: program.WLangProgramSchemaV2,
		Lang:   program.WLangLanguageV1,
		Program: []any{
			map[string]any{"let": map[string]any{"result": map[string]any{"zero": "example.Result"}}},
			map[string]any{"return": map[string]any{"literal": map[string]any{"type": "null", "value": nil}}},
		},
		Imports:          map[string]string{"workflow": "github.com/donutnomad/tenuto/sdk/workflow"},
		Types:            map[string]program.WLangTypeDescriptor{descriptor.ID: descriptor},
		HandlerABISchema: program.WLangHandlerABISchemaV1,
	}
	raw, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayWLangForDebug(context.Background(), raw, nil, nil); err != nil {
		t.Fatal(err)
	}

	artifact.HandlerABISchema = "workflow/update-handler-abi/v999"
	raw, err = json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayWLangForDebug(context.Background(), raw, nil, nil); err == nil {
		t.Fatal("debug replay accepted unsupported handler ABI schema")
	}
}
