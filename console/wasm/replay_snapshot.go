package main

// replay_snapshot.go implements snapshotValue: a marshal-safe, deep-copied
// rendering of a runtime value for the trace debugger. Unlike valueToAny it
// special-cases non-serializable values (routine handles holding chan/sync.Once,
// closures, OutArg carriers, and mock futures holding back-pointers to the API)
// BEFORE the json.Marshal fallback, and deep-copies containers so each OnStep
// snapshot is independent (required for correct time-travel).

import (
	"encoding/json"
	"strings"

	"github.com/donutnomad/wlang/runtime"
	"github.com/donutnomad/wlang/types"
)

// snapshotValue converts a runtime value into a JSON-serializable, deep-copied
// snapshot. Handles/closures/futures become opaque placeholders so marshaling
// never panics and later mutations of the underlying value cannot pollute an
// already-captured snapshot.
func snapshotValue(v types.Value) any {
	if v.IsNull() {
		return nil
	}
	// Type-name driven special cases take priority over the Go value, matching
	// how wlang tags these values (RoutineHandleType, func<...> closures).
	switch {
	case v.TypeName() == runtime.RoutineHandleType:
		return routineHandlePlaceholder(v)
	case strings.HasPrefix(v.TypeName(), "func<"):
		return map[string]any{"__type": "closure", "sig": v.TypeName()}
	}
	return snapshotAny(v.Go())
}

// snapshotAny renders a raw Go value (which may itself be a types.Value nested
// inside a container) into a marshal-safe, deep-copied snapshot.
func snapshotAny(raw any) any {
	switch val := raw.(type) {
	case nil:
		return nil

	// A nested types.Value inside a container: recurse through snapshotValue so
	// handles/closures nested in maps/slices are also placeholdered.
	case types.Value:
		return snapshotValue(val)

	// Non-serializable runtime carriers → opaque placeholders.
	case types.OutArg:
		return map[string]any{"__type": "outArg", "name": val.Name, "typeName": val.TypeName}
	case *types.OutArg:
		return map[string]any{"__type": "outArg", "name": val.Name, "typeName": val.TypeName}

	// Scalars are immutable, copy by value.
	case bool, string, float32, float64,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return val

	// Containers: deep-copy and recurse so nested handles are placeholdered and
	// later in-place mutations cannot pollute this snapshot.
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, item := range val {
			out[k] = snapshotAny(item)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = snapshotAny(item)
		}
		return out
	}

	// Fallback: marshal round-trip, which both serializes and deep-copies. If
	// the value is not marshalable, degrade to an opaque placeholder rather than
	// panicking or dropping the step.
	b, err := json.Marshal(raw)
	if err != nil {
		return map[string]any{"__type": "opaque"}
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return map[string]any{"__type": "opaque"}
	}
	return out
}

// routineHandlePlaceholder renders a routine handle. When the handle can be
// associated with a lane, the caller may enrich it; here we only guarantee a
// marshal-safe opaque tag.
func routineHandlePlaceholder(v types.Value) any {
	return map[string]any{"__type": "routineHandle"}
}
