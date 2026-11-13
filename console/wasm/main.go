//go:build js && wasm

package main

// main.go is the syscall/js bridge. All platform-neutral replay logic lives in
// replay_core.go (no build tag) so it can be unit-tested with a normal go test.

import (
	"encoding/json"
	"fmt"
	"sync"
	"syscall/js"

	"github.com/donutnomad/wlang/wflang"
)

const wasmVersion = "2026-07-13.v4"

func main() {
	js.Global().Set("initReplay", js.FuncOf(jsInitReplay))
	js.Global().Set("stepTo", js.FuncOf(jsStepTo))
	js.Global().Set("getPseudocode", js.FuncOf(jsGetPseudocode))
	js.Global().Set("traceReplay", js.FuncOf(jsTraceReplay))
	js.Global().Set("wasmVersion", js.FuncOf(func(_ js.Value, _ []js.Value) any {
		return wasmVersion
	}))

	// Block forever so the WASM module stays alive.
	select {}
}

// ── JS API ──

func jsInitReplay(_ js.Value, args []js.Value) any {
	if len(args) < 2 {
		return errResult("initReplay requires 2 arguments: programJSON, eventsJSON")
	}
	programJSON := args[0].String()
	eventsJSON := args[1].String()

	h, err := newReplayHandle([]byte(programJSON), []byte(eventsJSON))
	if err != nil {
		return errResult(err.Error())
	}

	handlesMu.Lock()
	id := nextHandleID
	nextHandleID++
	handles[id] = h
	handlesMu.Unlock()

	result := map[string]any{"handle": id}
	return toJSValue(result)
}

func jsStepTo(_ js.Value, args []js.Value) any {
	if len(args) < 2 {
		return errResult("stepTo requires 2 arguments: handle, eventIndex")
	}
	id := args[0].Int()
	eventIndex := args[1].Int()

	handlesMu.Lock()
	h, ok := handles[id]
	handlesMu.Unlock()
	if !ok {
		return errResult(fmt.Sprintf("invalid handle: %d", id))
	}

	state := h.stepTo(eventIndex)
	return toJSValue(state)
}

func jsGetPseudocode(_ js.Value, args []js.Value) any {
	if len(args) < 1 {
		return errResult("getPseudocode requires 1 argument: programJSON")
	}
	src := args[0].String()
	code, err := wflang.FormatPseudoCode([]byte(src))
	if err != nil {
		return errResult(err.Error())
	}
	return js.ValueOf(string(code))
}

func jsTraceReplay(_ js.Value, args []js.Value) any {
	if len(args) < 2 {
		return errResult("traceReplay requires 2 arguments: programJSON, eventsJSON")
	}
	programJSON := args[0].String()
	eventsJSON := args[1].String()

	result, err := buildTrace([]byte(programJSON), []byte(eventsJSON))
	if err != nil {
		return errResult(err.Error())
	}
	return toJSValue(result)
}

// ── Handle Registry ──

var (
	handlesMu    sync.Mutex
	handles      = map[int]*replayHandle{}
	nextHandleID = 1
)

// ── Helpers ──

func errResult(msg string) any {
	return toJSValue(map[string]any{"error": msg})
}

func toJSValue(v any) any {
	b, err := json.Marshal(v)
	if err != nil {
		return js.ValueOf(fmt.Sprintf(`{"error":"marshal: %s"}`, err.Error()))
	}
	return js.Global().Get("JSON").Call("parse", string(b))
}
