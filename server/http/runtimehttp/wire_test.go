package runtimehttp

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/server/biz/domain/batch"
	"github.com/donutnomad/tenuto/server/biz/domain/execution"
	"github.com/donutnomad/tenuto/server/biz/domain/schedule"
	"github.com/donutnomad/tenuto/server/biz/view/executionview"
)

// TestWireMappingKeepsJSON 固定逐字段转换的应答：每个导出字段都填上非零值，
// 线上形状的 JSON 与服务端类型原本的 JSON 逐字节相同。服务端类型新增字段而转换漏写时这里失败。
func TestWireMappingKeepsJSON(t *testing.T) {
	var run execution.Run
	fillExported(0, reflect.ValueOf(&run).Elem())
	var state execution.ActivityState
	fillExported(0, reflect.ValueOf(&state).Elem())
	var sched schedule.Schedule
	fillExported(0, reflect.ValueOf(&sched).Elem())
	var op batch.Operation
	fillExported(0, reflect.ValueOf(&op).Elem())
	var list execution.RunListResult
	fillExported(0, reflect.ValueOf(&list).Elem())
	var withBlocked executionview.RunWithBlocked
	fillExported(0, reflect.ValueOf(&withBlocked).Elem())
	var query execution.WorkflowQueryResult
	fillExported(0, reflect.ValueOf(&query).Elem())

	wiredList, _ := runListResultToWire(list, nil)
	wiredWithBlocked, _ := runWithBlockedToWire(&withBlocked, nil)
	wiredQuery, _ := queryResultToWire(query, nil)
	cases := []struct {
		name         string
		domain, wire any
	}{
		{"run", &run, runToWire(&run)},
		{"activity state", &state, activityStateToWire(&state)},
		{"schedule", &sched, scheduleToWire(&sched)},
		{"batch operation", &op, batchOperationToWire(&op)},
		{"run list", list, wiredList},
		{"run with blocked", &withBlocked, wiredWithBlocked},
		{"query result", query, wiredQuery},
	}
	for _, tc := range cases {
		want, err := json.Marshal(tc.domain)
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(tc.wire)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s:\nwire   %s\ndomain %s", tc.name, got, want)
		}
	}
}

// fillExported 给 v 的导出字段填上非零值；切片与 map 各放一个元素。
// 自引用的类型（如 protocol.Failure.Cause）只展开几层。
func fillExported(depth int, v reflect.Value) {
	if depth > 8 {
		return
	}
	if v.Type() == reflect.TypeOf(time.Time{}) {
		v.Set(reflect.ValueOf(time.Date(2026, 11, 11, 1, 2, 3, 0, time.UTC)))
		return
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString("x")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(7)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1.5)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		fillExported(depth+1, v.Elem())
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillExported(depth+1, v.Index(0))
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		key := reflect.New(v.Type().Key()).Elem()
		fillExported(depth+1, key)
		value := reflect.New(v.Type().Elem()).Elem()
		if value.Kind() == reflect.Interface {
			value.Set(reflect.ValueOf("x"))
		} else {
			fillExported(depth+1, value)
		}
		v.SetMapIndex(key, value)
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fillExported(depth+1, v.Field(i))
			}
		}
	}
}
