package runtimeclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRuntimePostKeepsExactNumbersInResponses(t *testing.T) {
	// 应答里的用户载荷（Query 结果、Update 结果、Activity 输入）保留数字的精确值。
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"result":{"amount":9007199254740993,"rate":0.25}}`))
	}))
	defer server.Close()
	var resp struct {
		Result map[string]any `json:"result"`
	}
	if err := runtimePost(context.Background(), server.Client(), server.URL, "/", "test api", nil, map[string]any{}, &resp); err != nil {
		t.Fatal(err)
	}
	if got := resp.Result["amount"]; got != int64(9007199254740993) {
		t.Fatalf("amount = %#v", got)
	}
	if got := resp.Result["rate"]; got != 0.25 {
		t.Fatalf("rate = %#v", got)
	}
}
