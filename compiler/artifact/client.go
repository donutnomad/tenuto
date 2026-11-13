package artifact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// 两个提交入口，请求体相同。
const (
	// RegisterPath 只登记产物，不改变 namespace 内该 workflow 的 active 版本。
	RegisterPath = "/runtime/workflow/artifacts"
	// PublishPath 登记并激活为 latest。
	PublishPath = "/runtime/workflow/register"
)

// Client 向 runtime HTTP API 提交产物。
type Client struct {
	BaseURL   string
	Namespace string
	HTTP      *http.Client
	// AuthToken 非空时请求带 `Authorization: Bearer <AuthToken>`（Server 配置了 WORKFLOW_HTTP_AUTH_TOKEN）。
	AuthToken string
}

// Submit 把 workflow.json 提交到 path（RegisterPath 或 PublishPath）。两个入口都只接收
// namespace 与 workflow，目录里的其他产物文件不上传。
func (c Client) Submit(ctx context.Context, path string, artifacts Artifacts) ([]byte, error) {
	return c.post(ctx, path, map[string]any{
		"namespace": c.Namespace,
		"workflow":  artifacts.Workflow,
	})
}

// post 发送 JSON 请求；非 2xx 把响应体一起放进错误。
func (c Client) post(ctx context.Context, path string, body any) ([]byte, error) {
	base := strings.TrimSuffix(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return nil, fmt.Errorf("未指定服务端 URL")
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("构造请求失败: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if c.AuthToken != "" {
		request.Header.Set("Authorization", "Bearer "+c.AuthToken)
	}
	client := c.HTTP
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("请求 %s 失败: %w", path, err)
	}
	defer func() { _ = response.Body.Close() }()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("读取 %s 响应失败: %w", path, err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return data, fmt.Errorf("%s 返回 %d: %s", path, response.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}
