package client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/donutnomad/tenuto/protocol"
	"github.com/donutnomad/tenuto/protocol/history"
	"github.com/donutnomad/tenuto/sdk/internal/runtimeclient"
	tenutotel "github.com/donutnomad/tenuto/telemetry"
	"github.com/google/uuid"
)

// Options 配置 Dial 建立的 Client。
type Options struct {
	// HostPort 是 Server 的 runtime HTTP 地址，写成 "host:port" 或完整 URL（"https://tenuto.example.com"）。
	// 没有 scheme 时按 http 连接，配置了 TLS 时按 https 连接。必填。
	HostPort string
	// Namespace 是本 Client 全部调用作用的 namespace，空值为 "default"。
	Namespace string
	// Identity 标识调用方，缺省为 "<pid>@<hostname>"。sdk/worker 以它作为 Worker 的缺省 Identity。
	Identity string
	// HTTPClient 发送全部请求；nil 时 Dial 自建一个，Close 释放它的空闲连接。网关要求其他凭证（按请求
	// 刷新的 token、签名头）时在这里配置 Transport。
	HTTPClient *http.Client
	// AuthToken 非空时每个请求带 `Authorization: Bearer <AuthToken>`，对应 Server 的 WORKFLOW_HTTP_AUTH_TOKEN。
	// 与 HTTPClient 同时设置时加在 HTTPClient 的 Transport 之外。token 以明文随请求发送，生产环境配合 https 使用。
	AuthToken string
	// TLS 是 Dial 自建 HTTP 客户端的 TLS 配置：RootCAs 校验 Server 证书，Certificates 是 mTLS 的客户端证书
	// （对应 Server 的 WORKFLOW_HTTP_TLS_CLIENT_CA_FILE）。设置后 HostPort 缺省按 https 连接。与 HTTPClient
	// 互斥：自带 HTTPClient 时在它的 Transport 上配置 TLS。
	TLS *tls.Config
	// Telemetry 给 HTTP 请求与 Update 调用加 trace 与指标；零值不加。sdk/worker 未配置 Telemetry 时沿用它。
	Telemetry tenutotel.Providers
	// Retry 配置调用遇到瞬时错误（连接失败或中断、HTTP 429/502/503/504）时的重试，零值取缺省值。
	// 从这个 Client 创建的 Worker 的轮询与上报使用同一个策略。
	Retry RetryOptions
}

// RetryOptions 配置瞬时错误的重试，对应 Temporal Go SDK 的 gRPC 重试：指数退避加 ±20% 抖动；ctx 有 deadline 时
// 重试到 deadline 为止，没有 deadline 时从第一次失败起最多重试 MaximumElapsedTime。重发会再执行一次的调用
// （ResetWorkflowExecution、Schedule 的 Create 与 Trigger、StartBatchOperation）只在请求确定没有到达 Server
// （连接没有建立、HTTP 429/503）时重试。零值字段取缺省值。
type RetryOptions struct {
	// Disabled 关闭重试，每个调用只发一次。
	Disabled bool
	// InitialInterval 是第一次重试之前的等待，缺省 200ms。
	InitialInterval time.Duration
	// MaximumInterval 是两次尝试之间等待的上限，缺省 10s。
	MaximumInterval time.Duration
	// BackoffCoefficient 是每次重试等待的倍数，缺省 2。
	BackoffCoefficient float64
	// MaximumElapsedTime 是 ctx 没有 deadline 时从第一次失败起继续重试的时长，缺省 1 分钟。
	MaximumElapsedTime time.Duration
}

// Client 是应用代码启动、交互与管理 Workflow 的入口，对应 Temporal Go SDK 的 client.Client。
// 全部方法可以并发调用，并在 ctx 取消或到期时返回。
type Client interface {
	// ExecuteWorkflow 启动一个 Workflow 并返回它的 WorkflowRun。workflowType 是 `@Workflow(id=...)` 声明的
	// Workflow Type；args 至多一个，编码成 JSON 对象（结构体或 map）作为 Workflow 的输入。
	// 同一 Workflow ID 的 Run 仍在运行时返回 ErrWorkflowExecutionAlreadyStarted；上一个 Run 已结束时启动新 Run。
	ExecuteWorkflow(ctx context.Context, options StartWorkflowOptions, workflowType string, args ...any) (WorkflowRun, error)

	// GetWorkflow 返回已有执行的 WorkflowRun，不访问 Server。runID 为空时 Get 从 Workflow ID 的当前 Run 开始跟随。
	GetWorkflow(ctx context.Context, workflowID, runID string) WorkflowRun

	// SignalWorkflow 向 Run 发送 Signal。runID 为空时发给 Workflow ID 的当前 Run；arg 编码成 JSON 对象，可以为 nil。
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg any) error

	// SignalWithStartWorkflow 向 Workflow ID 的运行中 Run 发送 Signal；没有运行中的 Run 时用 workflowType 与
	// workflowArgs 启动一个，再把 Signal 投给它。两步在 Server 上原子完成。
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg any,
		options StartWorkflowOptions, workflowType string, workflowArgs ...any) (WorkflowRun, error)

	// CancelWorkflow 请求取消 Run。取消送达 Workflow 代码，终态由它的返回值决定（见 WorkflowRun.Get）。
	CancelWorkflow(ctx context.Context, workflowID, runID string) error

	// TerminateWorkflow 立即终止 Run，Workflow 代码不再运行。reason 写进 WorkflowTerminated 事件。
	TerminateWorkflow(ctx context.Context, workflowID, runID, reason string) error

	// QueryWorkflow 对 Run 执行一次 Query。Server 把历史重放到最近一个已完成的 Workflow Task 再调用
	// handler，运行中与已结束的 Run 都可以查询。args 至多一个，编码成 JSON 对象。handler 拒绝或查询类型
	// 不存在时返回 *QueryFailedError。
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...any) (EncodedValue, error)

	// UpdateWorkflow 发送 Update 并等到 options.WaitForStage，返回跟踪它结果的句柄。
	// validator 拒绝时返回的错误满足 errors.Is(err, ErrUpdateRejected)，并可 errors.As 到 *apperror.Error。
	UpdateWorkflow(ctx context.Context, options UpdateWorkflowOptions) (WorkflowUpdateHandle, error)

	// UpdateWithStartWorkflow 在 Workflow ID 的运行中 Run 上执行 Update，没有运行中的 Run 时先启动。
	// 启动与 Update 在 Server 上原子完成；启动结果经 options.StartWorkflowOperation.Get 读取。
	UpdateWithStartWorkflow(ctx context.Context, options UpdateWithStartWorkflowOptions) (WorkflowUpdateHandle, error)

	// NewWithStartWorkflowOperation 构造 UpdateWithStartWorkflow 使用的一次性启动规格。
	// options.WorkflowIDConflictPolicy 必须是 WorkflowIDConflictPolicyUseExisting。
	NewWithStartWorkflowOperation(options StartWorkflowOptions, workflowType string, args ...any) WithStartWorkflowOperation

	// DescribeWorkflowExecution 读取 Run 的当前状态。runID 为空时读取 Workflow ID 的当前 Run。
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*WorkflowExecutionDescription, error)

	// GetWorkflowHistory 返回 Run 历史事件的迭代器，按事件 ID 升序逐页读取。runID 为空时读取当前 Run。
	GetWorkflowHistory(ctx context.Context, workflowID, runID string) HistoryEventIterator

	// ListWorkflow 按条件列出本 namespace 的 Run。
	ListWorkflow(ctx context.Context, options ListWorkflowOptions) (*ListWorkflowResult, error)

	// ResetWorkflowExecution 从 Run 历史中的一个 Workflow Task 处分叉出新 Run，旧 Run 被终止。
	// 返回新 Run 的 ID。
	ResetWorkflowExecution(ctx context.Context, options ResetWorkflowExecutionOptions) (string, error)

	// UpsertSearchAttributes 写入 Run 的搜索属性：给出的键覆盖原值，其余键保留。属性写进
	// SearchAttributesUpserted 事件，ListWorkflow 与批量操作按它过滤。runID 为空时写入当前 Run。
	// 键 TemporalChangeVersion 由 workflow.GetVersion 维护，写入它返回 ErrInvalidArgument。
	UpsertSearchAttributes(ctx context.Context, workflowID, runID string, attributes map[string]string) error

	// UpdateWorkflowExecutionOptions 修改运行中 Run 的版本策略。
	UpdateWorkflowExecutionOptions(ctx context.Context, options UpdateWorkflowExecutionOptions) error

	// ScheduleClient 返回管理 Schedule 的客户端。
	ScheduleClient() ScheduleClient

	// StartBatchOperation 对一批 Run 执行同一个动作（取消、终止、重置）。Server 在后台处理，
	// 返回受理时的记录，用 DescribeBatchOperation 读取进度。
	StartBatchOperation(ctx context.Context, options StartBatchOperationOptions) (*BatchOperation, error)

	// DescribeBatchOperation 读取批量操作的状态与进度。
	DescribeBatchOperation(ctx context.Context, operationID string) (*BatchOperation, error)

	// ListBatchOperations 分页列出本 namespace 的批量操作。
	ListBatchOperations(ctx context.Context, options ListBatchOperationsOptions) (*ListBatchOperationsResult, error)

	// StopBatchOperation 取消 pending 或 running 的批量操作并返回最新记录；已处理的 Run 不回滚。
	StopBatchOperation(ctx context.Context, operationID string) (*BatchOperation, error)

	// Close 释放 Dial 自建的 HTTP 连接。先停止从它创建的 Worker，再 Close。
	Close()
}

// Dial 按 options 创建 Client。它不访问 Server：地址错误或 Server 不可达在第一次调用时报告。
func Dial(options Options) (Client, error) {
	if options.TLS != nil && options.HTTPClient != nil {
		return nil, errors.New("client: Options.TLS and Options.HTTPClient are mutually exclusive; configure TLS on the HTTPClient's Transport")
	}
	baseURL, err := normalizeHostPort(options.HostPort, options.TLS != nil)
	if err != nil {
		return nil, err
	}
	identity := options.Identity
	if identity == "" {
		identity = defaultIdentity()
	}
	namespace := protocol.NormalizeNamespace(options.Namespace)
	httpClient, owned := options.HTTPClient, false
	if httpClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = options.TLS.Clone()
		httpClient = &http.Client{Transport: transport}
		owned = true
	}
	requestClient := httpClient
	if options.AuthToken != "" {
		requestClient = withBearerToken(httpClient, options.AuthToken)
	}
	c := &workflowClient{
		conn: runtimeclient.NewConn(baseURL, requestClient, namespace, identity, runtimeclient.RuntimeHTTPClientOptions{
			Telemetry: options.Telemetry,
			Retry: runtimeclient.RetryPolicy{
				Disabled: options.Retry.Disabled, InitialInterval: options.Retry.InitialInterval,
				MaximumInterval: options.Retry.MaximumInterval, BackoffCoefficient: options.Retry.BackoffCoefficient,
				MaximumElapsed: options.Retry.MaximumElapsedTime,
			},
		}),
		namespace: namespace,
	}
	if owned {
		c.ownedHTTP = httpClient
	}
	return c, nil
}

func normalizeHostPort(hostPort string, useTLS bool) (string, error) {
	hostPort = strings.TrimSpace(hostPort)
	if hostPort == "" {
		return "", errors.New("client: Options.HostPort is required")
	}
	if !strings.Contains(hostPort, "://") {
		if useTLS {
			hostPort = "https://" + hostPort
		} else {
			hostPort = "http://" + hostPort
		}
	}
	parsed, err := url.Parse(hostPort)
	if err != nil {
		return "", fmt.Errorf("client: invalid Options.HostPort %q: %w", hostPort, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("client: Options.HostPort scheme must be http or https, got %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("client: Options.HostPort %q has no host", hostPort)
	}
	if useTLS && parsed.Scheme != "https" {
		return "", fmt.Errorf("client: Options.TLS requires an https HostPort, got %q", hostPort)
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

// withBearerToken 返回在 client 的 Transport 之外加上 Authorization 头的副本，client 本身不变。
func withBearerToken(client *http.Client, token string) *http.Client {
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	cloned := *client
	cloned.Transport = &bearerTransport{header: "Bearer " + token, base: base}
	return &cloned
}

// bearerTransport 给每个请求设置 Authorization 头。RoundTripper 不能修改传入的请求，因此先复制。
type bearerTransport struct {
	header string
	base   http.RoundTripper
}

func (t *bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", t.header)
	return t.base.RoundTrip(req)
}

// CloseIdleConnections 让 http.Client.CloseIdleConnections 传到底层 Transport。
func (t *bearerTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// newID 生成 SDK 一侧的 Workflow ID、Update ID 与 request ID（UUIDv7 文本，与 Server 生成的 ID 同形）。
func newID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}

func defaultIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%d@%s", os.Getpid(), host)
}

type workflowClient struct {
	conn      *runtimeclient.Conn
	namespace string
	// ownedHTTP 是 Dial 自建的 *http.Client；调用方传入的 HTTPClient 由调用方管理。
	ownedHTTP *http.Client
}

// RuntimeConn 把连接交给 sdk/worker。返回类型在 sdk/internal 下，包外代码无法经接口断言调用它。
func (c *workflowClient) RuntimeConn() *runtimeclient.Conn { return c.conn }

func (c *workflowClient) Close() {
	if c.ownedHTTP != nil {
		c.ownedHTTP.CloseIdleConnections()
	}
}

func (c *workflowClient) workflows() *runtimeclient.WorkflowRuntimeHTTPClient { return c.conn.Workflow }

// resolveRunID 在 runID 为空时读取 Workflow ID 的当前 Run。
func (c *workflowClient) resolveRunID(ctx context.Context, workflowID, runID string) (string, error) {
	if runID != "" {
		return runID, nil
	}
	if workflowID == "" {
		return "", errors.New("client: workflow ID or run ID is required")
	}
	run, err := c.getRun(ctx, workflowID, "")
	if err != nil {
		return "", err
	}
	return run.Run.ID, nil
}

// WorkflowExecutionStatus 是 Run 的状态。
type WorkflowExecutionStatus string

const (
	// WorkflowExecutionStatusRunning 表示 Run 正在运行。
	WorkflowExecutionStatusRunning = WorkflowExecutionStatus(history.StatusRunning)
	// WorkflowExecutionStatusCompleted 表示 Workflow 返回了结果。
	WorkflowExecutionStatusCompleted = WorkflowExecutionStatus(history.StatusCompleted)
	// WorkflowExecutionStatusFailed 表示 Workflow 返回了错误或超时，且不再重试。
	WorkflowExecutionStatusFailed = WorkflowExecutionStatus(history.StatusFailed)
	// WorkflowExecutionStatusCanceled 表示 Workflow 在取消请求之后返回了取消错误。
	WorkflowExecutionStatusCanceled = WorkflowExecutionStatus(history.StatusCancelled)
	// WorkflowExecutionStatusTerminated 表示 Run 被终止。
	WorkflowExecutionStatusTerminated = WorkflowExecutionStatus(history.StatusTerminated)
	// WorkflowExecutionStatusContinuedAsNew 表示 Run 以 continue-as-new 结束，执行在下一个 Run 继续。
	WorkflowExecutionStatusContinuedAsNew = WorkflowExecutionStatus(history.StatusContinuedAsNew)
)
