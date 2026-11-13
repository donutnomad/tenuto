package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/testcert"
	"github.com/donutnomad/tenuto/sdk/client"
	"github.com/donutnomad/tenuto/server/app/assembly"
	"github.com/donutnomad/tenuto/server/http/runtimehttp"
	"github.com/donutnomad/tenuto/server/infra/memory"
)

const sdkTestToken = "sdk-test-token-0123456789"

// authServer 是要求 mTLS 客户端证书与 Bearer token 的 runtime HTTP Server，记录每个请求的路径、
// Authorization 头与客户端证书的 CommonName。
type authServer struct {
	url        string
	ca         *testcert.CA
	handshakes atomic.Int64

	mu       sync.Mutex
	requests []authRequest
}

type authRequest struct {
	path, authorization, peer string
}

func newAuthTestEnv(t *testing.T) (*testEnv, *authServer) {
	t.Helper()
	env := &testEnv{t: t, executions: memory.NewExecutionManager()}
	env.server = assembly.NewServer(assembly.Options{
		Stores:        assembly.Stores{Executions: env.executions},
		WorkflowQueue: memory.NewTaskQueue(256), ActivityRouter: memory.NewActivityQueueRouter(256),
	})
	auth := &authServer{ca: testcert.NewCA(t, "sdk-test-ca")}
	runtime := runtimehttp.NewRuntimeHTTPHandler(env.server)
	serverCtx, stopServer := context.WithCancel(context.Background())
	httpServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization := r.Header.Get("Authorization")
		auth.mu.Lock()
		auth.requests = append(auth.requests, authRequest{r.URL.Path, authorization, r.TLS.PeerCertificates[0].Subject.CommonName})
		auth.mu.Unlock()
		if authorization != "Bearer "+sdkTestToken {
			w.Header().Set("WWW-Authenticate", `Bearer realm="tenuto"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		runtime.ServeHTTP(w, r)
	}))
	httpServer.Config.BaseContext = func(net.Listener) context.Context { return serverCtx }
	httpServer.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{auth.ca.Server(t).TLSCertificate(t)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    auth.ca.Pool(),
		VerifyConnection: func(tls.ConnectionState) error {
			auth.handshakes.Add(1)
			return nil
		},
	}
	httpServer.StartTLS()
	auth.url = httpServer.URL
	env.url = httpServer.URL
	t.Cleanup(func() {
		stopServer()
		httpServer.Close()
		env.server.Stop()
	})
	return env, auth
}

// clientTLS 返回信任测试 CA 的 TLS 配置；withCert 为 true 时带上 CommonName 为 sdk-client 的客户端证书。
func (a *authServer) clientTLS(t *testing.T, withCert bool) *tls.Config {
	config := &tls.Config{RootCAs: a.ca.Pool(), MinVersion: tls.VersionTLS12}
	if withCert {
		config.Certificates = []tls.Certificate{a.ca.Client(t, "sdk-client").TLSCertificate(t)}
	}
	return config
}

func (a *authServer) recorded() []authRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]authRequest(nil), a.requests...)
}

// Client 与从它创建的 Worker 的每个请求都带 Options.AuthToken 与 Options.TLS 的客户端证书。
func TestClientAndWorkerSendTokenAndClientCertificate(t *testing.T) {
	env, auth := newAuthTestEnv(t)
	hostPort := strings.TrimPrefix(auth.url, "https://") // 没有 scheme：配置了 TLS 时按 https 连接
	c, err := client.Dial(client.Options{HostPort: hostPort, Identity: "auth-test", AuthToken: sdkTestToken, TLS: auth.clientTLS(t, true)})
	must(t, err)
	t.Cleanup(c.Close)
	env.client = c
	env.startWorker()
	env.register("counting", strings.Replace(countingSrc, "THRESHOLD", "1", 1))

	var out struct {
		N int64 `json:"n"`
	}
	must(t, get(t, env.execute("counting", "auth-1"), &out))
	if out.N != 1 {
		t.Fatalf("result = %+v", out)
	}

	seen := map[string]bool{}
	for _, request := range auth.recorded() {
		if request.authorization != "Bearer "+sdkTestToken || request.peer != "sdk-client" {
			t.Fatalf("request %s: Authorization = %q, client certificate = %q", request.path, request.authorization, request.peer)
		}
		for _, prefix := range []string{"/runtime/workflow/", "/runtime/workflow-task/", "/runtime/activity/"} {
			if strings.HasPrefix(request.path, prefix) {
				seen[prefix] = true
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("authenticated request groups = %v, want client, workflow task and activity requests", seen)
	}
}

// 401 不重试：重试策略开启时，凭证错误的调用也只发一次，返回 ErrUnauthenticated。
func TestUnauthenticatedIsNotRetried(t *testing.T) {
	_, auth := newAuthTestEnv(t)
	for _, token := range []string{"", "wrong-token-0123456789"} {
		c, err := client.Dial(client.Options{HostPort: auth.url, AuthToken: token, TLS: auth.clientTLS(t, true)})
		must(t, err)
		before := len(auth.recorded())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		started := time.Now()
		err = c.SignalWorkflow(ctx, "any-workflow", "", "add", nil)
		cancel()
		c.Close()
		var serviceErr *client.ServiceError
		if !errors.Is(err, client.ErrUnauthenticated) || !errors.As(err, &serviceErr) || serviceErr.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %q: err = %v, want ErrUnauthenticated", token, err)
		}
		if attempts := len(auth.recorded()) - before; attempts != 1 || time.Since(started) > time.Second {
			t.Fatalf("token %q: %d attempts in %s, want exactly one", token, attempts, time.Since(started))
		}
	}
}

// Server 要求客户端证书时，没有证书的 Client 在 TLS 握手失败后不重试到超时。
func TestMissingClientCertificateIsNotRetried(t *testing.T) {
	_, auth := newAuthTestEnv(t)
	c, err := client.Dial(client.Options{HostPort: auth.url, AuthToken: sdkTestToken, TLS: auth.clientTLS(t, false)})
	must(t, err)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	if err := c.SignalWorkflow(ctx, "any-workflow", "", "add", nil); err == nil {
		t.Fatal("request without a client certificate succeeded")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("failed after %s (%d handshakes), want a prompt failure without retries", elapsed, auth.handshakes.Load())
	}
	if len(auth.recorded()) != 0 {
		t.Fatalf("requests reached the handler: %v", auth.recorded())
	}
}

func TestDialValidatesTLSOptions(t *testing.T) {
	if _, err := client.Dial(client.Options{HostPort: "127.0.0.1:7400", TLS: &tls.Config{}, HTTPClient: http.DefaultClient}); err == nil {
		t.Fatal("TLS together with HTTPClient was accepted")
	}
	if _, err := client.Dial(client.Options{HostPort: "http://127.0.0.1:7400", TLS: &tls.Config{}}); err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("TLS with an http HostPort: err = %v", err)
	}
}

// AuthToken 加在调用方 HTTPClient 的 Transport 之外，不修改调用方的 HTTPClient。
func TestAuthTokenWrapsCallerHTTPClient(t *testing.T) {
	var got atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization") + "|" + r.Header.Get("X-Gateway"))
		http.Error(w, "missing", http.StatusNotFound)
	}))
	defer server.Close()
	httpClient := &http.Client{Transport: headerTransport{name: "X-Gateway", value: "g1"}}
	c, err := client.Dial(client.Options{HostPort: server.URL, AuthToken: sdkTestToken, HTTPClient: httpClient})
	must(t, err)
	defer c.Close()
	_ = c.SignalWorkflow(context.Background(), "w", "", "s", nil)
	if got.Load() != "Bearer "+sdkTestToken+"|g1" {
		t.Fatalf("headers = %v", got.Load())
	}
	if _, wrapped := httpClient.Transport.(headerTransport); !wrapped {
		t.Fatal("caller HTTPClient was modified")
	}
}

type headerTransport struct{ name, value string }

func (h headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set(h.name, h.value)
	return http.DefaultTransport.RoundTrip(req)
}
