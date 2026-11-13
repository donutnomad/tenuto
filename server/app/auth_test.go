package app

import (
	"crypto/sha256"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/donutnomad/tenuto/internal/testcert"
)

const (
	testTokenCurrent = "current-token-0123456789"
	testTokenNext    = "next-token-abcdefghijklmn"
)

func TestBearerAuthProtectsRuntimeAndAdmin(t *testing.T) {
	runtime := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	health := &healthState{}
	handler := newHTTPHandler(runtime, health, nil, newBearerAuth([]string{testTokenCurrent, testTokenNext}))

	// 鉴权先于就绪检查：未就绪时未鉴权的请求同样得到 401。
	assertAuth(t, handler, "/runtime/workflow/start", "", http.StatusUnauthorized, `Bearer realm="tenuto"`)
	health.ready.Store(true)

	cases := []struct {
		name, path, header string
		want               int
		challenge          string
	}{
		{"missing", "/runtime/workflow/start", "", http.StatusUnauthorized, `Bearer realm="tenuto"`},
		{"wrong token", "/runtime/workflow/start", "Bearer wrong-token-0123456789", http.StatusUnauthorized, `Bearer realm="tenuto", error="invalid_token"`},
		{"prefix of token", "/runtime/workflow/start", "Bearer " + testTokenCurrent[:len(testTokenCurrent)-1], http.StatusUnauthorized, `Bearer realm="tenuto", error="invalid_token"`},
		{"token with suffix", "/runtime/workflow/start", "Bearer " + testTokenCurrent + "x", http.StatusUnauthorized, `Bearer realm="tenuto", error="invalid_token"`},
		{"basic scheme", "/runtime/workflow/start", "Basic " + testTokenCurrent, http.StatusUnauthorized, `Bearer realm="tenuto", error="invalid_token"`},
		{"empty bearer", "/runtime/workflow/start", "Bearer ", http.StatusUnauthorized, `Bearer realm="tenuto", error="invalid_token"`},
		{"current token", "/runtime/workflow/start", "Bearer " + testTokenCurrent, http.StatusNoContent, ""},
		{"next token during rotation", "/runtime/activity/poll", "Bearer " + testTokenNext, http.StatusNoContent, ""},
		{"scheme is case-insensitive", "/runtime/workflow/list", "bearer " + testTokenCurrent, http.StatusNoContent, ""},
		{"admin missing", "/admin/partitions?queue=q", "", http.StatusUnauthorized, `Bearer realm="tenuto"`},
		// partitionAdmin 为 nil：鉴权通过后到达端点，得到 501。
		{"admin with token", "/admin/partitions?queue=q", "Bearer " + testTokenNext, http.StatusNotImplemented, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertAuth(t, handler, tc.path, tc.header, tc.want, tc.challenge)
		})
	}

	// 健康检查不鉴权。
	assertStatus(t, handler, http.MethodGet, "/health/live", http.StatusOK)
	assertStatus(t, handler, http.MethodGet, "/health/ready", http.StatusOK)
}

func TestBearerAuthDisabledWithoutTokens(t *testing.T) {
	if newBearerAuth(nil) != nil {
		t.Fatal("no tokens must disable authentication")
	}
	health := &healthState{}
	health.ready.Store(true)
	runtime := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := newHTTPHandler(runtime, health, nil, newBearerAuth(nil))
	assertStatus(t, handler, http.MethodPost, "/runtime/workflow/start", http.StatusNoContent)
}

// 校验比较的是定长 SHA-256 摘要（subtle.ConstantTimeCompare），不保存、不直接比较明文。
func TestBearerAuthStoresOnlyDigests(t *testing.T) {
	auth := newBearerAuth([]string{testTokenCurrent, testTokenNext})
	if len(auth.digests) != 2 || auth.digests[0] != sha256.Sum256([]byte(testTokenCurrent)) || auth.digests[1] != sha256.Sum256([]byte(testTokenNext)) {
		t.Fatalf("digests = %x", auth.digests)
	}
	for _, token := range []string{"", "x", testTokenCurrent + testTokenNext, strings.Repeat("a", 4096)} {
		if auth.valid(token) {
			t.Fatalf("token %q accepted", token)
		}
	}
}

func TestLoadConfigAuthTokens(t *testing.T) {
	env := validEnvironment()
	env["WORKFLOW_HTTP_AUTH_TOKEN"] = " " + testTokenCurrent + " , " + testTokenNext + " "
	cfg, err := loadConfig(mapLookup(env), "node")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.HTTP.AuthTokens, "|"); got != testTokenCurrent+"|"+testTokenNext {
		t.Fatalf("tokens = %q", got)
	}

	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(testTokenCurrent+"\n"+testTokenNext+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env = validEnvironment()
	env["WORKFLOW_HTTP_AUTH_TOKEN_FILE"] = file
	cfg, err = loadConfig(mapLookup(env), "node")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.HTTP.AuthTokens, "|"); got != testTokenCurrent+"|"+testTokenNext {
		t.Fatalf("tokens from file = %q", got)
	}

	cfg, err = loadConfig(mapLookup(validEnvironment()), "node")
	if err != nil || len(cfg.HTTP.AuthTokens) != 0 || cfg.HTTP.ClientCAFile != "" || cfg.HTTP.HealthAddress != "" {
		t.Fatalf("defaults: cfg.HTTP = %+v, err = %v", cfg.HTTP, err)
	}

	emptyFile := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(emptyFile, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	invalid := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"both sources", map[string]string{"WORKFLOW_HTTP_AUTH_TOKEN": testTokenCurrent, "WORKFLOW_HTTP_AUTH_TOKEN_FILE": file}, "only one of"},
		{"short token", map[string]string{"WORKFLOW_HTTP_AUTH_TOKEN": testTokenCurrent + ",short"}, "entry 2 must contain at least 16 bytes"},
		{"non-ASCII token", map[string]string{"WORKFLOW_HTTP_AUTH_TOKEN": "tökén-0123456789abcdef"}, "visible ASCII"},
		{"missing file", map[string]string{"WORKFLOW_HTTP_AUTH_TOKEN_FILE": filepath.Join(t.TempDir(), "absent")}, "read WORKFLOW_HTTP_AUTH_TOKEN_FILE"},
		{"empty file", map[string]string{"WORKFLOW_HTTP_AUTH_TOKEN_FILE": emptyFile}, "contains no token"},
		{"client CA without certificate", map[string]string{"WORKFLOW_HTTP_TLS_CLIENT_CA_FILE": "ca.pem"}, "WORKFLOW_HTTP_TLS_CLIENT_CA_FILE requires"},
		{"health address without port", map[string]string{"WORKFLOW_HEALTH_ADDR": "localhost"}, "WORKFLOW_HEALTH_ADDR"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			env := validEnvironment()
			for k, v := range tc.env {
				env[k] = v
			}
			_, err := loadConfig(mapLookup(env), "node")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestHTTPTLSRequiresClientCertificate(t *testing.T) {
	dir := t.TempDir()
	ca := testcert.NewCA(t, "tenuto-test-ca")
	rogue := testcert.NewCA(t, "rogue-ca")
	certFile, keyFile := ca.Server(t).WriteFiles(t, dir, "server")
	cfg := HTTPConfig{TLSCertFile: certFile, TLSKeyFile: keyFile, ClientCAFile: testcert.WriteFile(t, dir, "ca.pem", ca.CertPEM)}
	tlsConfig, err := httpTLSConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth = %v", tlsConfig.ClientAuth)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.TLS.PeerCertificates[0].Subject.CommonName))
	}))
	server.TLS = tlsConfig
	server.StartTLS()
	defer server.Close()

	get := func(certificates ...tls.Certificate) (string, error) {
		clientTLS := &tls.Config{RootCAs: ca.Pool(), MinVersion: tls.VersionTLS12, Certificates: certificates}
		client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: clientTLS}}
		defer client.CloseIdleConnections()
		resp, err := client.Get(server.URL)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return string(body), err
	}
	if got, err := get(ca.Client(t, "client").TLSCertificate(t)); err != nil || got != "client" {
		t.Fatalf("trusted client: body = %q, err = %v", got, err)
	}
	if _, err := get(); err == nil {
		t.Fatal("client without certificate was accepted")
	}
	if _, err := get(rogue.Client(t, "rogue").TLSCertificate(t)); err == nil {
		t.Fatal("client certificate from another CA was accepted")
	}

	cfg.ClientCAFile = testcert.WriteFile(t, dir, "not-a-ca.pem", []byte("not a certificate"))
	if _, err := httpTLSConfig(cfg); err == nil || !strings.Contains(err.Error(), "contains no PEM certificate") {
		t.Fatalf("invalid CA file: err = %v", err)
	}
	cfg.ClientCAFile = ""
	plain, err := httpTLSConfig(cfg)
	if err != nil || plain.ClientAuth != tls.NoClientCert {
		t.Fatalf("TLS without client CA: config = %+v, err = %v", plain, err)
	}
}

func TestHealthMuxServesOnlyHealth(t *testing.T) {
	health := &healthState{}
	health.ready.Store(true)
	mux := health.mux()
	assertStatus(t, mux, http.MethodGet, "/health/live", http.StatusOK)
	assertStatus(t, mux, http.MethodGet, "/health/ready", http.StatusOK)
	assertStatus(t, mux, http.MethodPost, "/runtime/workflow/start", http.StatusNotFound)
	assertStatus(t, mux, http.MethodGet, "/admin/partitions", http.StatusNotFound)
}

func assertAuth(t *testing.T, handler http.Handler, path, authorization string, want int, challenge string) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	if strings.HasPrefix(path, "/admin/") {
		request.Method = http.MethodGet
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != want {
		t.Fatalf("%s with %q: status = %d, want %d; body=%s", path, authorization, recorder.Code, want, recorder.Body.String())
	}
	if got := recorder.Header().Get("WWW-Authenticate"); got != challenge {
		t.Fatalf("%s with %q: WWW-Authenticate = %q, want %q", path, authorization, got, challenge)
	}
}
