package app

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// MinAuthTokenLength 是 WORKFLOW_HTTP_AUTH_TOKEN 中每个 token 的最小字节数。
const MinAuthTokenLength = 16

// bearerAuth 校验 `Authorization: Bearer <token>`。只保存 token 的 SHA-256：比较的是定长摘要，
// 耗时与请求里的 token 长度、内容以及与哪个已配置 token 接近都无关；每次请求比较全部摘要，不提前返回。
type bearerAuth struct {
	digests [][sha256.Size]byte
}

// newBearerAuth 返回校验 tokens 的中间件；tokens 为空时返回 nil，表示不鉴权。
func newBearerAuth(tokens []string) *bearerAuth {
	if len(tokens) == 0 {
		return nil
	}
	auth := &bearerAuth{digests: make([][sha256.Size]byte, len(tokens))}
	for i, token := range tokens {
		auth.digests[i] = sha256.Sum256([]byte(token))
	}
	return auth
}

// require 在 next 之前校验 Bearer token；a 为 nil 时原样返回 next。
// 缺少凭证应答 401 与 `WWW-Authenticate: Bearer realm="tenuto"`，凭证不对时再带 `error="invalid_token"`（RFC 6750）。
func (a *bearerAuth) require(next http.Handler) http.Handler {
	if a == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, present := bearerToken(r.Header.Get("Authorization"))
		if !a.valid(token) {
			challenge := `Bearer realm="tenuto"`
			if present {
				challenge += `, error="invalid_token"`
			}
			w.Header().Set("WWW-Authenticate", challenge)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *bearerAuth) valid(token string) bool {
	digest := sha256.Sum256([]byte(token))
	match := 0
	for i := range a.digests {
		match |= subtle.ConstantTimeCompare(digest[:], a.digests[i][:])
	}
	return token != "" && match == 1
}

// bearerToken 取出 Authorization 头里的 Bearer 凭证；scheme 不区分大小写。present 报告请求是否带了凭证。
func bearerToken(header string) (token string, present bool) {
	header = strings.TrimSpace(header)
	if header == "" {
		return "", false
	}
	scheme, value, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", true
	}
	return strings.TrimSpace(value), true
}

// loadAuthTokens 读取 WORKFLOW_HTTP_AUTH_TOKEN 或 WORKFLOW_HTTP_AUTH_TOKEN_FILE（二选一）。多个 token 以逗号
// 分隔（文件里也可以换行分隔），任一 token 都通过校验，轮换时先加入新 token、客户端切换后再删除旧 token。
func loadAuthTokens(lookup envLookup) ([]string, error) {
	inline := envOrDefault(lookup, "WORKFLOW_HTTP_AUTH_TOKEN", "")
	file := envOrDefault(lookup, "WORKFLOW_HTTP_AUTH_TOKEN_FILE", "")
	if inline != "" && file != "" {
		return nil, fmt.Errorf("set only one of WORKFLOW_HTTP_AUTH_TOKEN and WORKFLOW_HTTP_AUTH_TOKEN_FILE")
	}
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read WORKFLOW_HTTP_AUTH_TOKEN_FILE: %w", err)
		}
		tokens := splitNonEmpty(strings.ReplaceAll(string(data), "\n", ","), ",")
		if len(tokens) == 0 {
			return nil, fmt.Errorf("WORKFLOW_HTTP_AUTH_TOKEN_FILE %s contains no token", file)
		}
		return tokens, nil
	}
	return splitNonEmpty(inline, ","), nil
}

// validateAuthTokens 要求每个 token 至少 MinAuthTokenLength 字节，且只含可见 ASCII（可以原样放进 HTTP 头）。
func validateAuthTokens(tokens []string) error {
	for i, token := range tokens {
		if len(token) < MinAuthTokenLength {
			return fmt.Errorf("WORKFLOW_HTTP_AUTH_TOKEN entry %d must contain at least %d bytes", i+1, MinAuthTokenLength)
		}
		for j := 0; j < len(token); j++ {
			if token[j] <= ' ' || token[j] > '~' {
				return fmt.Errorf("WORKFLOW_HTTP_AUTH_TOKEN entry %d must contain only visible ASCII characters", i+1)
			}
		}
	}
	return nil
}

// httpTLSConfig 按 WORKFLOW_HTTP_TLS_* 构造 HTTP 监听的 TLS 配置；没有配置证书时返回 nil（明文 HTTP）。
// ClientCAFile 非空时要求客户端出示由它签发的证书（mTLS），握手阶段拒绝其他连接。
func httpTLSConfig(cfg HTTPConfig) (*tls.Config, error) {
	if cfg.TLSCertFile == "" {
		return nil, nil
	}
	certificate, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load workflow HTTP certificate: %w", err)
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
	if cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read WORKFLOW_HTTP_TLS_CLIENT_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("WORKFLOW_HTTP_TLS_CLIENT_CA_FILE %s contains no PEM certificate", cfg.ClientCAFile)
		}
		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = pool
	}
	return config, nil
}
