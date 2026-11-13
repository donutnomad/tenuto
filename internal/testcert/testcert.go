// Package testcert 在测试运行时生成 CA 与证书，供 mTLS 测试使用；证书与私钥不落进仓库。
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// CA 是测试时生成的自签 CA。
type CA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	// CertPEM 是 CA 证书的 PEM。
	CertPEM []byte
}

// Pair 是一张证书与它的私钥（PEM）。
type Pair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// NewCA 生成一个有效期一小时的 CA。
func NewCA(t testing.TB, name string) *CA {
	t.Helper()
	key := newKey(t)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{cert: cert, key: key, CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// Server 签发对 127.0.0.1 与 localhost 有效的服务端证书。
func (ca *CA) Server(t testing.TB) Pair {
	return ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
}

// Client 签发 CommonName 为 name 的客户端证书。
func (ca *CA) Client(t testing.TB, name string) Pair {
	return ca.issue(t, name, x509.ExtKeyUsageClientAuth)
}

// Pool 返回只含这个 CA 的证书池。
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// TLSCertificate 把 Pair 解析成 tls.Certificate。
func (p Pair) TLSCertificate(t testing.TB) tls.Certificate {
	t.Helper()
	certificate, err := tls.X509KeyPair(p.CertPEM, p.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

// WriteFiles 把证书与私钥写进 dir，返回两个文件的路径。
func (p Pair) WriteFiles(t testing.TB, dir, name string) (certFile, keyFile string) {
	t.Helper()
	certFile = WriteFile(t, dir, name+".pem", p.CertPEM)
	keyFile = WriteFile(t, dir, name+"-key.pem", p.KeyPEM)
	return certFile, keyFile
}

// WriteFile 把 data 写进 dir/name（0600），返回路径。
func WriteFile(t testing.TB, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (ca *CA) issue(t testing.TB, name string, usage x509.ExtKeyUsage) Pair {
	t.Helper()
	key := newKey(t)
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return Pair{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	}
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
