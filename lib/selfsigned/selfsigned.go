package selfsigned

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var Organization = "Self-signed dummy certificate"
var RSABits = 2048

func Generate(hostname string) (*tls.Certificate, error) {
	logrus.Infof("Generating self-signed certificate for %q", hostname)

	priv, err := rsa.GenerateKey(rand.Reader, RSABits)
	if err != nil {
		return nil, fmt.Errorf("Error generating self-signed certificate: %v", err)
	}

	notBefore := time.Now()
	validFor := 365 * 24 * time.Hour

	notAfter := notBefore.Add(validFor)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("Error generating self-signed certificate: error generating serial number: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{Organization},
			CommonName:   hostname,
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,

		IsCA: true,

		DNSNames: []string{hostname},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("Error generating self-signed certificate: CreateCertificate: %v", err)
	}

	parsedCert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Successfully generated self-signed certificate for %q", hostname)

	return &tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  priv,
		Leaf:        parsedCert,
	}, nil
}

type Provider struct {
	mu       sync.Mutex
	cert     *tls.Certificate
	config   *tls.Config
	hostname string
	pemBuf   []byte
	done     bool
}

func (p *Provider) GetHostname() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.done {
		return "", fmt.Errorf("No hostname set")
	}

	return p.hostname, nil
}

func (p *Provider) GetPEM(hostname string) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, err := p.holdingLockGetTLSConfig(hostname)
	if err != nil {
		return nil, err
	}

	return p.pemBuf, nil
}

func (p *Provider) holdingLockGetTLSConfig(hostname string) (*tls.Config, error) {
	if p.config != nil {
		if p.hostname != hostname {
			return nil, fmt.Errorf("Hostname changed (from %q to %q)", p.hostname, hostname)
		}
		return p.config, nil
	}

	cert, err := Generate(hostname)
	if err != nil {
		return nil, err
	}

	certpool := x509.NewCertPool()

	p.hostname = hostname
	p.cert = cert
	p.config = &tls.Config{
		Certificates: []tls.Certificate{*cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		RootCAs:      certpool,
		ClientCAs:    certpool,
	}
	p.config.BuildNameToCertificate()

	var buf bytes.Buffer
	if err := pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}); err != nil {
		return nil, fmt.Errorf("Error generating self-signed certificate: Encode: %v", err)
	}

	p.pemBuf = buf.Bytes()
	p.done = true

	if ok := certpool.AppendCertsFromPEM(p.pemBuf); !ok {
		return nil, err
	}

	return p.config, nil
}

func (p *Provider) GetTLSConfig(hostname string) (*tls.Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.holdingLockGetTLSConfig(hostname)
}
