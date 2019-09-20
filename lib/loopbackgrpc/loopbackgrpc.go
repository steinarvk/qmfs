package loopbackgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Params struct {
	Deadline           time.Duration
	Hostname           string
	AddressGRPC        string
	ServerCertPEM      []byte
	ClientCertificates []tls.Certificate
}

func Dial(ctx context.Context, params Params) (*grpc.ClientConn, error) {
	if params.Deadline > 0 {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(params.Deadline))
		defer cancel()
	}

	addr := params.AddressGRPC
	if params.Hostname != "" {
		addr = fmt.Sprintf("%s:%s", params.Hostname, strings.Split(addr, ":")[1])
	}

	certpool := x509.NewCertPool()
	certpool.AppendCertsFromPEM(params.ServerCertPEM)

	creds := credentials.NewTLS(&tls.Config{
		ServerName:   params.Hostname,
		RootCAs:      certpool,
		Certificates: params.ClientCertificates,
	})

	return grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(creds),
	)
}
