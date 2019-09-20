package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/steinarvk/orc"
	"github.com/steinarvk/orclib/bundle/orcstandardserver"
	"github.com/steinarvk/qmfs/lib/loopbackgrpc"
	"github.com/steinarvk/qmfs/lib/qmfs"
	"github.com/steinarvk/qmfs/lib/qmfsdb"
	"github.com/steinarvk/qmfs/lib/selfsigned"

	orcdebug "github.com/steinarvk/orclib/module/orc-debug"
	orcgrpcserver "github.com/steinarvk/orclib/module/orc-grpcserver"
	orcouterauth "github.com/steinarvk/orclib/module/orc-outerauth"
	orcpersistentkeys "github.com/steinarvk/orclib/module/orc-persistentkeys"
	server "github.com/steinarvk/orclib/module/orc-server"

	pb "github.com/steinarvk/qmfs/gen/qmfspb"
)

type listeningUpdate struct {
	Name string
	Addr string
}

type listenerProvider struct {
	ch       chan listeningUpdate
	hasMain  bool
	hasNoTLS bool
	hostname string
}

func (l *listenerProvider) ReportListening(name, addr string) {
	if name == "main" || name == "notls" {
		l.ch <- listeningUpdate{Name: name, Addr: addr}
	}
	if name == "main" {
		l.hasMain = true
	}
	if name == "notls" {
		l.hasNoTLS = true
	}
	if l.hasMain && l.hasNoTLS {
		close(l.ch)
	}
	logrus.Infof("Listening on %q: address %q", name, addr)
}

func (l *listenerProvider) GetListenAddresses() server.ListenAddress {
	return server.ListenAddress{
		Host:            l.hostname,
		RandomPort:      true,
		RandomNoTLSPort: true,
	}
}

/*
type serveroptsProvider struct {
	hostname    string
	tlsProvider *selfsigned.Provider
}

func (s *serveroptsProvider) GetGRPCServerOpts() ([]grpc.ServerOption, error) {
	certpool := x509.NewCertPool()

	pemBytes, err := s.tlsProvider.GetPEM(s.hostname)
	if err != nil {
		return nil, err
	}

	clientConfig, err := s.tlsProvider.GetTLSConfig(s.hostname)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Intentionally ignoring %v", pemBytes)

		if ok := certpool.AppendCertsFromPEM(pemBytes); !ok {
			return nil, fmt.Errorf("Failed to append PEM certificates for server")
		}

	creds := credentials.NewTLS(&tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: clientConfig.Certificates,
		ClientCAs:    certpool,
	})

	return []grpc.ServerOption{grpc.Creds(creds)}, nil
}
*/

func init() {
	provider := &selfsigned.Provider{}
	lisProvider := &listenerProvider{
		ch: make(chan listeningUpdate, 100),
	}
	var mountpoint string
	var localdb string

	mountCmd := orc.Command(Root, orc.ModulesWithSetup(
		func() {
			orcpersistentkeys.FakePersistentKeys = true
			server.ExternalTLS = provider
			server.ExternalListen = lisProvider
			server.ExternalListenSpy = lisProvider
			orcouterauth.DefaultDisableInboundAuth = true
			lisProvider.hostname = "localhost"
		},
		orcstandardserver.WithStandardIdentity(),
		orcgrpcserver.M,
	), cobra.Command{
		Use:   "serve",
		Short: "Serve qmfs as a fuse mount and service",
	}, func() error {
		hostname := lisProvider.hostname

		fuse.Debug = func(msg interface{}) {
			logrus.Debugf("fuse.Debug: %v", msg)
		}

		if err := orcdebug.M.RedirectMainToStatus(); err != nil {
			return err
		}

		if mountpoint == "" {
			return fmt.Errorf("Missing required flag --mountpoint")
		}

		if localdb == "" {
			return fmt.Errorf("Missing required flag --localdb")
		}

		ctx := context.Background()

		pathLocalDB, err := filepath.Abs(localdb)
		if err != nil {
			return err
		}

		db, err := qmfsdb.Open(ctx, pathLocalDB)
		if err != nil {
			return err
		}
		defer func() {
			if err := db.Close(); err != nil {
				logrus.Fatalf("Error closing database %q: %v", localdb, err)
			}
		}()
		logrus.Infof("Successfully opened database.")

		pb.RegisterQMetadataServiceServer(orcgrpcserver.M.Server, db)

		go func() {
			if err := server.ListenAndServe(); err != nil {
				logrus.Fatalf("Fatal: Server exited: %v", err)
			}
		}()

		var grpcAddress string
		var httpAddress string

		for update := range lisProvider.ch {
			switch update.Name {
			case "main":
				grpcAddress = update.Addr
			case "notls":
				httpAddress = update.Addr
			}
		}

		logrus.Infof("Established listening: http=%q grpc=%q", httpAddress, grpcAddress)

		certBytes, err := provider.GetPEM(hostname)
		if err != nil {
			return err
		}

		serverTLSConfig, err := provider.GetTLSConfig(hostname)
		if err != nil {
			return err
		}

		grpcAddress = fmt.Sprintf("%s:%s", hostname, strings.Split(grpcAddress, ":")[1])

		conn, err := loopbackgrpc.Dial(ctx, loopbackgrpc.Params{
			Deadline:           2 * time.Second,
			Hostname:           hostname,
			AddressGRPC:        grpcAddress,
			ServerCertPEM:      certBytes,
			ClientCertificates: serverTLSConfig.Certificates,
		})
		if err != nil {
			return err
		}
		defer conn.Close()

		client := pb.NewQMetadataServiceClient(conn)

		q, err := qmfs.New(ctx, client, qmfs.Params{
			ServiceData: qmfs.ServiceData{
				Hostname:          hostname,
				DatabasePath:      pathLocalDB,
				AddressGRPC:       grpcAddress,
				AddressHTTP:       httpAddress,
				ServerCertPEM:     certBytes,
				ClientCertificate: &serverTLSConfig.Certificates[0],
			},
			Mountpoint: mountpoint,
		})
		if err != nil {
			return fmt.Errorf("Failed to create qmfs: %v", err)
		}

		logrus.Infof("Performing mount.")

		fuseConn, err := fuse.Mount(
			mountpoint,
			fuse.FSName(localdb),
			fuse.Subtype("qmfs"),
			fuse.LocalVolume(),
		)
		if err != nil {
			logrus.Fatalf("Failed to set up fuse mount on %q: %v", mountpoint, err)
		}
		defer fuseConn.Close()

		logrus.Infof("Serving mount..")
		if err := fs.Serve(fuseConn, q); err != nil {
			logrus.Fatalf("Failed to serve fuse mount: %v", err)
		}

		<-fuseConn.Ready
		if err := fuseConn.MountError; err != nil {
			logrus.Fatalf("Mount error: %v", err)
		}

		defer func() {
			logrus.Infof("Attempting to unmount...")
			if err := fuse.Unmount(mountpoint); err != nil {
				logrus.Errorf("Unmount error: %v", err)
			} else {
				logrus.Infof("Unmount completed successfully.")
			}
		}()

		for {
			time.Sleep(time.Hour)
		}
	})

	mountCmd.Flags().StringVar(&mountpoint, "mountpoint", "", "path at which to mount file system")
	mountCmd.Flags().StringVar(&localdb, "localdb", "", "filename of local database")
}
