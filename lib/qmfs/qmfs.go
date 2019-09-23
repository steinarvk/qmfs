package qmfs

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/golang/protobuf/proto"
	lru "github.com/hashicorp/golang-lru"
	"github.com/sirupsen/logrus"
	"github.com/steinarvk/linetool/lib/lines"
	"github.com/steinarvk/orclib/lib/versioninfo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/steinarvk/qmfs/gen/qmfspb"
	"github.com/steinarvk/qmfs/lib/atomicfilefuse"
	"github.com/steinarvk/qmfs/lib/dyndirfuse"
	"github.com/steinarvk/qmfs/lib/linkfuse"
	"github.com/steinarvk/qmfs/lib/ondemandfuse"
	"github.com/steinarvk/qmfs/lib/qmfsquery"
	"github.com/steinarvk/qmfs/lib/qmfsshard"
	"github.com/steinarvk/qmfs/lib/readstreamfuse"
	"github.com/steinarvk/qmfs/lib/staticfuse"
)

var qmfsVersioninfoJSON string

type ServiceData struct {
	Hostname             string
	DatabasePath         string
	AddressGRPC          string
	AddressHTTP          string
	ServerCertPEM        []byte
	ClientCertificate    *tls.Certificate
	ForbiddenFilenameREs []string
}

func newServiceTree(ctx context.Context, svcdata ServiceData, client pb.QMetadataServiceClient, goodbyeChan chan<- error) (fs.Node, error) {
	tree := &fs.Tree{}

	grpcAddr := svcdata.AddressGRPC
	if svcdata.Hostname != "" && strings.Contains(svcdata.AddressGRPC, ":") {
		grpcAddr = fmt.Sprintf("%s:%s", svcdata.Hostname, strings.Split(svcdata.AddressGRPC, ":")[1])
	}
	if svcdata.AddressHTTP != "" {
		tree.Add("http", staticfuse.String(fmt.Sprintf("http://%s\n", svcdata.AddressHTTP)))
	}
	if grpcAddr != "" {
		tree.Add("grpc", staticfuse.String(grpcAddr))
	}
	if len(svcdata.ServerCertPEM) > 0 {
		tree.Add("server_cert.pem", staticfuse.Bytes(svcdata.ServerCertPEM))
	}

	if svcdata.ClientCertificate != nil {
		certDER := svcdata.ClientCertificate.Certificate[0]
		clientKey := svcdata.ClientCertificate.PrivateKey

		var certBuf bytes.Buffer
		if err := pem.Encode(&certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
			return nil, err
		}

		leafCertPEM := certBuf.Bytes()

		tree.Add("client_cert.pem", staticfuse.Bytes(leafCertPEM))

		privBytes, err := x509.MarshalPKCS8PrivateKey(clientKey)
		if err != nil {
			return nil, err
		}

		var keyBuf bytes.Buffer
		if err := pem.Encode(&keyBuf, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
			return nil, err
		}

		clientKeyPEM := keyBuf.Bytes()

		tree.Add("client_key.pem", staticfuse.Bytes(clientKeyPEM))
	}

	tree.Add("bad_filenames", staticfuse.Bytes(lines.AsBytes(svcdata.ForbiddenFilenameREs)))

	startupTime := time.Now()
	tree.Add("startup", staticfuse.String(fmt.Sprintf("%d", startupTime.UnixNano())))
	tree.Add("uptime", ondemandfuse.String(func(ctx context.Context) (string, error) {
		return fmt.Sprintf("%v", time.Since(startupTime)), nil
	}))

	statsTree := &fs.Tree{}
	statsTree.Add("heap_bytes", ondemandfuse.String(func(ctx context.Context) (string, error) {
		var memstats runtime.MemStats
		runtime.ReadMemStats(&memstats)
		return fmt.Sprintf("%d", memstats.HeapAlloc), nil
	}))

	tree.Add("stats", statsTree)

	tree.Add("last_changed", ondemandfuse.String(func(ctx context.Context) (string, error) {
		resp, err := client.GetDatabaseMetadata(ctx, &pb.GetDatabaseMetadataRequest{
			OnlyTimestamps: true,
		})
		if err != nil {
			logrus.Errorf("GetDatabaseMetadata: %v", err)
			return "", err
		}

		unixNanos := resp.GetMetadata().GetLastChanged().GetUnixNano()
		return fmt.Sprintf("%d", unixNanos), nil
	}))

	versionNode, err := staticfuse.JSON(versioninfo.MakeJSON())
	if err != nil {
		return nil, err
	}
	tree.Add("version.json", versionNode)

	tree.Add("database_path", staticfuse.String(svcdata.DatabasePath))

	tree.Add("pid", staticfuse.String(fmt.Sprintf("%d", os.Getpid())))

	return tree, nil
}

type Params struct {
	ServiceData
	Mountpoint   string
	ShutdownChan chan<- error
}

type Filesystem struct {
	svc    ServiceData
	client pb.QMetadataServiceClient
	root   *fs.Tree
}

func newNamespaceListNode(client pb.QMetadataServiceClient, mountpoint string, shardKey []byte, contextBG context.Context, isFilenameBad func(string) bool) fs.Node {
	return &dyndirfuse.DynamicDir{
		CacheSize: 100,
		Fields: map[string]interface{}{
			"dir": "namespaces",
		},
		List: func(ctx context.Context, cb func(string, fuse.DirentType)) error {
			resp, err := client.ListNamespaces(ctx, &pb.ListNamespacesRequest{})
			if err != nil {
				return err
			}

			for _, ns := range resp.Namespace {
				if ns != "" {
					cb(ns, fuse.DT_Dir)
				}
			}

			return nil
		},
		Get: func(ctx context.Context, namespaceName string) (fs.Node, fuse.DirentType, bool, error) {
			if !qmfsquery.ValidFilename(namespaceName) {
				return nil, fuse.DT_Unknown, false, fuse.ENOENT
			}

			tree := &fs.Tree{}
			if err := addRootNodesForNamespace(ctx, client, tree, contextBG, namespaceName, mountpoint, shardKey, isFilenameBad); err != nil {
				return nil, fuse.DT_Unknown, false, err
			}
			return tree, fuse.DT_Dir, true, nil
		},
	}
}

var (
	fileContentsCache *lru.Cache
	fileAttribsCache  *lru.Cache
	queryResultCache  *lru.Cache
)

type queryCacheKey struct {
	namespace string
	queryID   int64
	entityID  string
}

type fileCacheKey struct {
	namespace, entityID, filename string
}

type fileAttribCacheEntry struct {
	rowGUID   string
	length    uint64
	exists    bool
	directory bool
}

type fileContentsCacheEntry struct {
	rowGUID   string
	data      []byte
	exists    bool
	directory bool
}

func init() {
	cacheForLargeItems, err := lru.New(100)
	if err != nil {
		logrus.Fatalf("Failed to create file contents cache: %v", err)
	}
	fileContentsCache = cacheForLargeItems

	cacheForSmallItems, err := lru.New(10000)
	if err != nil {
		logrus.Fatalf("Failed to create file attribs cache: %v", err)
	}
	fileAttribsCache = cacheForSmallItems

	cacheForQueries, err := lru.New(10000)
	if err != nil {
		logrus.Fatalf("Failed to create file attribs cache: %v", err)
	}
	queryResultCache = cacheForQueries
}

func invalidateFileCacheFor(namespace, entityID, filename string) {
	cacheKey := fileCacheKey{namespace: namespace, entityID: entityID, filename: filename}

	logrus.WithFields(logrus.Fields{
		"namespace": namespace,
		"entity_id": entityID,
		"filename":  filename,
	}).Infof("Invalidating caches")

	fileAttribsCache.Remove(cacheKey)
	fileContentsCache.Remove(cacheKey)
}

func performReadOf(ctx context.Context, client pb.QMetadataServiceClient, namespace, entityID, filename string) (*fileContentsCacheEntry, error) {
	cacheKey := fileCacheKey{namespace: namespace, entityID: entityID, filename: filename}

	logrus.WithFields(logrus.Fields{
		"namespace": namespace,
		"entity_id": entityID,
		"filename":  filename,
	}).Infof("Performing ReadFile and caching result")

	resp, err := client.ReadFile(ctx, &pb.ReadFileRequest{
		Namespace: namespace,
		EntityId:  entityID,
		Filename:  filename,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			contentsEntry := &fileContentsCacheEntry{
				data:    nil,
				rowGUID: "",
				exists:  false,
			}
			fileContentsCache.Add(cacheKey, contentsEntry)
			fileAttribsCache.Add(cacheKey, &fileAttribCacheEntry{
				rowGUID: "",
				length:  0,
				exists:  false,
			})

			return contentsEntry, nil
		}
		return nil, err
	}

	data := resp.GetFile().GetData()
	rowGUID := resp.GetFile().GetHeader().GetRowGuid()
	directory := resp.GetFile().GetHeader().GetDirectory()

	putIntoCacheAs(cacheKey, data, rowGUID, true, directory)

	return &fileContentsCacheEntry{
		rowGUID:   rowGUID,
		data:      data,
		exists:    true,
		directory: directory,
	}, nil
}

func getFileAttribsOf(ctx context.Context, client pb.QMetadataServiceClient, namespace, entityID, path string) (*fileAttribCacheEntry, bool, error) {
	cacheKey := fileCacheKey{namespace: namespace, entityID: entityID, filename: path}
	cached, ok := fileAttribsCache.Get(cacheKey)

	logrus.WithFields(logrus.Fields{
		"namespace": namespace,
		"entity_id": entityID,
		"filename":  path,
		"cache_hit": ok,
	}).Infof("Lookup in file attribs cache")

	if ok {
		entry := cached.(*fileAttribCacheEntry)
		return entry, entry.exists, nil
	}

	contentsEntry, err := performReadOf(ctx, client, namespace, entityID, path)
	if err != nil && contentsEntry == nil {
		return &fileAttribCacheEntry{}, false, err
	}
	return &fileAttribCacheEntry{
		exists:    contentsEntry.exists,
		length:    uint64(len(contentsEntry.data)),
		directory: contentsEntry.directory,
		rowGUID:   contentsEntry.rowGUID,
	}, contentsEntry.exists, err
}

func writeFileOrDir(ctx context.Context, client pb.QMetadataServiceClient, namespace, entityID, filename string, data []byte, rev string, directory bool) (string, error) {
	authorship := &pb.AuthorshipMetadata{}

	if qmfsVersioninfoJSON != "" {
		authorship.QmfsVersioninfoJson = qmfsVersioninfoJSON
	}

	resp, err := client.WriteFile(ctx, &pb.WriteFileRequest{
		Namespace:          namespace,
		EntityId:           entityID,
		Filename:           filename,
		Data:               data,
		OldRevisionGuid:    rev,
		AuthorshipMetadata: authorship,
		Directory:          directory,
	})

	if err == nil {
		rowGUID := resp.GetHeader().GetRowGuid()

		cacheKey := fileCacheKey{namespace: namespace, entityID: entityID, filename: filename}
		putIntoCacheAs(cacheKey, data, rowGUID, true, directory)
	}

	if err != nil {
		return "", err
	}

	return resp.GetHeader().GetRowGuid(), nil
}

func putIntoCacheAs(cacheKey fileCacheKey, data []byte, rowGUID string, exists bool, directory bool) {
	fileContentsCache.Add(cacheKey, &fileContentsCacheEntry{
		data:      data,
		rowGUID:   rowGUID,
		exists:    exists,
		directory: directory,
	})
	fileAttribsCache.Add(cacheKey, &fileAttribCacheEntry{
		rowGUID:   rowGUID,
		length:    uint64(len(data)),
		exists:    exists,
		directory: directory,
	})
}

func getFileNode(ctx context.Context, client pb.QMetadataServiceClient, namespace, entityID, filename string) *atomicfilefuse.File {
	// TODO must keep all files with active handles in cache
	f := &atomicfilefuse.File{
		Fields: map[string]interface{}{
			"namespace": namespace,
			"entity_id": entityID,
			"filename":  filename,
		},
	}

	cacheKey := fileCacheKey{namespace: namespace, entityID: entityID, filename: filename}

	performRead := func(ctx context.Context) (*fileContentsCacheEntry, error) {
		return performReadOf(ctx, client, namespace, entityID, filename)
	}

	getFileContents := func(ctx context.Context) ([]byte, string, bool, error) {
		cached, ok := fileContentsCache.Get(cacheKey)

		logrus.WithFields(logrus.Fields{
			"namespace": namespace,
			"entity_id": entityID,
			"filename":  filename,
			"cache_hit": ok,
		}).Infof("Lookup in file contents cache")

		if ok {
			entry := cached.(*fileContentsCacheEntry)
			logrus.WithFields(logrus.Fields{
				"namespace": namespace,
				"entity_id": entityID,
				"filename":  filename,
				"exists":    entry.exists,
				"size":      len(entry.data),
			}).Infof("Cache hit in file contents cache")
			return entry.data, entry.rowGUID, entry.exists, nil
		}

		entry, err := performRead(ctx)
		if err != nil {
			return nil, "", false, err
		}
		return entry.data, entry.rowGUID, entry.exists, nil
	}

	getFileAttribs := func(ctx context.Context) (*fileAttribCacheEntry, bool, error) {
		return getFileAttribsOf(ctx, client, namespace, entityID, filename)
	}

	f.GetAttr = func(ctx context.Context, a *fuse.Attr) (bool, error) {
		attribs, ok, err := getFileAttribs(ctx)
		if err != nil {
			return ok, err
		}

		if a != nil {
			a.Valid = 0
			a.Mode = 0660
			if attribs.directory {
				a.Mode |= os.ModeDir
			}
			a.Size = uint64(attribs.length)
		}

		return ok, nil
	}
	f.AtomicRead = func(ctx context.Context) ([]byte, string, bool, error) {
		return getFileContents(ctx)
	}
	f.AtomicWrite = func(ctx context.Context, data []byte, rev string) (string, error) {
		rev, err := writeFileOrDir(ctx, client, namespace, entityID, filename, data, rev, false)
		return rev, err
	}
	return f
}

func getEntityRootNode(ctx context.Context, client pb.QMetadataServiceClient, namespace, entityID string, isFilenameBad func(string) bool) fs.Node {
	return getEntityDirNode(ctx, client, namespace, entityID, "", isFilenameBad)
}

func getEntityDirNode(ctx context.Context, client pb.QMetadataServiceClient, namespace, entityID, parentdir string, isFilenameBad func(string) bool) fs.Node {
	cacheSize := 1000
	if parentdir != "" {
		cacheSize = 0
	}

	isDirectChild := func(path string) string {
		if parentdir == "" {
			if strings.Contains(path, "/") {
				return ""
			}
			return path
		}
		prefix := parentdir + "/"
		if !strings.HasPrefix(path, prefix) {
			return ""
		}
		if strings.Contains(path[len(prefix):], "/") {
			return ""
		}
		return path[len(prefix):]
	}

	fullPath := func(childFilename string) string {
		if parentdir == "" {
			return childFilename
		}
		return parentdir + "/" + childFilename
	}

	isDir := func(ctx context.Context, path string) (bool, error) {
		attribs, _, err := getFileAttribsOf(ctx, client, namespace, entityID, path)
		if err != nil {
			return false, err
		}
		return attribs.directory, nil
	}

	f := &dyndirfuse.DynamicDir{
		CacheSize: cacheSize,
		Fields: map[string]interface{}{
			"dir":       "entity-files",
			"subdir":    parentdir,
			"namespace": namespace,
			"entity_id": entityID,
		},
		List: func(ctx context.Context, cb func(string, fuse.DirentType)) error {
			resp, err := client.GetEntity(ctx, &pb.GetEntityRequest{
				Namespace: namespace,
				EntityId:  entityID,
			})
			if err != nil {
				return err
			}

			for path, _ := range resp.GetEntity().GetFiles() {
				if relname := isDirectChild(path); relname != "" {
					dir, err := isDir(ctx, path)
					if err != nil {
						return err
					}
					if dir {
						cb(relname, fuse.DT_Dir)
					} else {
						cb(relname, fuse.DT_File)
					}
				}
			}

			return nil
		},
		Get: func(ctx context.Context, filename string) (fs.Node, fuse.DirentType, bool, error) {
			if !qmfsquery.ValidFilename(filename) {
				return nil, fuse.DT_Unknown, false, fuse.ENOENT
			}

			if isFilenameBad(filename) {
				logrus.WithFields(logrus.Fields{
					"filename": filename,
				}).Warningf("Refusing to allow file")

				return nil, fuse.DT_Unknown, false, fuse.EIO
			}

			path := fullPath(filename)

			dir, err := isDir(ctx, path)
			if err != nil {
				return nil, fuse.DT_Unknown, false, err
			}

			if dir {
				return getEntityDirNode(ctx, client, namespace, entityID, path, isFilenameBad), fuse.DT_Dir, true, nil
			}

			node := getFileNode(ctx, client, namespace, entityID, path)
			ft := fuse.DT_File

			ok, err := node.GetAttr(ctx, nil)
			if err != nil {
				return nil, ft, false, err
			}

			return node, ft, ok, nil
		},
		CreateDir: func(ctx context.Context, filename string) error {
			if !qmfsquery.ValidFilename(filename) {
				return fuse.EIO
			}
			path := fullPath(filename)
			_, err := writeFileOrDir(ctx, client, namespace, entityID, path, nil, "", true)
			return err
		},
		Delete: func(ctx context.Context, filename string, dir bool) error {
			if !qmfsquery.ValidFilename(filename) {
				return fuse.ENOENT
			}

			path := fullPath(filename)

			deltype := pb.DeletionType_DELETE_FILE
			if dir {
				deltype = pb.DeletionType_DELETE_DIR
			}

			logrus.Infof("Attempting DeleteFile")
			_, err := client.DeleteFile(ctx, &pb.DeleteFileRequest{
				Namespace:    namespace,
				EntityId:     entityID,
				Filename:     path,
				DeletionType: deltype,
			})
			if status.Code(err) == codes.NotFound {
				return fuse.ENOENT
			}
			invalidateFileCacheFor(namespace, entityID, path)
			logrus.Infof("Attempting DeleteFile: %v", err)
			return err
		},
	}
	return f
}

type entitiesQueryer struct {
	pathToRoot        string
	memberType        fuse.DirentType
	listAll           func(context.Context, []string, func(string) error) error
	checkEntityExists func(context.Context, string) (bool, error)
	getNode           func(context.Context, string) (fs.Node, bool, error)
	getShards         func(context.Context, []string) (map[string][]string, error)
}

func moreFields(ms ...map[string]interface{}) map[string]interface{} {
	rv := map[string]interface{}{}
	for _, m := range ms {
		if m != nil {
			for k, v := range m {
				rv[k] = v
			}
		}
	}
	return rv
}

func mkEntitiesListNode(ctx context.Context, client pb.QMetadataServiceClient, mountpoint string, shardKey []byte, namespace string, fields map[string]interface{}, q *entitiesQueryer, isRoot bool) (fs.Node, error) {
	sharder := qmfsshard.Key(shardKey)

	formSelector := &fs.Tree{}

	canonicalType := q.memberType
	if canonicalType == fuse.DT_Unknown {
		canonicalType = fuse.DT_Dir
	}
	hasCanonical := q.getNode != nil

	mkCanonicalPath := func(entityID string) string {
		shards := sharder.Shard(entityID)
		return fmt.Sprintf("shard/%s/%s/%s", shards[0], shards[1], entityID)
	}
	mkAbsCanonicalPath := func(entityID string) string {
		var qualifyNamespace string
		if namespace != "" {
			qualifyNamespace = fmt.Sprintf("namespace/%s/", namespace)
		}
		return filepath.Join(mountpoint, qualifyNamespace+"entities", mkCanonicalPath(entityID))
	}

	report := func(entityID string, canonical bool, cb func(string, fuse.DirentType)) {
		if hasCanonical && canonical {
			cb(entityID, canonicalType)
		} else {
			cb(entityID, fuse.DT_Link)
		}
	}

	hasShards := func(wantShards []string, entityID string) (bool, error) {
		if len(wantShards) == 0 {
			return true, nil
		}
		actualShards := sharder.Shard(entityID)
		if len(actualShards) < len(wantShards) {
			return false, status.Errorf(codes.Internal, "bad number of shards returned: %q => %v", entityID, actualShards)
		}
		for i, wantShard := range wantShards {
			if actualShards[i] != wantShard {
				return false, nil
			}
		}
		return true, nil
	}

	if q.listAll != nil && q.getShards == nil {
		q.getShards = func(ctx context.Context, prefix []string) (map[string][]string, error) {
			logrus.WithFields(fields).Warningf("Using dumb fallback getShards")

			m := map[string][]string{}

			err := q.listAll(ctx, prefix, func(entityID string) error {
				ok, err := hasShards(prefix, entityID)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}

				moreshards := sharder.Shard(entityID)[len(prefix):]
				if len(moreshards) == 0 {
					return nil
				}
				m[moreshards[0]] = moreshards[1:]
				return nil
			})
			if err != nil {
				return nil, err
			}

			return m, nil
		}
	}

	lister := func(canonical bool) func([]string) func(context.Context, func(string, fuse.DirentType)) error {
		return func(shards []string) func(context.Context, func(string, fuse.DirentType)) error {
			return func(ctx context.Context, cb func(string, fuse.DirentType)) error {
				return q.listAll(ctx, shards, func(entityID string) error {
					ok, err := hasShards(shards, entityID)
					if err != nil {
						return err
					}
					if ok {
						report(entityID, true, cb)
					} else {
						logrus.WithFields(logrus.Fields{
							"entity_id": entityID,
							"shards":    shards,
						}).Warningf("Inefficient query; filtering out non-matching shards from list")
					}
					return nil
				})
			}
		}
	}

	getter := func(canonical bool) func([]string) func(context.Context, string) (fs.Node, fuse.DirentType, bool, error) {
		return func(shards []string) func(context.Context, string) (fs.Node, fuse.DirentType, bool, error) {
			return func(ctx context.Context, entityID string) (fs.Node, fuse.DirentType, bool, error) {
				ok, err := hasShards(shards, entityID)
				if err != nil {
					return nil, fuse.DT_Unknown, false, err
				}
				if !ok {
					return nil, fuse.DT_Unknown, false, fuse.ENOENT
				}

				if q.checkEntityExists != nil {
					ok, err := q.checkEntityExists(ctx, entityID)
					if err != nil {
						return nil, fuse.DT_Unknown, false, err
					}
					if !ok {
						return nil, fuse.DT_Unknown, false, fuse.ENOENT
					}
				}

				if hasCanonical && canonical {
					node, ok, err := q.getNode(ctx, entityID)
					if err != nil {
						return nil, fuse.DT_Unknown, false, err
					}
					return node, canonicalType, ok, nil
				}

				memberType := fuse.DT_Link
				fullPath := mkAbsCanonicalPath(entityID)
				linkNode := linkfuse.Target(fullPath)
				return linkNode, memberType, true, nil
			}
		}
	}

	legacyAll := &dyndirfuse.DynamicDir{
		Fields:    moreFields(fields, map[string]interface{}{"resultset": "all"}),
		CacheSize: 100,
		List:      lister(false)(nil),
		Get:       getter(false)(nil),
	}

	shardingLevels := 2

	createLeafSharded := func(shards []string) fs.Node {
		return &dyndirfuse.DynamicDir{
			Fields: moreFields(fields, map[string]interface{}{
				"resultset":  "sharded",
				"shard_leaf": true,
				"shard":      shards,
			}),
			List: lister(true)(shards),
			Get:  getter(true)(shards),
		}
	}

	var createSharded func([]string) fs.Node

	createSharded = func(shardprefix []string) fs.Node {
		if len(shardprefix) == shardingLevels {
			return createLeafSharded(shardprefix)
		}

		return &dyndirfuse.DynamicDir{
			Fields: moreFields(fields, map[string]interface{}{
				"resultset":  "sharded",
				"shard_leaf": false,
				"shard":      shardprefix,
			}),
			List: func(ctx context.Context, cb func(string, fuse.DirentType)) error {
				m, err := q.getShards(ctx, shardprefix)
				if err != nil {
					return err
				}
				for k := range m {
					cb(k, fuse.DT_Dir)
				}
				return nil
			},
			Get: func(ctx context.Context, shardID string) (fs.Node, fuse.DirentType, bool, error) {
				newprefix := append(shardprefix, shardID)
				return createSharded(newprefix), fuse.DT_Dir, true, nil
			},
		}
	}

	sharded := createSharded(nil)

	formSelector.Add("list", readstreamfuse.Stream(ctx, func(ctx context.Context, w io.Writer) error {
		logrus.WithFields(logrus.Fields(moreFields(fields, map[string]interface{}{
			"stream": "list",
		}))).Infof("Beginning result stream")

		return q.listAll(ctx, nil, func(entityID string) error {
			logrus.WithFields(logrus.Fields(moreFields(fields, map[string]interface{}{
				"stream":    "list",
				"entity_id": entityID,
			}))).Infof("Continuing result stream")
			_, err := fmt.Fprintf(w, "%s\n", mkAbsCanonicalPath(entityID))
			return err
		})
	}))

	formSelector.Add("all", legacyAll)
	formSelector.Add("shard", sharded)

	if isRoot {
		linkAccessor := &dyndirfuse.DynamicDir{
			Fields:    moreFields(fields, map[string]interface{}{"resultset": "link"}),
			CacheSize: 100,
			List: func(ctx context.Context, cb func(string, fuse.DirentType)) error {
				return nil
			},
			Get: func(ctx context.Context, entityID string) (fs.Node, fuse.DirentType, bool, error) {
				memberType := fuse.DT_Link
				fullPath := mkAbsCanonicalPath(entityID)
				linkNode := linkfuse.Target(fullPath)
				return linkNode, memberType, true, nil
			},
		}
		formSelector.Add("link", linkAccessor)
	}

	return formSelector, nil
}

func addRootNodesForNamespace(shortLivedCtx context.Context, client pb.QMetadataServiceClient, tree *fs.Tree, contextBG context.Context, ns, mountpoint string, shardKey []byte, isFilenameBad func(string) bool) error {
	var nextQueryID int64 = 1

	listAllEntities, err := mkEntitiesListNode(contextBG, client, mountpoint, shardKey, ns, map[string]interface{}{
		"dir":       "entities",
		"namespace": ns,
	}, &entitiesQueryer{
		getNode: func(ctx context.Context, entityID string) (fs.Node, bool, error) {
			if !qmfsquery.ValidFilename(entityID) {
				return nil, false, fmt.Errorf("invalid filename")
			}
			node := getEntityRootNode(ctx, client, ns, entityID, isFilenameBad)
			return node, true, nil
		},
		listAll: func(ctx context.Context, shards []string, report func(string) error) error {
			req := &pb.QueryEntitiesRequest{
				Namespace: ns,
			}
			if len(shards) == 0 {
				req.Kind = &pb.QueryEntitiesRequest_All{
					All: true,
				}
			} else {
				req.Kind = &pb.QueryEntitiesRequest_ParsedQuery{
					ParsedQuery: &pb.EntitiesQuery{
						Clause: []*pb.EntitiesQuery_Clause{
							qmfsquery.EntityIDShards(shards),
						},
					},
				}
			}
			stream, err := client.QueryEntities(ctx, req)
			if err != nil {
				logrus.Warningf("QueryEntities: %v", err)
				return err
			}

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if err := report(resp.EntityId); err != nil {
					return err
				}
			}

			return nil
		},
	}, true)
	if err != nil {
		return err
	}

	tree.Add("entities", listAllEntities)

	queryCtxBG := contextBG

	tree.Add("query", &dyndirfuse.DynamicDir{
		Fields: map[string]interface{}{
			"dir":       "query",
			"namespace": ns,
		},
		CacheSize: 0, // No caching queries.
		List: func(ctx context.Context, cb func(string, fuse.DirentType)) error {
			// Always appears to be empty.
			return nil
		},
		Get: func(shortLivedCtx context.Context, querystring string) (fs.Node, fuse.DirentType, bool, error) {
			parsed, err := qmfsquery.Parse(querystring)
			if err != nil {
				logrus.Errorf("Bad query %q: %v", querystring, err)
				return nil, fuse.DT_Unknown, false, status.Errorf(codes.InvalidArgument, "invalid query: %q", err)
			}

			queryReq := &pb.QueryEntitiesRequest{
				Namespace: ns,
				Kind: &pb.QueryEntitiesRequest_ParsedQuery{
					ParsedQuery: parsed,
				},
			}

			queryID := atomic.AddInt64(&nextQueryID, 1)
			logrus.WithFields(logrus.Fields{
				"namespace":   ns,
				"querystring": querystring,
				"query_id":    queryID,
			}).Infof("Received query")

			listQueryEntities, err := mkEntitiesListNode(queryCtxBG, client, mountpoint, shardKey, ns, map[string]interface{}{
				"dir":         "query/instance",
				"querystring": querystring,
				"namespace":   ns,
				"query_id":    queryID,
			}, &entitiesQueryer{
				listAll: func(ctx context.Context, shards []string, report func(string) error) error {
					cloneIntf := proto.Clone(parsed)
					clone := cloneIntf.(*pb.EntitiesQuery)
					clone.Clause = append(clone.Clause, qmfsquery.EntityIDShards(shards))

					stream, err := client.QueryEntities(ctx, queryReq)
					if err != nil {
						return err
					}

					for {
						resp, err := stream.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							return err
						}

						queryResultCache.Add(queryCacheKey{
							namespace: ns,
							queryID:   queryID,
							entityID:  resp.EntityId,
						}, true)

						if err := report(resp.EntityId); err != nil {
							return err
						}
					}
					return nil
				},
				checkEntityExists: func(ctx context.Context, entityID string) (bool, error) {
					qck := queryCacheKey{
						namespace: ns,
						queryID:   queryID,
						entityID:  entityID,
					}

					result, ok := queryResultCache.Get(qck)

					var verifiedExists bool

					if ok && result != nil {
						verifiedExists = result.(bool)
					}

					if verifiedExists {
						return true, nil
					}

					logrus.WithFields(logrus.Fields{
						"namespace":   ns,
						"querystring": querystring,
						"query_id":    queryID,
						"entity_id":   entityID,
					}).Warningf("Not clear whether entity matches query -- verifying")

					cloneIntf := proto.Clone(parsed)
					clone := cloneIntf.(*pb.EntitiesQuery)
					clone.Clause = append(clone.Clause, qmfsquery.EntityIDEquals(entityID))

					verifyStream, err := client.QueryEntities(ctx, &pb.QueryEntitiesRequest{
						Namespace: ns,
						Kind: &pb.QueryEntitiesRequest_ParsedQuery{
							ParsedQuery: clone,
						},
					})
					if err != nil {
						return false, err
					}

					var rowcount int64

					for {
						_, err := verifyStream.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							return false, err
						}
						rowcount++

						if rowcount > 1 {
							logrus.WithFields(logrus.Fields{
								"namespace":   ns,
								"querystring": querystring,
								"query_id":    queryID,
								"entity_id":   entityID,
							}).Errorf("Verification query returned more than one entry")
							err := status.Errorf(codes.Internal, "verification query returned more than one entry")
							return false, err
						}
					}

					verifiedExists = rowcount > 0
					return verifiedExists, nil
				},
			}, false)
			if err != nil {
				return nil, fuse.DT_Unknown, false, err
			}

			return listQueryEntities, fuse.DT_Dir, true, nil
		},
	})
	return nil
}

func New(ctx context.Context, client pb.QMetadataServiceClient, params Params) (*Filesystem, error) {
	metadata, err := client.GetDatabaseMetadata(ctx, &pb.GetDatabaseMetadataRequest{})
	if err != nil {
		return nil, err
	}

	shardKey := metadata.GetMetadata().GetShardingKey().GetKey()
	if len(shardKey) <= 0 {
		return nil, fmt.Errorf("no sharding key provided")
	}

	var badFilenameREs []*regexp.Regexp
	for _, s := range params.ServiceData.ForbiddenFilenameREs {
		compiled, err := regexp.Compile(s)
		if err != nil {
			return nil, fmt.Errorf("bad forbidden-filename regex %q: %v", s, err)
		}
		badFilenameREs = append(badFilenameREs, compiled)
	}

	isFilenameBad := func(filename string) bool {
		for _, pat := range badFilenameREs {
			if pat.MatchString(filename) {
				return true
			}
		}
		return false
	}

	versioninfoJSON, err := json.MarshalIndent(versioninfo.MakeJSON(), "", "")
	if err != nil {
		return nil, err
	}
	qmfsVersioninfoJSON = string(versioninfoJSON)

	svcTree, err := newServiceTree(ctx, params.ServiceData, client, params.ShutdownChan)
	if err != nil {
		return nil, err
	}

	tree := &fs.Tree{}
	tree.Add("service", svcTree)

	tree.Add("namespace", newNamespaceListNode(client, params.Mountpoint, shardKey, ctx, isFilenameBad))

	if err := addRootNodesForNamespace(ctx, client, tree, ctx, "", params.Mountpoint, shardKey, isFilenameBad); err != nil {
		return nil, err
	}

	return &Filesystem{
		client: client,
		svc:    params.ServiceData,
		root:   tree,
	}, nil
}

func (f *Filesystem) Root() (fs.Node, error) {
	return f.root, nil
}
