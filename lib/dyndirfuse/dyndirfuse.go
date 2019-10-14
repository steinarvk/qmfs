package dyndirfuse

import (
	"context"
	"fmt"
	"os"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	lru "github.com/hashicorp/golang-lru"
	"github.com/sirupsen/logrus"
	"github.com/steinarvk/sectiontrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DynamicDir struct {
	Fields    map[string]interface{}
	CacheSize int
	List      func(context.Context, func(string, fuse.DirentType)) error
	Get       func(context.Context, string) (fs.Node, fuse.DirentType, bool, error)
	Delete    func(context.Context, string, bool) error
	CreateDir func(context.Context, string) error

	cachemu   sync.Mutex
	nodecache *lru.Cache
}

var removeSec = sectiontrace.New("dyndirfuse.Remove")

func (d *DynamicDir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	err := readDirAllSec.Do(ctx, func(ctx context.Context) error {
		return d.remove(ctx, req)
	})

	logrus.WithFields(d.Fields).Infof("Remove(name=%q, dir=%v) = err: %v", req.Name, req.Dir, err)
	return err
}

func (d *DynamicDir) remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if d.Delete == nil {
		logrus.WithFields(d.Fields).Warningf("no Delete method set")
		return fuse.EIO
	}

	return d.Delete(ctx, req.Name, req.Dir)
}

var readDirAllSec = sectiontrace.New("dyndirfuse.ReadDirAll")

func (d *DynamicDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var rv []fuse.Dirent

	err := readDirAllSec.Do(ctx, func(ctx context.Context) error {
		if d.List == nil {
			return nil
		}

		var tmpRV []fuse.Dirent

		if err := d.List(ctx, func(name string, t fuse.DirentType) {
			tmpRV = append(tmpRV, fuse.Dirent{
				Type: t,
				Name: name,
			})
		}); err != nil {
			logrus.WithFields(d.Fields).Warningf("ReadDirAll() listing failed: %v", err)
			return err
		}

		rv = tmpRV
		return nil
	})

	if err == context.Canceled || err == context.DeadlineExceeded || status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		logrus.WithFields(d.Fields).Errorf("ReadDirAll() failed with cancellation: %v", err)
		return nil, fuse.EINTR
	}

	if err != nil {
		logrus.WithFields(d.Fields).Errorf("ReadDirAll() failed: %v", err)
		return nil, fuse.EIO
	} else {
		logrus.WithFields(d.Fields).Infof("ReadDirAll(): OK")
	}
	return rv, err
}

var createSec = sectiontrace.New("dyndirfuse.Create")

func (d *DynamicDir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	var n fs.Node
	var h fs.Handle
	var err error

	createSec.Do(ctx, func(ctx context.Context) error {
		n, h, err = d.create(ctx, req, resp)
		return err
	})

	logrus.WithFields(d.Fields).Infof("Create(name=%q): err=%v", req.Name, err)

	return n, h, err
}

func (d *DynamicDir) create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	node, _, _, err := d.getMaybeCached(ctx, req.Name)
	if err != nil {
		return nil, nil, err
	}

	openerNode, ok := node.(fs.NodeOpener)
	if !ok {
		return nil, nil, fmt.Errorf("Node is not NodeOpener")
	}

	handle, err := openerNode.Open(ctx, &fuse.OpenRequest{
		Flags: req.Flags,
	}, &fuse.OpenResponse{})
	if err != nil {
		return nil, nil, err
	}

	return node, handle, nil
}

var lookupSec = sectiontrace.New("dyndirfuse.Lookup")

func (d *DynamicDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	var rv fs.Node
	var err error
	err = lookupSec.Do(ctx, func(ctx context.Context) error {
		rv, err = d.lookup(ctx, name)
		return err
	})
	logrus.WithFields(d.Fields).Infof("Lookup(name=%q): err=%v", name, err)
	return rv, err
}

func (d *DynamicDir) lookup(ctx context.Context, name string) (fs.Node, error) {
	node, _, ok, err := d.getMaybeCached(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fuse.ENOENT
	}
	return node, nil
}

var attrSec = sectiontrace.New("dyndirfuse.Attr")

func (d *DynamicDir) Attr(ctx context.Context, a *fuse.Attr) error {
	logrus.WithFields(d.Fields).Infof("Attr()")

	return attrSec.Do(ctx, func(ctx context.Context) error {
		a.Mode = os.ModeDir | 0755
		return nil
	})
}

var getMaybeCachedSec = sectiontrace.New("dyndirfuse.getMaybeCached")

func (d *DynamicDir) getCache() (*lru.Cache, error) {
	d.cachemu.Lock()
	defer d.cachemu.Unlock()

	if d.nodecache == nil {
		cache, err := lru.New(d.CacheSize)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Error creating cache: %v", err)
		}
		d.nodecache = cache
	}

	return d.nodecache, nil
}

type cacheableEntry struct {
	node     fs.Node
	fusetype fuse.DirentType
}

func (d *DynamicDir) getMaybeCached(ctx context.Context, name string) (fs.Node, fuse.DirentType, bool, error) {
	if d.CacheSize <= 0 {
		return d.Get(ctx, name)
	}

	var rvNode fs.Node
	var rvType fuse.DirentType
	var rvOK bool

	err := getMaybeCachedSec.Do(ctx, func(ctx context.Context) error {
		cache, err := d.getCache()
		if err != nil {
			return err
		}

		cached, ok := cache.Get(name)
		if ok {
			entry := cached.(*cacheableEntry)
			rvNode = entry.node
			rvType = entry.fusetype
			rvOK = true
			return nil
		}

		newNode, dirtype, ok, err := d.Get(ctx, name)

		rvNode = newNode
		rvType = dirtype
		rvOK = ok

		if err != nil || !ok {
			// Won't cache an absence or an error.
			return err
		}

		cache.Add(name, &cacheableEntry{
			node:     newNode,
			fusetype: dirtype,
		})

		return nil
	})

	return rvNode, rvType, rvOK, err
}

var mkdirSec = sectiontrace.New("dyndirfuse.Mkdir")

func (d *DynamicDir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	var rv fs.Node
	err := mkdirSec.Do(ctx, func(ctx context.Context) error {
		returned, err := d.mkdir(ctx, req)
		rv = returned
		return err
	})
	logrus.WithFields(d.Fields).Infof("Mkdir(%q) = err: %v", req.Name, err)
	return rv, err
}

func (d *DynamicDir) mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if d.Mkdir == nil {
		return nil, fuse.EIO
	}
	err := d.CreateDir(ctx, req.Name)
	if err != nil {
		return nil, fuse.EIO
	}

	rv, _, _, err := d.Get(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	return rv, nil
}
