package atomicfilefuse

import (
	"bytes"
	"context"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/sirupsen/logrus"
	"github.com/steinarvk/sectiontrace"
)

type WriteMetadata struct {
	LastRevision string
}

type Handle struct {
	mu           sync.Mutex
	lazy         bool
	trueTruncate bool
	originalData []byte
	data         []byte
	lastRevision string
	present      bool
	file         *File
}

var hEnsureRead = sectiontrace.New("atomicfilefuse.handle.ensureRead")

func (h *Handle) holdingLockEnsureFileWasRead(ctx context.Context) error {
	return hEnsureRead.Do(ctx, func(ctx context.Context) error {
		if !h.lazy {
			return nil
		}

		currentData, currentRevision, present, err := h.file.AtomicRead(ctx)
		if err != nil {
			return err
		}

		databuf := make([]byte, len(currentData))
		copy(databuf, currentData)

		if h.trueTruncate {
			databuf = nil
		}

		h.originalData = currentData
		h.data = databuf
		h.lastRevision = currentRevision
		h.present = present

		h.lazy = false

		return nil
	})
}

var hReadAllSec = sectiontrace.New("atomicfilefuse.handle.ReadAll")

func (h *Handle) ReadAll(ctx context.Context) ([]byte, error) {
	logrus.WithFields(h.file.Fields).Infof("handle.ReadAll()")
	defer func() {
		logrus.WithFields(h.file.Fields).Infof("handle.ReadAll() done ")
	}()

	var rv []byte

	err := hReadAllSec.Do(ctx, func(ctx context.Context) error {
		h.mu.Lock()
		defer h.mu.Unlock()

		if err := h.holdingLockEnsureFileWasRead(ctx); err != nil {
			return err
		}

		if !h.present {
			return fuse.ENOENT
		}

		rv = h.data
		return nil
	})

	return rv, err
}

func zeropad(xs []byte, toSize int64) []byte {
	if growBy := toSize - int64(len(xs)); growBy > 0 {
		zero := make([]byte, growBy)
		xs = append(xs, zero...)
	}
	return xs
}

var hWriteSec = sectiontrace.New("atomicfilefuse.handle.Write")

func (h *Handle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	logrus.WithFields(h.file.Fields).Infof("handle.Write()")
	defer func() {
		logrus.WithFields(h.file.Fields).Infof("handle.Write() done ")
	}()

	return hWriteSec.Do(ctx, func(ctx context.Context) error {
		h.mu.Lock()
		defer h.mu.Unlock()

		if err := h.holdingLockEnsureFileWasRead(ctx); err != nil {
			return err
		}

		writeFinishesAt := req.Offset + int64(len(req.Data))

		if h.file.SizeLimit > 0 && writeFinishesAt > h.file.SizeLimit {
			return fuse.EIO
		}

		h.data = zeropad(h.data, writeFinishesAt)

		nWritten := copy(h.data[req.Offset:], req.Data)

		resp.Size = nWritten

		return nil
	})
}

var hFlushSec = sectiontrace.New("atomicfilefuse.handle.Flush")

func (h *Handle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	logrus.WithFields(h.file.Fields).Infof("handle.Flush()")
	defer func() {
		logrus.WithFields(h.file.Fields).Infof("handle.Flush() done ")
	}()

	return hFlushSec.Do(ctx, func(ctx context.Context) error {
		h.mu.Lock()
		defer h.mu.Unlock()

		if err := h.holdingLockEnsureFileWasRead(ctx); err != nil {
			return err
		}

		if h.present && bytes.Compare(h.originalData, h.data) == 0 {
			logrus.WithFields(h.file.Fields).Infof("Not performing write (not modified)")
			return nil
		}

		if h.file.SizeLimit > 0 && int64(len(h.data)) > h.file.SizeLimit {
			logrus.WithFields(h.file.Fields).Warningf("Rejecting write (file size limit exceeded)")
			return fuse.EIO
		}

		err := h.file.AtomicWrite(ctx, h.data, h.lastRevision)

		if err == nil {
			h.originalData = h.data
		}

		if err != nil {
			logrus.WithFields(h.file.Fields).Errorf("handle.Flush() failed: %v", err)
		}

		return err
	})
}

type File struct {
	Fields      map[string]interface{}
	SizeLimit   int64
	GetAttr     func(ctx context.Context, a *fuse.Attr) (bool, error)
	AtomicRead  func(ctx context.Context) ([]byte, string, bool, error)
	AtomicWrite func(ctx context.Context, data []byte, revision string) error
}

var attrSec = sectiontrace.New("atomicfilefuse.Attr")

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	logrus.WithFields(f.Fields).Infof("Attr()")
	defer func() {
		logrus.WithFields(f.Fields).Infof("Attr() done ")
	}()

	return attrSec.Do(ctx, func(ctx context.Context) error {
		if f.GetAttr != nil {
			_, err := f.GetAttr(ctx, a)
			return err
		}
		data, _, _, err := f.AtomicRead(ctx)
		if err != nil {
			return err
		}
		a.Mode = 0
		a.Size = uint64(len(data))
		return nil
	})
}

var openSec = sectiontrace.New("atomicfilefuse.Open")

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var rv fs.Handle
	var err error

	openSec.Do(ctx, func(ctx context.Context) error {
		rv, err = f.open(ctx, req, resp)
		return err
	})

	logrus.WithFields(f.Fields).Infof("Open() = err: %v", err)

	return rv, err
}

func (f *File) open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return &Handle{
		lazy:         true,
		trueTruncate: req.Flags&fuse.OpenTruncate != 0,
		file:         f,
	}, nil
}

var resizeFileSec = sectiontrace.New("atomicfilefuse.resizeFile")

func (f *File) resizeFile(ctx context.Context, newSize int64) error {
	logrus.WithFields(f.Fields).Infof("ResizeFile(%d)", newSize)
	defer func() {
		logrus.WithFields(f.Fields).Infof("ResizeFile(%d) done", newSize)
	}()

	return resizeFileSec.Do(ctx, func(ctx context.Context) error {
		var newData []byte
		var lastRevision string

		if newSize > 0 {
			data, rev, _, err := f.AtomicRead(ctx)
			if err != nil {
				return err
			}

			lastRevision = rev
			newData = data

			if newSize < int64(len(data)) {
				newData = newData[:newSize]
			} else if newSize > int64(len(data)) {
				newData = zeropad(data, newSize)
			}
		}

		return f.AtomicWrite(ctx, newData, lastRevision)
	})
}

var setattrSec = sectiontrace.New("atomicfilefuse.Attr")

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	logrus.WithFields(f.Fields).Infof("Setattr()")
	defer func() {
		logrus.WithFields(f.Fields).Infof("Setattr() done")
	}()

	return setattrSec.Do(ctx, func(ctx context.Context) error {
		// Unfortunately, this method of truncation is not atomic.
		// "lazy open" is to handle the common access pattern: Open, Setattr, Write.

		settingSize := (req.Valid & fuse.SetattrSize) != 0

		if settingSize {
			if err := f.resizeFile(ctx, int64(req.Size)); err != nil {
				return err
			}
		}

		return nil
	})
}
