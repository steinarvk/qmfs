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

		truncated := h.file.state.IsLazilyTruncated()

		currentData, currentRevision, present, err := h.file.AtomicRead(ctx)
		if err != nil {
			return err
		}

		if !present && truncated {
			// Lazy truncation implies lazy creation.
			present = true
		}

		databuf := make([]byte, len(currentData))
		copy(databuf, currentData)

		if truncated || h.trueTruncate {
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

func (h *Handle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	flush := (req.ReleaseFlags & fuse.ReleaseFlush) != 0
	if flush {
		logrus.Debugf("Flushing on release.")
		if err := h.Flush(ctx, nil); err != nil {
			logrus.Errorf("Flush on release failed: %v", err)
		}
	}

	logrus.Debugf("Handle released: removing reference.")
	h.file.state.RemoveRef(ctx, h)
	logrus.Debugf("Handle release finished.")

	return nil
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
	// Note: must not use fields in FlushRequest, otherwise refactor Release.

	logrus.WithFields(h.file.Fields).Infof("handle.Flush()")
	defer func() {
		logrus.WithFields(h.file.Fields).Infof("handle.Flush() done ")
	}()

	return hFlushSec.Do(ctx, func(ctx context.Context) error {
		h.mu.Lock()

		if err := h.holdingLockEnsureFileWasRead(ctx); err != nil {
			h.mu.Unlock()
			return err
		}

		if len(h.data) == 0 {
			logrus.WithFields(h.file.Fields).Infof("Converting flush to lazy truncation")

			h.mu.Unlock()
			return h.file.state.SetLazilyTruncated(ctx, h.file)
		}

		defer h.mu.Unlock()

		if h.present && bytes.Compare(h.originalData, h.data) == 0 {
			h.file.state.ClearLazilyTruncated()
			logrus.WithFields(h.file.Fields).Infof("Not performing write (not modified)")
			return nil
		}

		if h.file.SizeLimit > 0 && int64(len(h.data)) > h.file.SizeLimit {
			logrus.WithFields(h.file.Fields).Warningf("Rejecting write (file size limit exceeded)")
			return fuse.EIO
		}

		newLastRevision, err := h.file.AtomicWrite(ctx, h.data, h.lastRevision)

		if err == nil {
			h.lastRevision = newLastRevision
			h.file.state.ClearLazilyTruncated()
			h.originalData = h.data
		}

		if err != nil {
			logrus.WithFields(h.file.Fields).Errorf("handle.Flush() failed: %v", err)
		}

		return err
	})
}

type FileState struct {
	mu       sync.Mutex
	m        map[*Handle]struct{}
	truncate bool
	file     *File
}

func (s *FileState) getFields() logrus.Fields {
	if p := s.file; p != nil {
		return p.Fields
	}
	return logrus.Fields{}
}

func (s *FileState) ClearLazilyTruncated() {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.WithFields(s.getFields()).Debugf("Clearing lazy-truncate flag.")

	s.truncate = false
}

func (s *FileState) SetLazilyTruncated(ctx context.Context, file *File) error {
	s.mu.Lock()

	s.file = file

	if len(s.m) == 0 {
		logrus.WithFields(s.getFields()).Infof("No handles open: eagerly truncating.")

		// in this case, eagerly truncate.
		s.mu.Unlock()
		return file.resizeFile(ctx, 0)
	}

	defer s.mu.Unlock()

	logrus.WithFields(s.getFields()).Debugf("Setting lazy-truncate flag.")

	s.truncate = true

	return nil
}

func (s *FileState) IsLazilyTruncated() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.WithFields(s.getFields()).Debugf("Checked lazy-truncate flag: %v", s.truncate)

	return s.truncate
}

func (r *FileState) RemoveRef(ctx context.Context, h *Handle) {
	r.mu.Lock()

	_, ok := r.m[h]
	if ok {
		delete(r.m, h)
	}

	if len(r.m) == 0 && r.truncate {
		r.mu.Unlock()
		err := r.file.resizeFile(ctx, 0)
		if err != nil {
			logrus.Errorf("Last-minute truncate failed: %v", err)
		}
		logrus.Debugf("Finished last-minute truncate!")
		return
	}

	r.mu.Unlock()
}

func (r *FileState) AddRef(h *Handle) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.m == nil {
		r.m = map[*Handle]struct{}{}
	}

	r.m[h] = struct{}{}
}

// Set the lazily truncated flag.
// Whenever a new handle is opened, check for it.
// Whenever a handle performs a write, clear it.
// When a handle is closed, if it was the last handle and the flag is set,
//   perform a non-lazy truncation.

type File struct {
	state       FileState
	Fields      map[string]interface{}
	SizeLimit   int64
	GetAttr     func(ctx context.Context, a *fuse.Attr) (bool, error)
	AtomicRead  func(ctx context.Context) ([]byte, string, bool, error)
	AtomicWrite func(ctx context.Context, data []byte, revision string) (string, error)
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
	rv := &Handle{
		lazy:         true,
		trueTruncate: req.Flags&fuse.OpenTruncate != 0,
		file:         f,
	}
	f.state.AddRef(rv)
	return rv, nil
}

func (f *File) lazilyResizeFile(ctx context.Context, newSize int64) error {
	if newSize > 0 {
		return f.resizeFile(ctx, newSize)
	}

	// Lazy truncation.
	// If there are no handles open: okay, just truncate the file.
	// If there are handles open:
	//   - if any of them have _not_ yet done a read, mark truncated.
	//   - if any of them has done a read, ignore it.
	//   - when all of these handles are released
	//     - UNLESS one of them performs a write first, in which case cancel
	return f.state.SetLazilyTruncated(ctx, f)
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

		_, err := f.AtomicWrite(ctx, newData, lastRevision)
		if err == nil {
			f.state.ClearLazilyTruncated()
		}
		return err
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
			if err := f.lazilyResizeFile(ctx, int64(req.Size)); err != nil {
				return err
			}
		}

		return nil
	})
}
