package readstreamfuse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/steinarvk/sectiontrace"
)

type Handle struct {
	mu         sync.Mutex
	cond       *sync.Cond
	buf        *bytes.Buffer
	delayedErr error
	released   bool
	closed     bool
	bytesRead  int64
}

var readSec = sectiontrace.New("readstreamfuse.Read")

func (h *Handle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	return readSec.Do(ctx, func(ctx context.Context) error {
		h.mu.Lock()
		defer h.mu.Unlock()

		if h.delayedErr != nil {
			return h.delayedErr
		}

		if h.released {
			return fuse.EIO
		}

		offset := req.Offset
		if offset != h.bytesRead {
			return fuse.EIO
		}

		sz := req.Size

		for h.buf.Len() == 0 {
			if h.closed {
				return nil
			}
			h.cond.Wait()
		}

		respbytes := make([]byte, sz)

		n, err := h.buf.Read(respbytes)
		h.bytesRead += int64(n)

		if err != nil {
			return fuse.EIO
		}

		resp.Data = respbytes[:n]

		return nil
	})
}

func (h *Handle) setError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.delayedErr = err
}

func (h *Handle) setClosed() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.cond.Broadcast()
	h.closed = true
}

func (h *Handle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.released = true

	return nil
}

func (h *Handle) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.released {
		return 0, fmt.Errorf("stream broken")
	}

	n, err := h.buf.Write(p)

	h.cond.Broadcast()
	return n, err
}

type File struct {
	bgctx    context.Context
	streamer func(context.Context, io.Writer) error
}

func Stream(bgctx context.Context, cb func(context.Context, io.Writer) error) *File {
	return &File{bgctx: bgctx, streamer: cb}
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	if !req.Flags.IsReadOnly() {
		return nil, fuse.EIO
	}
	rv := &Handle{
		buf: bytes.NewBuffer(nil),
	}

	rv.mu.Lock()
	defer rv.mu.Unlock()
	rv.cond = sync.NewCond(&rv.mu)

	go func() {
		err := f.streamer(f.bgctx, rv)
		defer rv.setClosed()
		if err != nil {
			rv.setError(err)
		}
	}()

	resp.Flags = fuse.OpenDirectIO | fuse.OpenNonSeekable

	return rv, nil
}

var attrSec = sectiontrace.New("readstreamfuse.Attr")

func (s *File) Attr(ctx context.Context, a *fuse.Attr) error {
	return attrSec.Do(ctx, func(ctx context.Context) error {
		a.Mode = 0444
		a.Size = 0

		return nil
	})
}
