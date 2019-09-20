package linkfuse

import (
	"context"
	"os"

	"bazil.org/fuse"
	"github.com/steinarvk/sectiontrace"
)

func Target(s string) *Link {
	return &Link{s}
}

type Link struct {
	target string
}

var attrSec = sectiontrace.New("linkfuse.Attr")

func (s *Link) Attr(ctx context.Context, a *fuse.Attr) error {
	return attrSec.Do(ctx, func(ctx context.Context) error {
		a.Mode = 0444 | os.ModeSymlink
		return nil
	})
}

func (s *Link) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	return s.target, nil
}
