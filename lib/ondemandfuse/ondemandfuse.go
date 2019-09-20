package ondemandfuse

import (
	"context"
	"strings"

	"bazil.org/fuse"
	"github.com/steinarvk/sectiontrace"
)

func String(cb func(context.Context) (string, error)) *File {
	return &File{
		contents: func(ctx context.Context) ([]byte, error) {
			contents, err := cb(ctx)
			if err != nil {
				return nil, err
			}
			return []byte(strings.TrimSpace(contents) + "\n"), nil
		},
	}
}

type File struct {
	contents func(ctx context.Context) ([]byte, error)
}

var attrSec = sectiontrace.New("ondemandfuse.Attr")

func (s *File) Attr(ctx context.Context, a *fuse.Attr) error {
	return attrSec.Do(ctx, func(ctx context.Context) error {
		data, err := s.contents(ctx)
		if err != nil {
			return err
		}

		a.Mode = 0444
		a.Size = uint64(len(data))

		return nil
	})
}

var readAllSec = sectiontrace.New("ondemandfuse.ReadAll")

func (s *File) ReadAll(ctx context.Context) ([]byte, error) {
	var data []byte
	err := readAllSec.Do(ctx, func(ctx context.Context) error {
		outdata, err := s.contents(ctx)
		data = outdata
		return err
	})
	return data, err
}
