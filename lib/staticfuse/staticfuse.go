package staticfuse

import (
	"context"
	"encoding/json"
	"strings"

	"bazil.org/fuse"
	"github.com/steinarvk/sectiontrace"
)

func Bytes(x []byte) *Static {
	return &Static{x}
}

func String(s string) *Static {
	s = strings.TrimSpace(s) + "\n"
	return &Static{[]byte(s)}
}

func JSON(value interface{}) (*Static, error) {
	marshalled, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	return String(string(marshalled)), nil
}

type Static struct {
	contents []byte
}

var attrSec = sectiontrace.New("staticfuse.Attr")

func (s *Static) Attr(ctx context.Context, a *fuse.Attr) error {
	return attrSec.Do(ctx, func(ctx context.Context) error {
		a.Mode = 0444
		a.Size = uint64(len(s.contents))
		return nil
	})
}

func (s *Static) ReadAll(ctx context.Context) ([]byte, error) {
	return s.contents, nil
}
