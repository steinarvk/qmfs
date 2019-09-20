package changewatch

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"
)

type Options struct {
	Delay  time.Duration
	Action func(context.Context) error
}

type Watch struct {
	opts Options
	ch   chan struct{}
}

func (w *Watch) OnChange() {
	if w == nil {
		return
	}

	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func worker(ctx context.Context, ch chan struct{}, opts Options) error {
	timerCh := make(chan struct{}, 1)
	defer close(timerCh)

	waitingForFlush := false

	for {
		select {
		case <-ctx.Done():
			close(ch)

			return ctx.Err()

		case <-ch:
			if !waitingForFlush {
				waitingForFlush = true
				time.AfterFunc(opts.Delay, func() {
					timerCh <- struct{}{}
				})
			}

		case <-timerCh:
			waitingForFlush = false

			if err := opts.Action(ctx); err != nil {
				logrus.Errorf("changewatch action failed: %v", err)
			}
		}
	}

	return nil
}

func New(ctx context.Context, opts Options) (*Watch, error) {
	if opts.Action == nil {
		return nil, nil
	}

	ch := make(chan struct{}, 1)

	go worker(ctx, ch, opts)

	return &Watch{
		opts: opts,
		ch:   ch,
	}, nil
}
