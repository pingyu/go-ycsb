package tikv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type RawKVClient interface {
	Close() error
	Get(ctx context.Context, key []byte, options ...rawkv.RawOption) (value []byte, err error)
	BatchGet(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) (values [][]byte, err error)
	PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...rawkv.RawOption) error
	GetKeyTTL(ctx context.Context, key []byte, options ...rawkv.RawOption) (ttl *uint64, err error)
	Put(ctx context.Context, key, value []byte, options ...rawkv.RawOption) error
	BatchPut(ctx context.Context, keys, values [][]byte, options ...rawkv.RawOption) error
	BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...rawkv.RawOption) error
	Delete(ctx context.Context, key []byte, options ...rawkv.RawOption) error
	BatchDelete(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) error
	DeleteRange(ctx context.Context, startKey []byte, endKey []byte, options ...rawkv.RawOption) error
	Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error)
	ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error)
	CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...rawkv.RawOption) (value []byte, success bool, err error)
	Checksum(ctx context.Context, startKey, endKey []byte, options ...rawkv.RawOption) (check rawkv.RawChecksum, err error)
}

var _ RawKVClient = (*rawkv.Client)(nil)
var _ RawKVClient = (*ClientWithRetry)(nil)

type BuildClientFn func() (*rawkv.Client, error)

type rawkvClientWrapper struct {
	Inner *rawkv.Client
	Err   error
}

// sync.Pool will create object even the pool is not empty, and consume too much memory, and eventually OOM killed.
// So a single pool is used here.
type SinglePool struct {
	stack []*rawkvClientWrapper
	mu    sync.Mutex
	newFn func() *rawkvClientWrapper
}

func NewSinglePool(newFn func() *rawkvClientWrapper) *SinglePool {
	return &SinglePool{
		stack: make([]*rawkvClientWrapper, 0, 1000),
		mu:    sync.Mutex{},
		newFn: newFn,
	}
}

func (p *SinglePool) Put(w *rawkvClientWrapper) {
	if w != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.stack = append(p.stack, w)
	}
}

func (p *SinglePool) Get() (w *rawkvClientWrapper) {
	p.mu.Lock()
	if len(p.stack) > 0 {
		w = p.stack[len(p.stack)-1]
		p.stack = p.stack[:len(p.stack)-1]
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	return p.newFn()
}

// ClientWithRetry is a work around client for rawkv.Client.
// As rawkv.Client seems to not be able to recovery connection when tikv-server is killed.
// See https://github.com/tikv/client-go/issues/522.
type ClientWithRetry struct {
	retryCnt       int
	retryInterval  time.Duration
	rebuildOnError bool
	pool           *SinglePool
	isClosed       bool
}

func NewClientWithRetry(buildClientFn BuildClientFn, retryCnt int, retryInterval time.Duration, rebuildOnError bool) (*ClientWithRetry, error) {
	client := &ClientWithRetry{retryCnt, retryInterval, rebuildOnError, nil, false}
	pool := NewSinglePool(func() *rawkvClientWrapper {
		if !client.IsClosed() {
			cli, err := buildClientFn()
			return &rawkvClientWrapper{cli, err}
		}
		return nil
	})
	client.SetPool(pool)
	return client, nil
}

func (c *ClientWithRetry) Close() error {
	c.isClosed = true
	cnt := 0
	for {
		cli := c.pool.Get()
		if cli == nil {
			log.Warn("INFO: ClientWithRetry.Close", zap.Int("poolSize", cnt))
			return nil
		}
		if cli.Err == nil {
			cli.Inner.Close()
			cnt += 1
		}
	}
}

func (c *ClientWithRetry) SetPool(pool *SinglePool) {
	c.pool = pool
}

func (c *ClientWithRetry) IsClosed() bool {
	return c.isClosed
}

func (c *ClientWithRetry) withRetry(ctx context.Context, f func(*rawkv.Client) error) (err error) {
	i := 0
	for {
		{
			cli := c.pool.Get()
			putBack := func() {
				if cli != nil && cli.Err == nil {
					c.pool.Put(cli)
				}
				cli = nil
			}

			e := cli.Err
			if e == nil {
				if e = f(cli.Inner); e == nil {
					putBack()
					return nil
				} else if c.rebuildOnError {
					cli.Inner.Close()
					cli = nil
				}
			}
			err = multierr.Append(err, errors.Trace(e))
			log.Warn("ClientWithRetry.withRetry error", zap.Int("retryCnt", i), zap.Error(err))

			putBack()
			if i >= c.retryCnt {
				return err
			}
		}

		i += 1
		select {
		case <-ctx.Done():
			return err
		default:
			// Do NOT use `<-time.After(c.retryInterval)`. Timer consumes lots of memory
			time.Sleep(c.retryInterval)
		}
	}
}

func (c *ClientWithRetry) Get(ctx context.Context, key []byte, options ...rawkv.RawOption) (value []byte, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			value, e = cli.Get(ctx, key, options...)
			return e
		},
	)
	return
}

func (c *ClientWithRetry) BatchGet(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) (values [][]byte, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			values, e = cli.BatchGet(ctx, keys, options...)
			return e
		},
	)
	return
}

func (c *ClientWithRetry) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.PutWithTTL(ctx, key, value, ttl, options...)
		},
	)
}

func (c *ClientWithRetry) GetKeyTTL(ctx context.Context, key []byte, options ...rawkv.RawOption) (ttl *uint64, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			ttl, e = cli.GetKeyTTL(ctx, key, options...)
			return e
		},
	)
	return
}

func (c *ClientWithRetry) Put(ctx context.Context, key, value []byte, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.Put(ctx, key, value, options...)
		},
	)
}

func (c *ClientWithRetry) BatchPut(ctx context.Context, keys, values [][]byte, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.BatchPut(ctx, keys, values, options...)
		},
	)
}

func (c *ClientWithRetry) BatchPutWithTTL(ctx context.Context, keys, values [][]byte, ttls []uint64, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.BatchPutWithTTL(ctx, keys, values, ttls, options...)
		},
	)
}

func (c *ClientWithRetry) Delete(ctx context.Context, key []byte, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.Delete(ctx, key, options...)
		},
	)
}

func (c *ClientWithRetry) BatchDelete(ctx context.Context, keys [][]byte, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.BatchDelete(ctx, keys, options...)
		},
	)
}

func (c *ClientWithRetry) DeleteRange(ctx context.Context, startKey []byte, endKey []byte, options ...rawkv.RawOption) error {
	return c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			return cli.DeleteRange(ctx, startKey, endKey, options...)
		},
	)
}

func (c *ClientWithRetry) Scan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption,
) (keys [][]byte, values [][]byte, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			keys, values, e = cli.Scan(ctx, startKey, endKey, limit, options...)
			return e
		},
	)
	return
}

func (c *ClientWithRetry) ReverseScan(ctx context.Context, startKey, endKey []byte, limit int, options ...rawkv.RawOption) (keys [][]byte, values [][]byte, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			keys, values, e = cli.ReverseScan(ctx, startKey, endKey, limit, options...)
			return e
		},
	)
	return
}

func (c *ClientWithRetry) CompareAndSwap(ctx context.Context, key, previousValue, newValue []byte, options ...rawkv.RawOption) (value []byte, success bool, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			value, success, e = cli.CompareAndSwap(ctx, key, previousValue, newValue, options...)
			return e
		},
	)
	return
}

func (c *ClientWithRetry) Checksum(ctx context.Context, startKey, endKey []byte, options ...rawkv.RawOption) (checksum rawkv.RawChecksum, err error) {
	err = c.withRetry(
		ctx,
		func(cli *rawkv.Client) error {
			var e error
			checksum, e = cli.Checksum(ctx, startKey, endKey, options...)
			return e
		},
	)
	return
}

var (
	clientCounter uint64
)

func NewRawKVClient(ctx context.Context, pdAddrs []string, opts ...rawkv.ClientOpt) (*rawkv.Client, error) {
	cli, err := rawkv.NewClientWithOpts(ctx, pdAddrs, opts...)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&clientCounter, 1)
	return cli, err
}

func NewRawKVClientWithRetry(
	ctx context.Context,
	pdAddrs []string,
	rebuildOnError bool,
	retryCnt int,
	retryInterval time.Duration,
	opts ...rawkv.ClientOpt) (RawKVClient, error) {
	buildClient := func() (*rawkv.Client, error) {
		return NewRawKVClient(ctx, pdAddrs, opts...)
	}

	clientWithRetry, err := NewClientWithRetry(buildClient, retryCnt, retryInterval, rebuildOnError)
	if err != nil {
		return nil, err
	}
	return clientWithRetry, nil
}
