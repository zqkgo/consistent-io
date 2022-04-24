package consistentio

import (
	"fmt"
	"io"

	"github.com/golang/groupcache/consistenthash"
)

type writer struct {
	w   io.Writer
	key string
}

// ConsistentIO write to io.Writer(s) consistently.
type ConsistentIO struct {
	o *Options

	writers map[string]io.Writer
	ring    *consistenthash.Map
}

func NewConsistentIO(opts ...opt) (*ConsistentIO, error) {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}

	n := len(o.Writers)
	if n == 0 {
		return nil, fmt.Errorf("writer(s) not specified")
	}

	var (
		m    = make(map[string]io.Writer, n)
		keys = make([]string, n)
	)

	for i := 0; i < len(o.Writers); i++ {
		wrt := o.Writers[i]
		m[wrt.key] = wrt.w
		keys[i] = wrt.key
	}

	r := consistenthash.New(o.Replicas, o.Hash)
	r.Add(keys...)

	cio := &ConsistentIO{
		o:       o,
		writers: m,
		ring:    r,
	}
	return cio, nil
}

func (cio *ConsistentIO) Write(key string, p []byte) (n int, err error) {
	w := cio.writers[cio.ring.Get(key)]
	return w.Write(p)
}
