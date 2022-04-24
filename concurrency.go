package consistentio

import (
	"fmt"
	"sync/atomic"
)

type Err struct {
	n     int
	err   error
	reqID uint64
}

type info struct {
	reqID uint64
	key   string
	data  []byte
	err   chan Err
}

// ConcurrentConsistentIO write to io.Writer(s) concurrently.
type ConcurrentConsistentIO struct {
	cio *ConsistentIO

	chs   map[string]chan info
	reqID uint64
}

func NewConcurrentConsistentIO(cio *ConsistentIO) (*ConcurrentConsistentIO, error) {
	if cio == nil {
		return nil, fmt.Errorf("ConsistentIO has not been specified")
	}
	o := cio.o
	if o == nil {
		return nil, fmt.Errorf("options has not been specified")
	}
	routines := len(o.Writers)
	if routines == 0 {
		return nil, fmt.Errorf("writers have not been specified")
	}

	ccio := &ConcurrentConsistentIO{
		cio: cio,
	}

	chs := make(map[string]chan info)
	for i := 0; i < routines; i++ {
		ch := make(chan info, 10) // FIXME: add to options
		w := o.Writers[i]
		chs[w.key] = ch
		go ccio.write(ch)
	}
	ccio.chs = chs

	return ccio, nil
}

func (ccio *ConcurrentConsistentIO) Write(key string, p []byte) (reqID uint64, ch chan Err) {
	reqID = atomic.AddUint64(&ccio.reqID, 1)
	ch = make(chan Err, 1)
	ccio.chs[key] <- info{
		reqID: reqID,
		key:   key,
		data:  p,
		err:   ch,
	}
	return reqID, ch
}

func (ccio *ConcurrentConsistentIO) write(ch chan info) {
	for i := range ch {
		k := i.key
		w := ccio.cio.writers[k]
		n, err := w.Write(i.data)
		i.err <- Err{
			n:     n,
			err:   err,
			reqID: i.reqID,
		}
	}
}
