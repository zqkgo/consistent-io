package consistentio

import (
	"io"

	"github.com/golang/groupcache/consistenthash"
)

type opt func(o *Options)

type Options struct {
	Replicas int
	Hash     consistenthash.Hash
	Writers  []writer
}

func AddWriter(key string, w io.Writer) opt {
	return func(o *Options) {
		o.Writers = append(o.Writers, writer{w, key})
	}
}

func Replicas(n int) opt {
	return func(o *Options) {
		o.Replicas = n
	}
}

func Hash(h consistenthash.Hash) opt {
	return func(o *Options) {
		o.Hash = h
	}
}
