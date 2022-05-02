package consistentio

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentConsistentIO(t *testing.T) {
	_, err := NewConcurrentConsistentIO(nil)
	require.NotNil(t, err)

	k1, w1 := "k1", bytes.NewBuffer(nil)
	cio, err := NewConsistentIO(AddWriter(k1, w1))
	require.Nil(t, err)
	cio.o = nil
	_, err = NewConcurrentConsistentIO(cio)
	require.NotNil(t, err)

	cio, err = NewConsistentIO(AddWriter(k1, w1))
	require.Nil(t, err)
	cio.o.Writers = nil
	_, err = NewConcurrentConsistentIO(cio)
	require.NotNil(t, err)

	cio, err = NewConsistentIO(AddWriter(k1, w1))
	require.Nil(t, err)
	_, err = NewConcurrentConsistentIO(cio)
	require.Nil(t, err)
}

func TestConcurrentConsistentIOWrite(t *testing.T) {
	var (
		opts []opt
		ks   []int
		n    = 1000
		ws   = make(map[string]*bytes.Buffer, n)
	)
	for i := 0; i < n; i++ {
		k, w := fmt.Sprintf("%d", i), bytes.NewBuffer(nil)
		ws[k] = w
		ks = append(ks, i)
		opts = append(opts, AddWriter(k, w))
	}

	cio, err := NewConsistentIO(opts...)
	require.Nil(t, err)
	require.Equal(t, n, len(cio.writers))

	ccio, err := NewConcurrentConsistentIO(cio)
	require.Nil(t, err)

	var (
		nums []int
		mu   sync.Mutex
		wg   sync.WaitGroup
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%d", i)
		go func(k string) {
			defer wg.Done()
			reqID, ch := ccio.Write(k, []byte(k))
			e := <-ch
			require.Nil(t, e.err)
			require.Equal(t, reqID, e.reqID)
			i, err := strconv.Atoi(ws[k].String())
			require.Nil(t, err)
			mu.Lock()
			nums = append(nums, i)
			mu.Unlock()
		}(k)
	}
	wg.Wait()
	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})
	require.Equal(t, ks, nums)
}
