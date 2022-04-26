package consistentio

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsistentIO(t *testing.T) {
	_, err := NewConsistentIO()
	require.NotNil(t, err)

	_, err = NewConsistentIO(
		Replicas(-1),
		Hash(nil),
	)
	require.NotNil(t, err)

	k1 := "93b3e49e"
	k2 := "9228c"
	k3 := "c748cbe16d5239"

	w1 := bytes.NewBuffer([]byte{})
	w2 := bytes.NewBuffer([]byte{})
	w3 := bytes.NewBuffer([]byte{})

	cio, err := NewConsistentIO(
		Replicas(50),
		AddWriter(k1, w1),
		AddWriter(k2, w2),
		AddWriter(k3, w3),
	)
	require.Nil(t, err)
	require.NotNil(t, cio)

	n, err := cio.Write(k1, []byte("Hello,"))
	require.Nil(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, 6, w3.Len())
	n, err = cio.Write(k1, []byte(" "))
	require.Nil(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 7, w3.Len())
	n, err = cio.Write(k1, []byte("World"))
	require.Nil(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, 12, w3.Len())
	require.Equal(t, "Hello, World", w3.String())

	n, err = cio.Write(k2, []byte("f1529ad"))
	require.Nil(t, err)
	require.Equal(t, 7, n)
	require.Equal(t, 7, w2.Len())

	n, err = cio.Write(k3, []byte("f45efcf84fc3"))
	require.Nil(t, err)
	require.Equal(t, 12, n)
	require.Equal(t, 12, w1.Len())

	n, err = cio.Write(k2, []byte("d7"))
	require.Nil(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 9, w2.Len())

	n, err = cio.Write(k3, []byte("0d373c2c803"))
	require.Nil(t, err)
	require.Equal(t, 11, n)
	require.Equal(t, 23, w1.Len())

	require.Equal(t, "f45efcf84fc30d373c2c803", w1.String())
	require.Equal(t, "f1529add7", w2.String())
	require.Equal(t, "Hello, World", w3.String())
}
