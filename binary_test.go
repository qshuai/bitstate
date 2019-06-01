package main

import (
	"bytes"
	"testing"
)

func TestCompressedUint32(t *testing.T) {
	b := CompressedUint32(2345)
	expected := []byte{41, 9}
	if !(bytes.Equal(b, expected)) {
		t.Errorf("encode compressed uint32 integer failed,"+
			" want: %v, but got: %v", expected, b)
	}
}
