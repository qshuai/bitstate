package main

func PutUint8(v uint8) []byte {
	return []byte{v}
}

func PutUint16(v uint16) []byte {
	b := make([]byte, 2)

	b[0] = byte(v)
	b[1] = byte(v >> 8)

	return b
}

func PutUint32(v uint32) []byte {
	b := make([]byte, 4)

	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)

	return b
}

func PutUint64(v uint64) []byte {
	b := make([]byte, 8)

	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)

	return b
}
