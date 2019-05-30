package main

import (
	"bytes"
	"encoding/binary"

	"github.com/btcsuite/btcd/wire"
)

type UtxoView struct {
	isFromCoinbase bool
	height         uint32
	amount         int64
	spent          bool
	pkScript       []byte
}

func (view *UtxoView) encode() ([]byte, error) {
	varLen := wire.VarIntSerializeSize(uint64(len(view.pkScript)))
	container := make([]byte, 4+8+1+varLen+len(view.pkScript))

	w := bytes.NewBuffer(container)
	w.Write(PutUint32(view.compactCoinbaseAndHeight()))
	w.Write(PutUint64(uint64(view.amount)))
	if view.spent {
		w.Write(PutUint8(1))
	} else {
		w.Write(PutUint8(0))
	}
	err := wire.WriteVarInt(w, 0, uint64(varLen))
	if err != nil {
		return nil, err
	}
	w.Write(view.pkScript)

	return w.Bytes(), nil
}

func (view *UtxoView) decode(value []byte) (*UtxoView, error) {
	coinbaseAndHeight := binary.LittleEndian.Uint32(value[0:4])
	amount := binary.LittleEndian.Uint64(value[4:12])

	// read lock script
	r := bytes.NewReader(value[13:])
	length, err := wire.ReadVarInt(r, 0)
	if err != nil {
		return nil, err
	}

	return &UtxoView{
		isFromCoinbase: coinbaseAndHeight&0x01 == 1,
		height:         coinbaseAndHeight / 2,
		amount:         int64(amount),
		spent:          value[12] == 1,
		pkScript:       value[13+wire.VarIntSerializeSize(length):],
	}, nil
}

func (view *UtxoView) compactCoinbaseAndHeight() uint32 {
	if view.isFromCoinbase {
		return view.height*2 | 1
	}

	return view.height * 2
}
