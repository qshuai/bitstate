package handler

import (
	"bytes"
	"encoding/binary"

	"github.com/btcsuite/btcd/wire"
	"github.com/qshuai/bitstate/utils"
)

const (
	UnSpendTag uint8 = iota
	SpendTag

	CoinbaseFlag uint32 = 0x01
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
	container := make([]byte, 0, 4+8+1+varLen+len(view.pkScript))

	w := bytes.NewBuffer(container)
	w.Write(utils.PutUint32(view.compactCoinbaseAndHeight()))
	w.Write(utils.PutUint64(uint64(view.amount)))
	if view.spent {
		w.Write(utils.PutUint8(SpendTag))
	} else {
		w.Write(utils.PutUint8(UnSpendTag))
	}
	err := wire.WriteVarInt(w, 0, uint64(varLen))
	if err != nil {
		return nil, err
	}
	w.Write(view.pkScript)

	return w.Bytes(), nil
}

func (view *UtxoView) decode(value []byte) error {
	coinbaseAndHeight := binary.LittleEndian.Uint32(value[0:4])
	amount := binary.LittleEndian.Uint64(value[4:12])

	// read lock script
	r := bytes.NewReader(value[13:])
	length, err := wire.ReadVarInt(r, 0)
	if err != nil {
		return err
	}

	view.isFromCoinbase = coinbaseAndHeight&CoinbaseFlag != 0
	view.height = coinbaseAndHeight >> 1
	view.amount = int64(amount)
	view.spent = value[12] == SpendTag
	view.pkScript = value[13+wire.VarIntSerializeSize(length):]

	return nil
}

func (view *UtxoView) compactCoinbaseAndHeight() uint32 {
	if view.isFromCoinbase {
		return view.height<<1 | CoinbaseFlag
	}

	return view.height << 1
}
