package main

import (
	"encoding/binary"

	"github.com/btcsuite/btcd/wire"
)

type AddressBalanceInfo struct {
	received    int64
	send        int64
	txes        uint32
	unspentTxes uint32
	created     uint32
	updated     uint32

	bestTxHash string // short hash represent the transaction hash
}

func (info *AddressBalanceInfo) Encode() []byte {
	r := make([]byte, 32)
	binary.LittleEndian.PutUint64(r[0:8], uint64(info.received))
	binary.LittleEndian.PutUint64(r[8:16], uint64(info.send))
	binary.LittleEndian.PutUint32(r[16:20], info.txes)
	binary.LittleEndian.PutUint32(r[20:24], info.unspentTxes)
	binary.LittleEndian.PutUint32(r[24:28], info.created)
	binary.LittleEndian.PutUint32(r[28:32], info.updated)

	return r
}

func (info *AddressBalanceInfo) Decode(value []byte) {
	info.received = int64(binary.LittleEndian.Uint64(value[0:8]))
	info.send = int64(binary.LittleEndian.Uint64(value[8:16]))
	info.txes = binary.LittleEndian.Uint32(value[16:20])
	info.unspentTxes = binary.LittleEndian.Uint32(value[20:24])
	info.created = binary.LittleEndian.Uint32(value[24:28])
	info.updated = binary.LittleEndian.Uint32(value[28:32])
}

func (info *AddressBalanceInfo) receiveCoin(txHash string, blockTime uint32,
	out *wire.TxOut, isFirst bool) {

	if info.bestTxHash != txHash {
		info.txes++
		info.bestTxHash = txHash
	}

	if isFirst {
		info.created = blockTime
	}

	info.received += out.Value
	info.unspentTxes++
	info.updated = blockTime
}

func (info *AddressBalanceInfo) spendCoin(txHash string, blockTime uint32, value int64) {
	if info.bestTxHash != txHash {
		info.txes++
		info.bestTxHash = txHash
	}

	info.updated = blockTime
	info.send += value
	info.unspentTxes--
}
