package handler

import (
	"encoding/binary"

	"github.com/btcsuite/btcd/wire"
)

const (
	// AddressBalanceInfoEncodeSize represents the encoded size of struct
	// of AddressBalanceInfo. 8 bytes for `received` field, 8 bytes for
	// `send`, and `txes`,`unspentTxes`, `created`, `updated` will be allocate
	// 4 bytes respectively.
	AddressBalanceInfoEncodeSize = 8 + 8 + 4 + 4 + 4 + 4 // 32 bytes
)

// AddressBalanceInfo represents the balance and transaction
// state of the synced blockchain height.
type AddressBalanceInfo struct {
	received    int64  // received coins in Satoshi
	send        int64  // sent coins in Satoshi
	txes        uint32 // the number of transactions
	unspentTxes uint32 // the number of utxo
	created     uint32 // the time when the address appears on blockchain for the first time
	updated     uint32 // the time when the address transaction on blockchain recently

	bestTxHash string // short hash representing the tx hash
}

// Encode encodes an address info into 32 bytes.
func (info *AddressBalanceInfo) Encode() []byte {
	r := make([]byte, AddressBalanceInfoEncodeSize)
	binary.LittleEndian.PutUint64(r[0:8], uint64(info.received))
	binary.LittleEndian.PutUint64(r[8:16], uint64(info.send))
	binary.LittleEndian.PutUint32(r[16:20], info.txes)
	binary.LittleEndian.PutUint32(r[20:24], info.unspentTxes)
	binary.LittleEndian.PutUint32(r[24:28], info.created)
	binary.LittleEndian.PutUint32(r[28:32], info.updated)

	return r
}

// Decode decodes 32 bytes into a readable info.
func (info *AddressBalanceInfo) Decode(value []byte) {
	info.received = int64(binary.LittleEndian.Uint64(value[0:8]))
	info.send = int64(binary.LittleEndian.Uint64(value[8:16]))
	info.txes = binary.LittleEndian.Uint32(value[16:20])
	info.unspentTxes = binary.LittleEndian.Uint32(value[20:24])
	info.created = binary.LittleEndian.Uint32(value[24:28])
	info.updated = binary.LittleEndian.Uint32(value[28:32])
}

// receiveCoin receives an utxo.
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

// spendCoin spends an utxo.
func (info *AddressBalanceInfo) spendCoin(txHash string, blockTime uint32, value int64) {
	if info.bestTxHash != txHash {
		info.txes++
		info.bestTxHash = txHash
	}

	info.updated = blockTime
	info.send += value
	info.unspentTxes--
}
