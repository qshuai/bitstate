package main

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

func shortHash(txHash *chainhash.Hash) []byte {
	if txHash == nil {
		return nil
	}

	return txHash[chainhash.HashSize-6:]
}
