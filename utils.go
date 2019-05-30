package main

import (
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
)

func shortHash(txHash *chainhash.Hash) []byte {
	if txHash == nil {
		return nil
	}

	return txHash[chainhash.HashSize-6:]
}

func isNullDataOutput(pkscript []byte) bool {
	if pkscript != nil && len(pkscript) > 0 && pkscript[0] == txscript.OP_RETURN {
		return true
	}

	return false
}
