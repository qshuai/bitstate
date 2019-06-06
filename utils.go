package main

import (
	"encoding/hex"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
	"github.com/pkg/errors"
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

func generateCanonicalScript(pkScript []byte) ([][]byte, error) {
	if pkScript == nil || len(pkScript) == 0 {
		return nil, errors.New("nil or empty lock script")
	}

	ret, err := parseScript(pkScript)
	if err != nil {
		// ignore nostandard script
		log.Warnf("parse script failed for script: %s, reason: %s",
			hex.EncodeToString(pkScript), err.Error())
		return [][]byte{pkScript}, nil
	}

	switch typeOfScript(ret) {
	case PubKeyTy:
		pk := ret[0].data
		ph, err := payToPubKeyHashScript(btcutil.Hash160(pk))
		if err != nil {
			return nil, err
		}

		return [][]byte{ph}, nil
	case MultiSigTy:
		l := len(ret)
		phes := make([][]byte, 0, l-2-1)

		entries := make(map[string]struct{})
		for _, pop := range ret[1 : l-2] {
			ph, err := payToPubKeyHashScript(btcutil.Hash160(pop.data))
			if err != nil {
				return nil, err
			}

			// strip duplicated script
			if _, ok := entries[hex.EncodeToString(ph)]; ok {
				continue
			}

			entries[hex.EncodeToString(ph)] = struct{}{}
			phes = append(phes, ph)
		}
	case PubKeyHashTy:
		ph, err := payToPubKeyHashScript(ret[2].data)
		if err != nil {
			return nil, err
		}

		return [][]byte{ph}, err
	case ScriptHashTy:
		ps, err := payToScriptHashScript(ret[1].data)
		if err != nil {
			return nil, err
		}

		return [][]byte{ps}, nil
	}

	return [][]byte{pkScript}, nil
}
