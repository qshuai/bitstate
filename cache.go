package main

type UtxoViewCache struct {
	// compressed hexadecimal string of the short hash and index
	key string

	entry *UtxoView
}

func (uc *UtxoViewCache) GetKey() interface{} {
	return uc.key
}

type AddressBalanceInfoCache struct {
	// hexadecimal string for script bytes
	key string

	entry *AddressBalanceInfo
}

func (ac *AddressBalanceInfoCache) GetKey() interface{} {
	return ac.key
}
