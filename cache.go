package main

type UtxoViewCache struct {
	key string

	entry *UtxoView
}

func (uc *UtxoViewCache) GetKey() interface{} {
	return uc.key
}

type AddressBalanceInfoCache struct {
	key string

	entry *AddressBalanceInfo
}

func (ac *AddressBalanceInfoCache) GetKey() interface{} {
	return ac.key
}
