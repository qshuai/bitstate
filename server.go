package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/qshuai/bitstate/database"
	"github.com/qshuai/bitstate/database/bboltdb"
	"github.com/qshuai/bitstate/database/leveldb"
	"github.com/qshuai/go-bitcoind"
	"github.com/qshuai/lru"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"go.etcd.io/bbolt"
)

var (
	addressBucket     = []byte("a")
	utxoBucket        = []byte("u")
	addressListBucket = []byte("l")
	bestHeightKey     = []byte("bestheight")

	dummyScript = []byte{0, 0, 0, 0}

	// the following items are trace indicator
	utxoReadCount             = 0
	utxoRead          float64 = 0
	utxoWriteCount            = 0
	utxoWrite         float64 = 0
	utxoDelCount              = 0
	utxoDel           float64 = 0
	addressReadCount          = 0
	addressRead       float64 = 0
	addressWriteCount         = 0
	addressWrite      float64 = 0
)

var addressMapping = make(map[string]struct{}, 5000000)

type Server struct {
	bestHeight int

	// db stores utxo and address info
	db database.DB

	// addressDB stores addresses on blockchain and the relationship to shortened hash,
	// and this database is responsible for reading address, bboltdb recommended.
	addressDB database.DB

	rpcBackend     *bitcoind.Bitcoind
	blockContainer chan *Block

	// lru cache
	utxoCache    *lru.Cache
	addressCache *lru.Cache

	// block header cache
	mtx     sync.Mutex
	headers map[uint32]*bitcoind.BlockHeader

	startBlock int
	endBlock   int

	closed atomic.Value
	done   chan bool

	wg sync.WaitGroup
}

func (s *Server) syncBlocks() {
	defer func() {
		// the channel should be closed by producer
		close(s.blockContainer)

		log.Debug("Sync block goroutine exit")
	}()

	// sync blockHeader
	if s.startBlock != 0 {
		for i := 0; i < s.startBlock; i++ {
			blockHeader, err := s.getBlockHeader(i)
			if err != nil {
				return
			}

			s.mtx.Lock()
			s.headers[uint32(i)] = blockHeader
			s.mtx.Unlock()
		}
	}

	for i := s.startBlock; i <= s.endBlock; i++ {
		if s.closed.Load().(bool) {
			log.Info("[block sync]Receiving shutdown requested")
			s.done <- true
			return
		}

		blockheader, err := s.getBlockHeader(i)
		if err != nil {
			return
		}

		s.mtx.Lock()
		s.headers[uint32(i)] = blockheader
		s.mtx.Unlock()

		rawBlock, err := s.rpcBackend.GetRawBlock(blockheader.Hash)
		if err != nil {
			log.Errorf("get block failed: %s", err)
			return
		}
		var block wire.MsgBlock
		blockBytes, err := hex.DecodeString(rawBlock)
		if err != nil {
			log.Errorf("decode block failed: %s\n", err)
			return
		}
		err = block.Deserialize(bytes.NewReader(blockBytes))
		if err != nil {
			log.Errorf("deserialize block failed: %s\n", err)
			return
		}

		s.blockContainer <- &Block{
			block:  &block,
			height: uint32(i),
		}

		log.Debugf("Have synced block: %s:%d", block.BlockHash(), i)
	}
}

func (s *Server) start() {
	// the goroutine be responsible for get block and put to cache
	go s.syncBlocks()

	var inputs, outputs int
	for block := range s.blockContainer {
		select {
		case <-s.done:
			return
		default:
		}

		inputs, outputs = 0, 0

		utxoReadCount, utxoWriteCount, utxoDelCount = 0, 0, 0
		utxoRead, utxoWrite, utxoDel = 0, 0, 0
		addressReadCount, addressWriteCount = 0, 0
		addressRead, addressWrite = 0, 0

		for _, tx := range block.block.Transactions {
			inputs += len(tx.TxIn)
			outputs += len(tx.TxOut)

			payment, viewCache, err := s.fetchPayment(tx)
			if err != nil {
				log.Errorf("Fetch payment information failed: %s", err)
				return
			}
			txHash := tx.TxHash()

			// handle spend coin
			for idx := 0; idx < len(tx.TxIn); idx++ {
				// coinbase transaction input does not spent any utxo and impact any address balance.
				// so skip the coinbase transaction input.
				if blockchain.IsCoinBaseTx(tx) {
					break
				}

				err := s.spend(&txHash, viewCache[idx], payment)
				if err != nil {
					log.Errorf("spend coin failed, input: %s:%d, reason: %s", txHash.String(), idx, err)
					return
				}
			}

			for idx, output := range tx.TxOut {
				if isNullDataOutput(output.PkScript) {
					continue
				}

				view := &UtxoView{
					isFromCoinbase: blockchain.IsCoinBaseTx(tx),
					height:         block.height,
					amount:         output.Value,
					spent:          false,
					pkScript:       output.PkScript,
				}
				key := hex.EncodeToString(generateUtxoKey(&txHash, uint32(idx)))
				err := s.utxoCache.Add(&UtxoViewCache{
					key:   key,
					entry: view,
				})
				if err != nil {
					log.Error("Add utxo entry to cache failed: ", err)
					return
				}

				log.Debugf("Save utxo entry to cache: %s", key)
				err = s.receive(&txHash, view, payment)
				if err != nil {
					log.Errorf("receiving coin failed, output: %s:%d, reason: %s", txHash.String(), idx, err)
					return
				}
			}
		}

		s.bestHeight = int(block.height)

		log.Infof("Handle block %s:%d, inputs: %d, outputs: %d, utxo read: %f(%d), utxo write: %f(%d), utxo delete: %f(%d), "+
			"address read: %f(%d) address write: %f(%d)", block.block.BlockHash().String(), block.height, inputs, outputs, utxoRead,
			utxoReadCount, utxoWrite, utxoWriteCount, utxoDel, utxoDelCount, addressRead, addressReadCount, addressWrite, addressWriteCount)
	}
}

func (s *Server) getBlockHeader(blockHeight int) (*bitcoind.BlockHeader, error) {
	// todo<qshuai> fetch headers by local file firstly

	blockHash, err := s.rpcBackend.GetBlockHash(uint64(blockHeight))
	if err != nil {
		log.Errorf("get block hash failed: %s", err)
		return nil, err
	}

	// for block header cache
	blockheader, err := s.rpcBackend.GetBlockheader(blockHash)
	if err != nil {
		log.Errorf("get block header failed: %s", err)
		return nil, err
	}

	return blockheader, nil
}

// FetchPayment aims to get incoming and outgoings for a single bitcoin address.
// It must handle the situation a same address in input and output and a same
// address in input or output at the same time.
// The amount will be a positive number if an address receiving some coin. On
// the contrary, the amount will be a negative number if an address spending some
// coin.
func (s *Server) fetchPayment(tx *wire.MsgTx) (map[string]int64, []*UtxoView, error) {
	payment := make(map[string]int64)
	viewCache := make([]*UtxoView, len(tx.TxIn))

	// as to transaction input, we should fetch utxo information firstly
	for idx, input := range tx.TxIn {
		if blockchain.IsCoinBaseTx(tx) {
			break
		}

		view, err := s.fetchUtxo(input)
		if err != nil {
			return nil, nil, errors.New("Fetch utxo failed: " + err.Error())
		}

		// cache fetched utxoview
		viewCache[idx] = view

		scripts, err := generateCanonicalScript(getSafeScript(view.pkScript))
		if err != nil {
			return nil, nil, err
		}

		// hexadecimal encoding as key
		for _, script := range scripts {
			key := hex.EncodeToString(script)

			// spend coin
			if amount, ok := payment[key]; ok {
				payment[key] = amount - view.amount
			} else {
				payment[key] = view.amount
			}
		}
	}

	for _, output := range tx.TxOut {
		scripts, err := generateCanonicalScript(getSafeScript(output.PkScript))
		if err != nil {
			return nil, nil, err
		}

		// hexadecimal encoding as key
		for _, script := range scripts {
			key := hex.EncodeToString(script)

			// spend coin
			if amount, ok := payment[key]; ok {
				payment[key] = amount + output.Value
			} else {
				payment[key] = output.Value
			}
		}
	}

	return payment, viewCache, nil
}

func (s *Server) fetchUtxo(input *wire.TxIn) (*UtxoView, error) {
	outpoint := input.PreviousOutPoint

	utxoDBKey := generateUtxoKey(&outpoint.Hash, outpoint.Index)
	utxoMapKey := hex.EncodeToString(utxoDBKey)
	cacheEntry := s.utxoCache.Get(utxoMapKey)
	if cacheEntry != nil {
		view := cacheEntry.(*UtxoViewCache)
		s.utxoCache.Remove(utxoMapKey)

		return view.entry, nil
	} else {
		start := time.Now()
		utxoReadCount++
		value, err := s.db.Get(utxoBucket, utxoDBKey)
		if err != nil {
			if err == database.ErrNotFound {
				return nil, errors.New(fmt.Sprintf("utxo entry not found in database: %s:%d(%s)",
					outpoint.Hash.String(), outpoint.Index, hex.EncodeToString(utxoDBKey)))
			}

			return nil, err
		}

		utxoRead += time.Now().Sub(start).Seconds()

		// return the utxo result to caller
		var entry UtxoView
		err = entry.decode(value)
		if err != nil {
			return nil, err
		}

		delStart := time.Now()
		utxoDelCount++
		err = s.db.Remove(utxoBucket, utxoDBKey)
		if err != nil {
			return nil, err
		}
		utxoDel += time.Now().Sub(delStart).Seconds()

		return &entry, nil
	}
}

func (s *Server) carryUtxoCache(entry lru.Item) error {
	view := entry.(*UtxoViewCache)
	key, _ := hex.DecodeString(view.key)
	value, err := view.entry.encode()
	if err != nil {
		log.Error("Encode utxoview failed:", err)
		return err
	}
	utxoWriteCount++
	start := time.Now()
	err = s.db.Put(utxoBucket, key, value)
	if err != nil {
		return err
	}
	utxoWrite += time.Now().Sub(start).Seconds()

	return nil
}

func (s *Server) carryAddressCache(entry lru.Item) error {
	info := entry.(*AddressBalanceInfoCache)
	key, _ := hex.DecodeString(info.key)
	value := info.entry.Encode()
	addressWriteCount++
	start := time.Now()
	err := s.db.Put(addressBucket, key, value)
	if err != nil {
		return err
	}
	addressWrite += time.Now().Sub(start).Seconds()

	return nil
}

func (s *Server) flushUtxoLRUCache(item lru.Item) error {
	view := item.(*UtxoViewCache)
	key, _ := hex.DecodeString(view.key)
	value, err := view.entry.encode()
	if err != nil {
		return err
	}
	err = s.db.Put(utxoBucket, key, value)

	return err
}

func (s *Server) flushAddressLRUCache(item lru.Item) error {
	info := item.(*AddressBalanceInfoCache)
	key, _ := hex.DecodeString(info.key)
	value := info.entry.Encode()

	return s.db.Put(addressBucket, key, value)
}

func (s *Server) flushCache() error {
	err := s.utxoCache.Iterate()
	if err != nil {
		return err
	}

	return s.addressCache.Iterate()
}

func (s *Server) spend(txHash *chainhash.Hash, view *UtxoView, payment map[string]int64) error {
	scripts, err := generateCanonicalScript(getSafeScript(view.pkScript))
	if err != nil {
		return err
	}

	for _, script := range scripts {
		shortenedKey, cacheKey, _, err := s.generateAddressKey(script)
		if err != nil {
			return err
		}

		// get payment information
		amountDiff, ok := payment[hex.EncodeToString(script)]
		if !ok {
			return errors.New("payment information absent for address: " +
				hex.EncodeToString(getSafeScript(view.pkScript)) + ":" +
				hex.EncodeToString(script))
		}

		infoCache := s.addressCache.Get(cacheKey)
		if infoCache != nil {
			info := infoCache.(*AddressBalanceInfoCache).entry

			// update info
			err := s.spendCoin(txHash, view, amountDiff, info)
			if err != nil {
				return err
			}
		} else {
			// search from backend database
			addressReadCount++
			start := time.Now()
			value, err := s.db.Get(addressBucket, shortenedKey)
			if err != nil {
				if err == database.ErrNotFound {
					return errors.New("address entry not found: " + cacheKey)
				}

				return err
			}
			addressRead += time.Now().Sub(start).Seconds()

			var info AddressBalanceInfo
			info.Decode(value)

			err = s.spendCoin(txHash, view, amountDiff, &info)
			if err != nil {
				return err
			}

			err = s.addressCache.Add(&AddressBalanceInfoCache{
				key:   cacheKey,
				entry: &info,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) spendCoin(txHash *chainhash.Hash, view *UtxoView, amountDiff int64, info *AddressBalanceInfo) error {
	if info.bestTxHash != txHash.String() {
		// update tx count and send/receive fields when meeting a different transaction
		info.txes++
		if amountDiff < 0 {
			info.send -= amountDiff
		}

		info.bestTxHash = txHash.String()
	}

	s.mtx.Lock()
	info.updated = uint32(s.headers[view.height].Time)
	s.mtx.Unlock()
	info.unspentTxes--

	return nil
}

func (s *Server) receive(txHash *chainhash.Hash, view *UtxoView, payment map[string]int64) error {
	scripts, err := generateCanonicalScript(getSafeScript(view.pkScript))
	if err != nil {
		return err
	}

	for _, script := range scripts {
		shortenedKey, cacheKey, existed, err := s.generateAddressKey(script)
		if err != nil {
			return err
		}

		// get payment information
		amountDiff, ok := payment[hex.EncodeToString(script)]
		if !ok {
			return errors.New("payment information absent for address: " +
				hex.EncodeToString(getSafeScript(view.pkScript)) + ":" +
				hex.EncodeToString(script))
		}

		infoCache := s.addressCache.Get(cacheKey)
		if infoCache != nil {
			info := infoCache.(*AddressBalanceInfoCache).entry

			// update info
			err := s.receiveCoin(txHash, view, amountDiff, info)
			if err != nil {
				return err
			}
		} else {

			// search from backend database
			var value []byte
			if existed {
				addressReadCount++
				start := time.Now()
				value, err = s.db.Get(addressBucket, shortenedKey)
				if err != nil {
					// handle ErrNotFound specially
					if err == database.ErrNotFound {
						value = nil
					} else {
						return err
					}
				}
				addressRead += time.Now().Sub(start).Seconds()
			}

			if value != nil {
				var info AddressBalanceInfo
				info.Decode(value)

				err = s.receiveCoin(txHash, view, amountDiff, &info)
				if err != nil {
					return err
				}

				err = s.addressCache.Add(&AddressBalanceInfoCache{
					key:   cacheKey,
					entry: &info,
				})
				if err != nil {
					return err
				}
			} else {
				// Now turn out the address is a new one
				s.mtx.Lock()
				err = s.addressCache.Add(&AddressBalanceInfoCache{
					key: cacheKey,
					entry: &AddressBalanceInfo{
						received:    view.amount,
						txes:        1,
						unspentTxes: 1,
						created:     uint32(s.headers[view.height].Time),
						updated:     uint32(s.headers[view.height].Time),
						bestTxHash:  txHash.String(),
					},
				})
				s.mtx.Unlock()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (s *Server) receiveCoin(txHash *chainhash.Hash, view *UtxoView, amountDiff int64, info *AddressBalanceInfo) error {
	if info.bestTxHash != txHash.String() {
		info.txes++
		// when a address receiving the first coin, the create field should be maintained by method caller.
		if amountDiff > 0 {
			info.received += amountDiff
		}

		info.bestTxHash = txHash.String()
	}

	s.mtx.Lock()
	info.updated = uint32(s.headers[view.height].Time)
	s.mtx.Unlock()
	info.unspentTxes++

	return nil
}

func (s *Server) stop() {
	s.closed.Store(true)

	log.Info("Flush cache before exiting")
	err := s.flushCache()
	if err != nil {
		log.Errorf("flush utxo and address cache failed: %s", err)
	}

	log.Infof("write the best block height: %d", s.bestHeight)
	err = s.db.Put(nil, bestHeightKey, []byte(strconv.Itoa(s.bestHeight)))
	if err != nil {
		log.Errorf("write the best block height failed: %s", err)
	}

	log.Info("Flush cache completed!")

	err = s.db.Shutdown()
	if err != nil {
		log.Errorf("close database failed: %s", err)
	}

	s.wg.Done()
}

func generateUtxoKey(txHash *chainhash.Hash, idx uint32) []byte {
	shash := shortHash(txHash)
	index := CompressedUint32(idx)

	buf := bytes.NewBuffer(make([]byte, 0, len(shash)+len(index)))
	buf.Write(shash)
	buf.Write(index)

	return buf.Bytes()
}

func (s *Server) generateAddressKey(script []byte) ([]byte, string, bool, error) {
	if len(script) <= 0 {
		// todo confirm true
		return dummyScript, hex.EncodeToString(getSafeScript(script)), true, nil
	}

	// shorten hash
	sum := sha256.Sum256(script)
	target := sum[:6]
	//_, err := s.addressDB.Get(addressListBucket, target)
	//existed := true
	//if err != nil {
	//	if err == database.ErrNotFound {
	//		// store
	//		err = s.addressDB.Put(addressListBucket, target, script)
	//		if err != nil {
	//			return nil, "", false, err
	//		}
	//		existed = false
	//	} else {
	//		return nil, "", false, err
	//	}
	//}
	key := hex.EncodeToString(target)
	_, ok := addressMapping[key]
	existed := true
	if !ok {
		existed = false
		addressMapping[key] = struct{}{}
	}

	return target, key, existed, nil
}

func getSafeScript(script []byte) []byte {
	if script == nil || len(script) == 0 {
		return dummyScript
	}

	return script
}

func newAddressListDB() (database.DB, error) {
	dbFile := viper.GetString("server.db.bbolt.dbpath")
	bboltDB, err := bboltdb.New(dbFile, &bbolt.Options{
		Timeout:      0,
		NoGrowSync:   false,
		FreelistType: bbolt.FreelistArrayType,
	})
	if err != nil {
		return nil, err
	}

	// initialize necessary bucket
	err = bboltDB.InitBucket(addressListBucket)
	if err != nil {
		log.Errorf("Create necessary db bucket failed: %s", err)
		return nil, err
	}

	return bboltDB, nil
}

func NewServer() (*Server, error) {
	driverName := viper.GetString("server.db.driver")
	driver, ok := database.GetDriver(driverName)
	if !ok {
		return nil, errors.New("database driver not exited")
	}

	var db database.DB
	switch driver {
	case database.BboltDriver:
		dbFile := viper.GetString("server.db.bbolt.dbpath")
		bboltDB, err := bboltdb.New(dbFile, &bbolt.Options{
			Timeout:      0,
			NoGrowSync:   false,
			FreelistType: bbolt.FreelistArrayType,
		})
		if err != nil {
			return nil, err
		}

		// initialize necessary bucket
		err = bboltDB.InitBucket(utxoBucket, addressBucket)
		if err != nil {
			log.Errorf("Create necessary db bucket failed: %s", err)
			return nil, err
		}

		db = bboltDB

	case database.LeveldbDriver:
		dbPath := viper.GetString("server.db.leveldb.dbpath")
		levelDB, err := leveldb.New(dbPath, &opt.Options{})
		if err != nil {
			return nil, err
		}

		db = levelDB
	default:
		return nil, errors.New("invalid database driver")
	}

	addressListDB, err := newAddressListDB()
	if err != nil {
		return nil, err
	}

	bc, err := bitcoind.New(
		viper.GetString("bitcoin.rpc.host"),
		viper.GetInt("bitcoin.rpc.port"),
		viper.GetString("bitcoin.rpc.user"),
		viper.GetString("bitcoin.rpc.password"),
		false)
	if err != nil {
		return nil, err
	}
	_, err = bc.GetBestBlockhash()
	if err != nil {
		return nil, errors.New("the tcp connection to bitcoin core unreached: " + err.Error())
	}

	// todo<qshuai> set defalult value
	utxoCacheSize := viper.GetInt("server.cache.utxo")
	addressCacheSize := viper.GetInt("server.cache.address")

	server := &Server{
		db:             db,
		addressDB:      addressListDB,
		rpcBackend:     bc,
		blockContainer: make(chan *Block, blockCacheSize),
		utxoCache:      lru.New(utxoCacheSize, false),
		addressCache:   lru.New(addressCacheSize, true),

		endBlock: viper.GetInt("server.task.end"),

		done: make(chan bool, 1),
		wg:   sync.WaitGroup{},
	}
	server.closed.Store(false)
	// exit before flush all cache
	server.wg.Add(1)

	server.utxoCache.Callback = server.carryUtxoCache
	server.addressCache.Callback = server.carryAddressCache

	server.utxoCache.ForEach = server.flushUtxoLRUCache
	server.addressCache.ForEach = server.flushAddressLRUCache

	server.headers = make(map[uint32]*bitcoind.BlockHeader, server.endBlock)

	// recover the last synced height if existed
	value, err := server.db.Get(nil, bestHeightKey)
	if err != nil {
		if err == database.ErrNotFound {
			server.startBlock = 0
		} else {
			return nil, errors.New("fetch the best synced block height failed: " + err.Error())
		}
	}
	if value != nil {
		bestHeight, err := strconv.Atoi(string(value))
		if err != nil {
			return nil, errors.New("invalid number(block height)")
		}
		server.startBlock = bestHeight + 1
	}
	log.Infof("bitstate will sync from block height: %d", server.startBlock)

	// clean existed address info data if start block height is 0
	if server.startBlock == 0 {
		err = server.db.Clean(addressBucket)
		if err != nil {
			return nil, err
		}
	}

	return server, nil
}
