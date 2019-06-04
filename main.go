package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
	"github.com/qshuai/lru"
	"github.com/spf13/viper"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/toorop/go-bitcoind"
)

const (
	logFile = "bitstate.log"

	blockCacheSize = 5

	flushSize = 20000
)

var (
	log btclog.Logger

	// cache size default value
	utxoCacheSize    = 200000
	addressCacheSize = 300000
	// subcache size default value
	utxoSubcacheSize    = 10000
	addressSubcacheSize = 20000

	// debug
	utxoReadCount     int     = 0
	utxoRead          float64 = 0
	utxoWriteCount            = 0
	utxoWrite         float64 = 0
	utxoDelCount              = 0
	utxoDel           float64 = 0
	addressReadCount  int     = 0
	addressRead       float64 = 0
	addressWriteCount int     = 0
	addressWrite      float64 = 0

	dummyScript = []byte{0, 0, 0, 0}

	readOpt = &opt.ReadOptions{DontFillCache: true}
	wBatch  = leveldb.Batch{}

	utxoKeyPrefix    = []byte{0}
	addressKeyPrefix = []byte{1}
)

type Block struct {
	block  *wire.MsgBlock
	height uint32
}

type Server struct {
	db             *leveldb.DB
	rpcBackend     *bitcoind.Bitcoind
	blockContainer chan *Block

	// cache
	utxoCache    *lru.Cache
	addressCache *lru.Cache

	// subcache, received from lru
	utxoSubcache    map[string]*UtxoView
	addressSubcache map[string]*AddressBalanceInfo

	// block header cache
	mtx     sync.Mutex
	headers map[uint32]*bitcoind.BlockHeader

	startBlock int
	endBlock   int

	closed atomic.Value
	done   chan bool

	interrupt chan os.Signal
}

func (s *Server) SyncBlocks() {
	defer func() {
		// the channel should be closed by producer
		close(s.blockContainer)

		log.Debug("Sync block goroutine exit")
	}()

	for i := s.startBlock; i <= s.endBlock; i++ {
		if s.closed.Load().(bool) {
			log.Info("[block sync]Receiving shutdown requested")
			s.done <- true
			return
		}

		blockHash, err := s.rpcBackend.GetBlockHash(uint64(i))
		if err != nil {
			log.Errorf("get block hash failed: %s", err)
			return
		}

		// for block header cache
		blockheader, err := s.rpcBackend.GetBlockheader(blockHash)
		if err != nil {
			log.Errorf("get block header failed: %s", err)
			return
		}
		s.mtx.Lock()
		s.headers[uint32(i)] = blockheader
		s.mtx.Unlock()

		rawBlock, err := s.rpcBackend.GetRawBlock(blockHash)
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

		if err != nil {
			log.Errorf("get block failed: %s\n", err)
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
	for i := 0; i < s.startBlock; i++ {
		blockHash, err := s.rpcBackend.GetBlockHash(uint64(i))
		if err != nil {
			log.Error("get block hash failed: %s", err)
			return
		}

		blockHeader, err := s.rpcBackend.GetBlockheader(blockHash)
		if err != nil {
			log.Error("get block header failed: %s", err)
			return
		}

		s.headers[uint32(i)] = blockHeader
	}

	// the goroutine be responsible for get block and put to cache
	go s.SyncBlocks()

	var inputs, outputs int
	for block := range s.blockContainer {
		inputs, outputs = 0, 0

		utxoReadCount, utxoWriteCount, utxoDelCount = 0, 0, 0
		utxoRead, utxoWrite, utxoDel = 0, 0, 0
		addressReadCount, addressWriteCount = 0, 0
		addressRead, addressWrite = 0, 0

		select {
		case <-s.interrupt:
			log.Info("Receiving interrupt signal, preparing exit program")
			// notify block sync goroutine should exit
			s.closed.Store(true)

			// fetch a entry from block container channel avoiding to block SyncBlocks goroutine
			<-s.blockContainer

			// wait sync block goroutine exit complete
			<-s.done

			goto exit
		default:
		}

		for _, tx := range block.block.Transactions {
			inputs += len(tx.TxIn)
			outputs += len(tx.TxOut)

			payment, viewCache, err := s.FetchPayment(tx)
			if err != nil {
				log.Errorf("Fetch payment information failed: %s", err)
				return
			}
			txHash := tx.TxHash()
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
				err := s.utxoCache.Add(&UtxoViewCache{
					key:   hex.EncodeToString(generateUtxoKey(&txHash, uint32(idx))),
					entry: view,
				})
				if err != nil {
					log.Error("Add utxo entry to cache failed:", err)
					return
				}

				log.Debugf("Save utxo entry to primary cache: %s",
					hex.EncodeToString(generateUtxoKey(&txHash, uint32(idx))))

				err = s.receive(&txHash, view, payment)
				if err != nil {
					log.Errorf("receiving coin failed, output: %s:%d, reason: %s", txHash.String(), idx, err)
					return
				}
			}
		}

		log.Infof("Handle block %s:%d, inputs: %d, outputs: %d, utxo read: %f(%d), utxo write: %f(%d), utxo delete: %f(%d), "+
			"address read: %f(%d) address write: %f(%d)", block.block.BlockHash().String(), block.height, inputs, outputs, utxoRead,
			utxoReadCount, utxoWrite, utxoWriteCount, utxoDel, utxoDelCount, addressRead, addressReadCount, addressWrite, addressWriteCount)
	}

exit:
}

// FetchPayment aims to get incoming and outgoings for a single bitcoin address.
// It must handle the situation a same address in input and output and a same
// address in input or output at the same time.
// The amount will be a positive number if an address receiving some coin. On
// the contrary, the amount will be a negative number if an address spending some
// coin.
func (s *Server) FetchPayment(tx *wire.MsgTx) (map[string]int64, []*UtxoView, error) {
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
	// find utxo in primary cache
	if cacheEntry != nil {
		view := cacheEntry.(*UtxoViewCache)
		s.utxoCache.Remove(utxoMapKey)

		return view.entry, nil
	} else {
		// find utxo in secondary cache
		entry := s.utxoSubcache[utxoMapKey]
		if entry != nil {
			delete(s.utxoSubcache, utxoMapKey)

			return entry, nil
		} else {
			utxoReadCount++
			start := time.Now()
			value, err := s.db.Get(utxoDBKey, readOpt)
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, errors.New(fmt.Sprintf("utxo entry not found in database: %s:%d(%s)",
					outpoint.Hash.String(), outpoint.Index, hex.EncodeToString(utxoDBKey)))
			}

			utxoRead += time.Now().Sub(start).Seconds()

			// return the utxo result to caller
			var entry UtxoView
			err = entry.decode(value)
			if err != nil {
				return nil, err
			}

			utxoDelCount++
			delStart := time.Now()
			err = s.db.Delete(utxoDBKey, nil)
			if err != nil {
				return nil, err
			}
			utxoDel += time.Now().Sub(delStart).Seconds()

			return &entry, nil
		}
	}
}

func (s *Server) shutdown() {
	log.Info("Flush cache before exiting")
	err := s.flushCache()
	if err != nil {
		log.Errorf("flush utxo and address cache failed: %s", err)
	}
	log.Info("Flush cache completed!")

	err = s.db.Close()
	if err != nil {
		log.Errorf("close bbolt database failed: %s", err)
	}
}

func (s *Server) carryUtxoSubcache(entry lru.Item) error {
	view := entry.(*UtxoViewCache)
	s.utxoSubcache[view.key] = view.entry
	log.Debugf("Save utxo entry to secondary cache: %s", view.key)

	// check whether trigger full cache
	if len(s.utxoSubcache) >= utxoSubcacheSize {
		start := time.Now()
		err := s.flushUtxoSubcache()
		if err != nil {
			return err
		}
		utxoWrite += time.Now().Sub(start).Seconds()
	}

	return nil
}

func (s *Server) flushUtxoSubcache() error {
	for hashAndIndex, view := range s.utxoSubcache {
		log.Debugf("Save utxo entry to db: %s", hashAndIndex)
		key, _ := hex.DecodeString(hashAndIndex)
		value, err := view.encode()
		if err != nil {
			return err
		}
		utxoWriteCount++
		wBatch.Put(key, value)
	}

	start := time.Now()
	if err := s.db.Write(&wBatch, nil); err != nil {
		return err
	}
	utxoWrite += time.Now().Sub(start).Seconds()
	wBatch.Reset()
	s.utxoSubcache = make(map[string]*UtxoView, utxoSubcacheSize)

	return nil
}

func (s *Server) carryAddressSubcache(entry lru.Item) error {
	view := entry.(*AddressBalanceInfoCache)
	s.addressSubcache[view.key] = view.entry

	// check whether trigger full cache
	if len(s.addressSubcache) >= addressSubcacheSize {
		err := s.flushAddressSubcache()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) flushAddressSubcache() error {
	for scriptHex, info := range s.addressSubcache {
		key, _ := hex.DecodeString(scriptHex)
		addressWriteCount++
		wBatch.Put(key, info.Encode())
	}

	start := time.Now()
	if err := s.db.Write(&wBatch, nil); err != nil {
		return err
	}
	addressWrite += time.Now().Sub(start).Seconds()
	wBatch.Reset()
	s.addressSubcache = make(map[string]*AddressBalanceInfo, addressSubcacheSize)

	return nil
}

func (s *Server) flushUtxoLRUCache(item lru.Item) error {
	view := item.(*UtxoViewCache)
	key, _ := hex.DecodeString(view.key)
	v, err := view.entry.encode()
	if err != nil {
		return err
	}
	wBatch.Put(key, v)
	if wBatch.Len() > flushSize {
		err = s.db.Write(&wBatch, nil)
		if err != nil {
			return err
		}

		wBatch = leveldb.Batch{}
	}

	return nil
}

func (s *Server) flushAddressLRUCache(item lru.Item) error {
	info := item.(*AddressBalanceInfoCache)
	key, _ := hex.DecodeString(info.key)
	wBatch.Put(key, info.entry.Encode())
	if wBatch.Len() > flushSize {
		err := s.db.Write(&wBatch, nil)
		if err != nil {
			return err
		}

		wBatch = leveldb.Batch{}
	}
	return nil
}

func (s *Server) flushCache() error {
	err := s.flushUtxoSubcache()
	if err != nil {
		return err
	}

	err = s.flushAddressSubcache()
	if err != nil {
		return err
	}

	err = s.utxoCache.Iterate()
	if err != nil {
		return err
	}
	if err := s.db.Write(&wBatch, nil); err != nil {
		return err
	}

	err = s.addressCache.Iterate()
	if err != nil {
		return err
	}

	return s.db.Write(&wBatch, nil)
}

func (s *Server) spend(txHash *chainhash.Hash, view *UtxoView, payment map[string]int64) error {
	scripts, err := generateCanonicalScript(getSafeScript(view.pkScript))
	if err != nil {
		return err
	}

	for _, script := range scripts {
		cacheKey := generateAddressKey(script)

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

			// optimize cache
			_, err = s.ReduceCache(cacheKey, info)
			if err != nil {
				return err
			}
		} else {
			// search in secondary cache
			info, ok := s.addressSubcache[cacheKey]
			if ok {
				err := s.spendCoin(txHash, view, amountDiff, info)
				if err != nil {
					return err
				}

				striped, err := s.ReduceCache(cacheKey, info)
				if err != nil {
					return err
				}

				// valuable entry
				if !striped {
					// move to primary cache from secondary cache
					delete(s.addressSubcache, cacheKey)

					err = s.addressCache.Add(&AddressBalanceInfoCache{
						key:   cacheKey,
						entry: info,
					})
					if err != nil {
						return err
					}
				}
			} else {
				addressReadCount++
				start := time.Now()
				value, err := s.db.Get(script, readOpt)
				addressRead += time.Now().Sub(start).Seconds()
				if err != nil {
					if err == leveldb.ErrNotFound {
						return errors.New("address entry not found in leveldb: " + hex.EncodeToString(script))
					}

					return err
				}

				var info AddressBalanceInfo
				info.Decode(value)

				err = s.spendCoin(txHash, view, amountDiff, &info)
				if err != nil {
					return err
				}

				striped, err := s.ReduceCache(cacheKey, &info)
				if err != nil {
					return err
				}

				if striped {
					// push back to secondary cache
					s.addressSubcache[cacheKey] = &info
				} else {
					// push back to primary cache
					err = s.addressCache.Add(&AddressBalanceInfoCache{
						key:   cacheKey,
						entry: &info,
					})
					if err != nil {
						return err
					}
				}
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
		cacheKey := generateAddressKey(script)

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
			// search in secondary cache
			info, ok := s.addressSubcache[cacheKey]
			if ok {
				err := s.receiveCoin(txHash, view, amountDiff, info)
				if err != nil {
					return err
				}

				// move to primary cache from secondary cache
				delete(s.addressSubcache, cacheKey)

				err = s.addressCache.Add(&AddressBalanceInfoCache{
					key:   cacheKey,
					entry: info,
				})
				if err != nil {
					return err
				}
			} else {
				var notFound bool
				// search from backend database
				addressReadCount++
				start := time.Now()
				value, err := s.db.Get(script, readOpt)
				addressRead += time.Now().Sub(start).Seconds()
				if err != nil {
					if err == leveldb.ErrNotFound {
						notFound = true
					} else {
						return err
					}
				}

				if !notFound {
					var info AddressBalanceInfo
					info.Decode(value)

					err = s.receiveCoin(txHash, view, amountDiff, &info)
					if err != nil {
						return err
					}

					// push back to primary cache
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

func (s *Server) ReduceCache(cacheKey string, info *AddressBalanceInfo) (bool, error) {
	if info.GetBalance() == 0 {
		s.addressCache.Remove(cacheKey)
		s.addressSubcache[cacheKey] = info

		// check whether trigger full cache
		if len(s.addressSubcache) >= addressSubcacheSize {
			err := s.flushAddressSubcache()
			if err != nil {
				return true, err
			}
		}

		return true, nil
	}

	return false, nil
}

func generateUtxoKey(txHash *chainhash.Hash, idx uint32) []byte {
	shash := shortHash(txHash)
	index := CompressedUint32(idx)

	buf := bytes.NewBuffer(make([]byte, 0, 1+len(shash)+len(index)))
	buf.Write(utxoKeyPrefix)
	buf.Write(shash)
	buf.Write(index)

	return buf.Bytes()
}

func generateAddressKey(script []byte) string {
	buf := bytes.NewBuffer(make([]byte, 0, 1+len(getSafeScript(script))))
	buf.Write(addressKeyPrefix)
	buf.Write(getSafeScript(script))

	return hex.EncodeToString(buf.Bytes())
}

func getSafeScript(script []byte) []byte {
	if script == nil || len(script) == 0 {
		return dummyScript
	}

	return script
}

func main() {
	// read config file
	viper.SetConfigFile("config.yml")
	viper.SetConfigType("yml")
	viper.AddConfigPath("./")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Read config file:", err)
		return
	}

	// setup log
	logPath := viper.GetString("server.log.log-path")
	_, err = os.Stat(logPath)
	if err != nil {
		if !os.IsExist(err) {
			err = os.Mkdir(logPath, os.ModePerm)
			if err != nil {
				fmt.Println("Make logger directory failed:", err)
				return
			}
		} else {
			fmt.Println("Acquire logger path information failed:", err)
			return
		}
	}
	file, err := os.OpenFile(filepath.Join(logPath, logFile), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Open logger file failed:", err)
		return
	}
	log = btclog.NewBackend(file).Logger("")
	levelConf := viper.GetString("server.log.level")
	level, ok := btclog.LevelFromString(levelConf)
	if !ok {
		log.Warnf("Set log level failed, want: %s, but current log level is: %s", levelConf, level)
	}
	log.SetLevel(level)

	// get server instance
	server, err := NewServer()
	if err != nil {
		log.Error("New server instance failed:", err)
		return
	}

	server.start()

	// finally flush all cache and close database
	server.shutdown()
}

func NewServer() (*Server, error) {
	dbFile := viper.GetString("server.db.dbpath")
	db, err := leveldb.OpenFile(dbFile, &opt.Options{
		Filter:      filter.NewBloomFilter(10),
		Compression: opt.SnappyCompression,
	})
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

	utxoCacheSize = viper.GetInt("server.cache.utxo")
	addressCacheSize = viper.GetInt("server.cache.address")
	utxoSubcacheSize = viper.GetInt("server.cache.utxo-sub")
	addressSubcacheSize = viper.GetInt("server.cache.address-sub")

	s := &Server{
		db:              db,
		rpcBackend:      bc,
		blockContainer:  make(chan *Block, blockCacheSize),
		utxoCache:       lru.New(utxoCacheSize, false),
		addressCache:    lru.New(addressCacheSize, true),
		utxoSubcache:    make(map[string]*UtxoView, utxoSubcacheSize),
		addressSubcache: make(map[string]*AddressBalanceInfo, addressSubcacheSize),

		startBlock: viper.GetInt("server.task.start"),
		endBlock:   viper.GetInt("server.task.end"),

		done:      make(chan bool, blockCacheSize),
		interrupt: make(chan os.Signal, 1),
	}
	s.closed.Store(false)
	s.utxoCache.Callback = s.carryUtxoSubcache
	s.addressCache.Callback = s.carryAddressSubcache

	s.utxoCache.ForEach = s.flushUtxoLRUCache
	s.addressCache.ForEach = s.flushAddressLRUCache

	s.headers = make(map[uint32]*bitcoind.BlockHeader)

	signal.Notify(s.interrupt, syscall.SIGINT, syscall.SIGTERM)

	return s, nil
}
