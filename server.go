package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/qshuai/lru"
	"github.com/spf13/viper"
	"github.com/toorop/go-bitcoind"
	"go.etcd.io/bbolt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Server struct {
	db             *bbolt.DB
	rpcBackend     *bitcoind.Bitcoind
	blockContainer chan *Block

	// cache
	utxoCache    *lru.Cache
	addressCache *lru.Cache

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

				log.Debugf("Save utxo entry to cache: %s",
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
		scripts, err := generateCanonicalScript(output.PkScript)
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
		var vCopy []byte
		start := time.Now()
		utxoReadCount++
		err := s.db.View(func(tx *bbolt.Tx) error {
			value := tx.Bucket(utxoBucket).Get(utxoDBKey)
			if value == nil {
				return errors.New(fmt.Sprintf("utxo entry not found in database: %s:%d(%s)",
					outpoint.Hash.String(), outpoint.Index, hex.EncodeToString(utxoDBKey)))
			}

			// copy the db entry value in this transaction
			vCopy = make([]byte, len(value))
			copy(vCopy, value)

			return nil
		})
		if err != nil {
			return nil, err
		}
		utxoRead += time.Now().Sub(start).Seconds()

		// return the utxo result to caller
		var entry UtxoView
		err = entry.decode(vCopy)
		if err != nil {
			return nil, err
		}

		delStart := time.Now()
		utxoDelCount++
		err = s.db.Update(func(tx *bbolt.Tx) error {
			return tx.Bucket(utxoBucket).Delete(utxoDBKey)
		})
		if err != nil {
			return nil, err
		}
		utxoDel += time.Now().Sub(delStart).Seconds()

		return &entry, nil
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

func (s *Server) carryUtxoCache(entry lru.Item) error {
	view := entry.(*UtxoViewCache)

	utxoWriteCount++
	start := time.Now()
	err := s.db.Update(func(tx *bbolt.Tx) error {
		key, _ := hex.DecodeString(view.key)
		value, err := view.entry.encode()
		if err != nil {
			log.Error("Encode utxoview failed:", err)
			return err
		}
		err = tx.Bucket(utxoBucket).Put(key, value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	utxoWrite += time.Now().Sub(start).Seconds()

	return nil
}

func (s *Server) carryAddressCache(entry lru.Item) error {
	info := entry.(*AddressBalanceInfoCache)

	addressWriteCount++
	start := time.Now()
	err := s.db.Update(func(tx *bbolt.Tx) error {
		key, _ := hex.DecodeString(info.key)
		value := info.entry.Encode()
		err := tx.Bucket(addressBucket).Put(key, value)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	addressWrite += time.Now().Sub(start).Seconds()

	return nil
}

func (s *Server) flushUtxoLRUCache(item lru.Item) error {
	view := item.(*UtxoViewCache)
	err := s.db.Update(func(tx *bbolt.Tx) error {
		key, _ := hex.DecodeString(view.key)
		v, err := view.entry.encode()
		if err != nil {
			return err
		}
		return tx.Bucket(utxoBucket).Put(key, v)
	})

	return err
}

func (s *Server) flushAddressLRUCache(item lru.Item) error {
	info := item.(*AddressBalanceInfoCache)
	err := s.db.Update(func(tx *bbolt.Tx) error {
		key, _ := hex.DecodeString(info.key)
		return tx.Bucket(addressBucket).Put(key, info.entry.Encode())
	})

	return err
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
		} else {
			// search from backend database
			var vCopy []byte
			addressReadCount++
			start := time.Now()
			err := s.db.View(func(tx *bbolt.Tx) error {
				value := tx.Bucket(addressBucket).Get(script)
				if value == nil {
					return errors.New("address entry not found: " + cacheKey)
				}

				vCopy = make([]byte, len(value))
				copy(vCopy, value)

				return nil
			})
			if err != nil {
				return err
			}
			addressRead += time.Now().Sub(start).Seconds()

			var info AddressBalanceInfo
			info.Decode(vCopy)

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

			// search from backend database
			var vCopy []byte
			addressReadCount++
			start := time.Now()
			err := s.db.View(func(tx *bbolt.Tx) error {
				value := tx.Bucket(addressBucket).Get(script)
				// receiving coin first time
				if value != nil {
					vCopy = make([]byte, len(value))
					copy(vCopy, value)
				}

				return nil
			})
			if err != nil {
				return err
			}
			addressRead += time.Now().Sub(start).Seconds()

			if vCopy != nil {
				var info AddressBalanceInfo
				info.Decode(vCopy)

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

func generateUtxoKey(txHash *chainhash.Hash, idx uint32) []byte {
	shash := shortHash(txHash)
	index := CompressedUint32(idx)

	buf := bytes.NewBuffer(make([]byte, 0, len(shash)+len(index)))
	buf.Write(shash)
	buf.Write(index)

	return buf.Bytes()
}

func generateAddressKey(script []byte) string {
	return hex.EncodeToString(getSafeScript(script))
}

func getSafeScript(script []byte) []byte {
	if script == nil || len(script) == 0 {
		return dummyScript
	}

	return script
}

func mkDirAndFile(filePath string) error {
	dir := filepath.Dir(filePath)
	_, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(dir, os.ModePerm)
			if err != nil {
				return err
			}
			_, err = os.Create(filePath)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func NewServer() (*Server, error) {
	dbFile := viper.GetString("server.db.dbpath")
	err := mkDirAndFile(dbFile)
	if err != nil {
		log.Error("Make db directory or touch db file failed:", err)
		return nil, err
	}
	db, err := bbolt.Open(viper.GetString("server.db.dbpath"), 0600,
		&bbolt.Options{
			Timeout:      0,
			NoGrowSync:   false,
			FreelistType: bbolt.FreelistArrayType,
		})
	if err != nil {
		return nil, err
	}

	// initialize necessary bucket
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(utxoBucket)
		if err != nil {
			return err
		}

		_, err = tx.CreateBucketIfNotExists(addressBucket)
		return err
	})
	if err != nil {
		log.Errorf("Create necessary db bucket failed: %s", err)
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

	s := &Server{
		db:             db,
		rpcBackend:     bc,
		blockContainer: make(chan *Block, blockCacheSize),
		utxoCache:      lru.New(utxoCacheSize, true),
		addressCache:   lru.New(addressCacheSize, false),

		startBlock: viper.GetInt("server.task.start"),
		endBlock:   viper.GetInt("server.task.end"),

		done:      make(chan bool, blockCacheSize),
		interrupt: make(chan os.Signal, 1),
	}
	s.closed.Store(false)
	s.utxoCache.Callback = s.carryUtxoCache
	s.addressCache.Callback = s.carryAddressCache

	s.utxoCache.ForEach = s.flushUtxoLRUCache
	s.addressCache.ForEach = s.flushAddressLRUCache

	s.headers = make(map[uint32]*bitcoind.BlockHeader)

	signal.Notify(s.interrupt, syscall.SIGINT, syscall.SIGTERM)

	return s, nil
}
