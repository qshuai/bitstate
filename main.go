package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/btcsuite/btcd/blockchain"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
	"github.com/qshuai/lru"
	"github.com/spf13/viper"
	"github.com/toorop/go-bitcoind"
	"go.etcd.io/bbolt"
)

const (
	logFile = "bitstate.log"

	blockCacheSize = 5

	// cache size
	utxoCacheSize    = 1200000
	addressCacheSize = 50000

	// subcache size
	utxoSubcacheSize    = 10000
	addressSubcacheSize = 200
)

var (
	log btclog.Logger

	addressBucket = []byte("address")
	utxoBucket    = []byte("utxo")

	utxoRead  float64 = 0
	utxoWrite float64 = 0
	utxoDel   float64 = 0
)

type Block struct {
	block  *wire.MsgBlock
	height uint32
}

type Server struct {
	db             *bbolt.DB
	rpcBackend     *bitcoind.Bitcoind
	blockContainer chan *Block

	// cache
	utxoCache    *lru.Cache
	addressCache *lru.Cache

	// subcache, received from lru
	utxoSubcache    map[string]*UtxoView
	addressSubcache map[string]*AddressBalanceInfo

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
		utxoRead, utxoWrite, utxoDel = 0, 0, 0

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

			txHash := tx.TxHash()
			for _, input := range tx.TxIn {
				// coinbase transaction input does not spent any utxo and impact any address balance.
				// so skip the coinbase transaction input.
				if blockchain.IsCoinBaseTx(tx) {
					break
				}

				utxoDBKey := generateUtxoKey(&input.PreviousOutPoint.Hash, input.PreviousOutPoint.Index)
				utxoMapKey := hex.EncodeToString(utxoDBKey)
				cacheEntry := s.utxoCache.Get(utxoMapKey)
				// find utxo in primary cache
				if cacheEntry != nil {
					view := cacheEntry.(*UtxoViewCache)
					s.utxoCache.Remove(utxoMapKey)

					spend(&txHash, view.entry)
				} else {
					// find utxo in secondary cache
					entry := s.utxoSubcache[utxoMapKey]
					if entry != nil {
						delete(s.utxoSubcache, utxoMapKey)

						spend(&txHash, entry)
					} else {
						start := time.Now()
						err := s.db.View(func(tx *bbolt.Tx) error {
							value := tx.Bucket(utxoBucket).Get(utxoDBKey)
							if value == nil {
								return errors.New(fmt.Sprintf("utxo entry not found in database: %s:%d(%s)",
									input.PreviousOutPoint.Hash.String(), input.PreviousOutPoint.Index, hex.EncodeToString(utxoDBKey)))
							}

							return nil
						})
						if err != nil {
							log.Error("Fetch utxo entry in database failed:", err)
							return
						}
						utxoRead += time.Now().Sub(start).Seconds()

						delStart := time.Now()
						err = s.db.Update(func(tx *bbolt.Tx) error {
							return tx.Bucket(utxoBucket).Delete(utxoDBKey)
						})
						if err != nil {
							log.Error("Remove the spent utxo entry failed:", err)
							return
						}
						utxoDel += time.Now().Sub(delStart).Seconds()
					}
				}
			}

			for idx, output := range tx.TxOut {
				if isNullDataOutput(output.PkScript) {
					continue
				}

				err := s.utxoCache.Add(&UtxoViewCache{
					key: hex.EncodeToString(generateUtxoKey(&txHash, uint32(idx))),
					entry: &UtxoView{
						isFromCoinbase: blockchain.IsCoinBaseTx(tx),
						height:         block.height,
						amount:         output.Value,
						spent:          false,
						pkScript:       output.PkScript,
					},
				})
				if err != nil {
					log.Error("Add utxo entry to cache failed:", err)
					return
				}

				log.Debugf("Save utxo entry to primary cache: %s",
					hex.EncodeToString(generateUtxoKey(&txHash, uint32(idx))))

				receive(&txHash, output)
			}
		}

		log.Infof("Handle block %s:%d, inputs: %d, outputs: %d, utxo read: %f, utxo write: %f, utxo delete: %f",
			block.block.BlockHash().String(), block.height, inputs, outputs, utxoRead, utxoWrite, utxoDel)
	}

exit:
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
		err := s.db.Update(func(tx *bbolt.Tx) error {
			key, _ := hex.DecodeString(hashAndIndex)
			value, err := view.encode()
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
	}

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
		err := s.db.Update(func(tx *bbolt.Tx) error {
			key, _ := hex.DecodeString(scriptHex)
			return tx.Bucket(addressBucket).Put(key, info.Encode())
		})
		if err != nil {
			return err
		}
	}

	s.addressSubcache = make(map[string]*AddressBalanceInfo, addressSubcacheSize)

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

	return s.addressCache.Iterate()
}

func spend(txHash *chainhash.Hash, view *UtxoView) {

}

func receive(txHash *chainhash.Hash, output *wire.TxOut) {

}

func generateUtxoKey(txHash *chainhash.Hash, idx uint32) []byte {
	shash := shortHash(txHash)
	index := CompressedUint32(idx)

	buf := bytes.NewBuffer(make([]byte, 0, len(shash)+len(index)))
	buf.Write(shash)
	buf.Write(index)

	return buf.Bytes()
}

func mkDirAndFile(filePath string) error {
	dir := filepath.Dir(filePath)
	//filename := filepath.Base(filePath)
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

	s := &Server{
		db:              db,
		rpcBackend:      bc,
		blockContainer:  make(chan *Block, blockCacheSize),
		utxoCache:       lru.New(utxoCacheSize, true),
		addressCache:    lru.New(addressCacheSize, false),
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

	signal.Notify(s.interrupt, syscall.SIGINT, syscall.SIGTERM)

	return s, nil
}
