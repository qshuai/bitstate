package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

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
	utxoCacheSize    = 200000
	addressCacheSize = 500000

	// subcache size
	utxoSubcacheSize    = 10000
	addressSubcacheSize = 10000
)

var (
	log btclog.Logger

	addressBucket = []byte("address")
	utxoBucket    = []byte("utxo")
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
}

func (s *Server) SyncBlocks() {
	for i := s.startBlock; i <= s.endBlock; i++ {
		if s.closed.Load().(bool) {
			fmt.Println("has requested shutdown")
			close(s.blockContainer)
			return
		}

		blockHash, err := s.rpcBackend.GetBlockHash(uint64(i))
		if err != nil {
			fmt.Printf("get block hash failed: %s", err)
			close(s.blockContainer)
			return
		}

		rawBlock, err := s.rpcBackend.GetRawBlock(blockHash)
		if err != nil {
			fmt.Printf("get block failed: %s", err)
			close(s.blockContainer)
			return
		}
		var block wire.MsgBlock
		blockBytes, err := hex.DecodeString(rawBlock)
		if err != nil {
			fmt.Printf("decode block failed: %s\n", err)
			close(s.blockContainer)
			return
		}
		err = block.Deserialize(bytes.NewReader(blockBytes))
		if err != nil {
			fmt.Printf("deserialize block failed: %s\n", err)
			close(s.blockContainer)
			return
		}

		if err != nil {
			fmt.Printf("get block failed: %s\n", err)
			s.closed.Store(true)
			close(s.blockContainer)
			return
		}

		s.blockContainer <- &Block{
			block:  &block,
			height: uint32(i),
		}
	}

	// the channel should be closed by producer
	close(s.blockContainer)
}

func (s *Server) start() {
	// the goroutine be responsible for get block and put to cache
	go s.SyncBlocks()

	var inputs, outputs int
	for block := range s.blockContainer {
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
					s.utxoCache.Remove(view.GetKey())

					spend(&txHash, view.entry)
				} else {
					// find utxo in secondary cache
					entry := s.utxoSubcache[hex.EncodeToString(utxoDBKey)]
					if entry != nil {
						delete(s.utxoSubcache, utxoMapKey)

						spend(&txHash, entry)
					} else {
						err := s.db.View(func(tx *bbolt.Tx) error {
							value := tx.Bucket(utxoBucket).Get(utxoDBKey)
							if value == nil {
								return errors.New("utxo entry not found in database")
							}

							return nil
						})
						if err != nil {
							log.Error("Fetch utxo entry in database failed:", err)
							return
						}

						err = s.db.View(func(tx *bbolt.Tx) error {
							return tx.Bucket(utxoBucket).Delete(utxoDBKey)
						})
						if err != nil {
							log.Error("Remove the spent utxo entry failed:", err)
							return
						}
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

				receive(&txHash, output)
			}
		}

		log.Infof("Handle block %s:%d, inputs: %d, outputs: %d",
			block.block.BlockHash().String(), block.height, inputs, outputs)
	}
}

func (s *Server) shutdown() {
	err := s.db.Close()
	if err != nil {
		log.Error("close bbolt database failed:", err)
	}
}

func (s *Server) carryUtxoSubcache(entry lru.Item) error {
	view := entry.(*UtxoViewCache)
	s.utxoSubcache[view.key] = view.entry

	// check whether trigger full cache
	if len(s.utxoSubcache) >= utxoSubcacheSize {
		for hashAndIndex, view := range s.utxoSubcache {
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
	}

	return nil
}

func (s *Server) carryAddressSubcache(entry lru.Item) error {
	view := entry.(*AddressBalanceInfoCache)
	s.addressSubcache[view.key] = view.entry

	// check whether trigger full cache
	if len(s.addressSubcache) >= addressSubcacheSize {
		for scriptHex, info := range s.addressSubcache {
			err := s.db.Update(func(tx *bbolt.Tx) error {
				key, _ := hex.DecodeString(scriptHex)
				err := tx.Bucket(utxoBucket).Put(key, info.Encode())
				if err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
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
	level, _ := btclog.LevelFromString("server.log.level")
	log.SetLevel(level)

	// get server instance
	server, err := NewServer()
	if err != nil {
		log.Error("New server instance failed:", err)
		return
	}

	server.start()
}

func NewServer() (*Server, error) {
	db, err := bbolt.Open(viper.GetString("server.db.dbpath"), 0600,
		&bbolt.Options{
			Timeout:      0,
			NoGrowSync:   false,
			FreelistType: bbolt.FreelistArrayType,
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

	s := &Server{
		db:              db,
		rpcBackend:      bc,
		blockContainer:  make(chan *Block),
		utxoCache:       lru.New(addressCacheSize, true),
		addressCache:    lru.New(utxoCacheSize, false),
		utxoSubcache:    make(map[string]*UtxoView, utxoSubcacheSize),
		addressSubcache: make(map[string]*AddressBalanceInfo, addressSubcacheSize),

		startBlock: viper.GetInt("server.task.start"),
		endBlock:   viper.GetInt("server.task.end"),
	}
	s.closed.Store(false)
	s.utxoCache.Callback = s.carryUtxoSubcache
	s.addressCache.Callback = s.carryAddressSubcache

	return s, nil
}
