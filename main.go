package main

import (
	"fmt"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btclog"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
)

const (
	logFile = "bitstate.log"

	blockCacheSize = 5
)

var (
	log btclog.Logger

	addressBucket = []byte("a")
	utxoBucket    = []byte("u")
	bestHeightKey = []byte("bestheight")

	// cache size default value
	utxoCacheSize    = 1200000
	addressCacheSize = 50000

	utxoReadCount  int     = 0
	utxoRead       float64 = 0
	utxoWriteCount         = 0
	utxoWrite      float64 = 0
	utxoDelCount           = 0
	utxoDel        float64 = 0

	addressReadCount  int     = 0
	addressRead       float64 = 0
	addressWriteCount int     = 0
	addressWrite      float64 = 0

	dummyScript = []byte{0, 0, 0, 0}
)

type Block struct {
	block  *wire.MsgBlock
	height uint32
}

func main() {
	// read config file
	viper.SetConfigFile("config.yml")
	viper.SetConfigType("yml")
	viper.AddConfigPath("./")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Read config file failed: ", err)
		return
	}

	// setup log
	logPath := viper.GetString("server.log.log-path")
	_, err = os.Stat(logPath)
	if err != nil {
		if !os.IsExist(err) {
			err = os.Mkdir(logPath, os.ModePerm)
			if err != nil {
				fmt.Println("Make logger directory failed: ", err)
				os.Exit(1)
			}
		} else {
			fmt.Println("Acquire logger path information failed: ", err)
			os.Exit(1)
		}
	}
	file, err := os.OpenFile(filepath.Join(logPath, logFile), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Open logger file failed: ", err)
		os.Exit(1)
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
		log.Error("New server instance failed: ", err)
		os.Exit(1)
	}

	server.start()

	// finally flush all cache and close database
	server.shutdown()
}