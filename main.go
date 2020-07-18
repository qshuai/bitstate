package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/btcsuite/btclog"
	"github.com/qshuai/bitstate/handler"
	"github.com/qshuai/bitstate/proto"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

const (
	logFile = "bitstate.log"
)

var (
	log btclog.Logger
)

func main() {
	loadConfig()
	setupLogging()

	// get server instance
	server, err := handler.NewServer(log)
	if err != nil {
		log.Errorf("New server instance failed: %s", err)
		os.Exit(1)
	}

	// exit gracefully
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-interrupt:
			log.Info("Receiving interrupt signal, preparing exit program")
			server.Stop()
		}
	}()

	go func() {
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Errorf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		proto.RegisterBlockServiceServer(s, &RPCService{blockHandler: server})
		if err := s.Serve(lis); err != nil {
			log.Errorf("failed to serve: %v", err)
		}
	}()

	server.Start()
}

func loadConfig() {
	// read config file
	viper.SetConfigFile("config.yml")
	viper.SetConfigType("yml")
	viper.AddConfigPath("./")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Read config file failed:", err)
		os.Exit(1)
	}
}

func setupLogging() {
	// setup log
	logPath := viper.GetString("server.log.log-path")
	_, err := os.Stat(logPath)
	if err != nil {
		if os.IsNotExist(err) {
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
}
