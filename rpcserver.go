package main

import (
	"context"

	"github.com/qshuai/bitstate/handler"
	"github.com/qshuai/bitstate/proto"
)

const (
	port = ":50051"
)

type RPCService struct {
	blockHandler *handler.BlockHandler
	proto.UnimplementedBlockServiceServer
}

func (s *RPCService) GetBestBlock(ctx context.Context, in *proto.BestBlockRequest) (*proto.BestBlock, error) {
	block, err := s.blockHandler.GetBestBlock()
	if err != nil {
		return nil, err
	}

	return &proto.BestBlock{
		Hash:   block.Hash,
		Height: int32(block.Height),
	}, nil
}
