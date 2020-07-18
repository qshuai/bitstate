package main

import (
	"context"
	"log"
	"time"

	"github.com/qshuai/bitstate/proto"
	"google.golang.org/grpc"
)

const (
	address = "localhost:50051"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := proto.NewBlockServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.GetBestBlock(ctx, &proto.BestBlockRequest{})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("the best block: %s-%d", r.GetHash(), r.GetHeight())
}
