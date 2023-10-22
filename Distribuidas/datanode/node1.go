package main

import (
	"context"
	"fmt"
	"main/pb"
	"log"
	"net"

	"google.golang.org/grpc"
)

type server struct {
	pb.DataNodeServiceServer
}

func (s *server) Storage(ctx context.Context, req *pb.DataNodeServiceStorage) (*pb.GreetingServiceReply, error) {
	fmt.Println("Storage request recived")
	fmt.Println(req.Id)
	fmt.Println(req.EstaMuerto)
	return &pb.GreetingServiceReply{
		Message: fmt.Sprintf("Hello"),
	}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":8070")
	if err != nil {
		panic(err)
	}
	fmt.Println("Server running on port :8070")

	s := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(s, &server{})
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}