package main

import (
	"context"
	"fmt"
	"main/pb"
	"log"
	"net"
	"os"
	"google.golang.org/grpc"
)

type server struct {
	pb.DataNodeServiceServer
}

func writeLine(Estado bool, Id string) {
	var EstadoF string
	if Estado {
		EstadoF = "muerto"
	} else {
		EstadoF = "infectado"
	}
	line := []byte(Id + " " + EstadoF + "\n")
	f, err := os.OpenFile("DATA.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(line); err != nil {
		log.Fatal(err)
	}
}

func (s *server) Storage(ctx context.Context, req *pb.DataNodeServiceStorage) (*pb.GreetingServiceReply, error) {
	fmt.Println("Storage request recived")
	fmt.Println(req.Id)
	fmt.Println(req.EstaMuerto)
	writeLine(req.EstaMuerto, req.Id)
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