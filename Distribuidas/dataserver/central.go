package main

import (
	"context"
	"fmt"
	"main/pb"
	"log"
	"net"
	"strings"
	"os"

	"google.golang.org/grpc"
	"github.com/google/uuid"
)

type GreetingServer struct {
	pb.GreetingServiceServer
	pb.DataServiceServer
	dataNodeClient1 pb.DataNodeServiceClient
	dataNodeClient2 pb.DataNodeServiceClient
}

func (s *DataServiceServer) GetData(ctx context.Context, req *pb.DataRequest) (*pb.DataResponse, error) {
	data, err := readDataFromFile() // Implement this function to read data from "data.txt"
	if err != nil {
		return nil, err
	}
	return &pb.DataResponse{Data: data}, nil
}

func writeLine(Nombre string, Apellido string, Id string){
	line := []byte(Nombre + " " + Apellido + " " + Id + "\n")
	f, err := os.OpenFile("DATA.txt", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(line); err != nil {
		log.Fatal(err)
	}
}

func (s *GreetingServer) Greeting(ctx context.Context, req *pb.GreetingServiceRequest) (*pb.GreetingServiceReply, error) {
	fmt.Println("Greeting request received")
	fmt.Println(req.Nombre)
	fmt.Println(req.Apellido)
	fmt.Println(req.EstaMuerto)

	uniqueID := generateUniqueID()
	fmt.Println(uniqueID)

	writeLine(req.Nombre, req.Apellido, uniqueID)



	dataNodeRequest := &pb.DataNodeServiceStorage{
		Id:        uniqueID,
		EstaMuerto: req.EstaMuerto,
	}

	var dataNodeClient pb.DataNodeServiceClient
	if strings.ToLower(req.Apellido) < "m" {
		dataNodeClient = s.dataNodeClient1
	} else {
		dataNodeClient = s.dataNodeClient2
	}

	_, err := dataNodeClient.Storage(ctx, dataNodeRequest)
	if err != nil {
		log.Fatalf("Failed to call DataNodeService.Storage: %v", err)
	}

	return &pb.GreetingServiceReply{
		Message: fmt.Sprintf("Hello, %s", req.Nombre),
	}, nil
}

func generateUniqueID() string {
    id := uuid.New()
    return id.String()
}

func main() {
	dataNodeConn1, err := grpc.Dial("localhost:8070", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to DataNodeService server 1: %v", err)
	}
	defer dataNodeConn1.Close()
	dataNodeClient1 := pb.NewDataNodeServiceClient(dataNodeConn1)

	dataNodeConn2, err := grpc.Dial("localhost:8071", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to DataNodeService server 2: %v", err)
	}
	defer dataNodeConn2.Close()
	dataNodeClient2 := pb.NewDataNodeServiceClient(dataNodeConn2)

	greetingServer := &GreetingServer{
		dataNodeClient1: dataNodeClient1,
		dataNodeClient2: dataNodeClient2,
	}

	greetingListener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to create listener for GreetingService: %v", err)
	}
	defer greetingListener.Close()

	server := grpc.NewServer()
	pb.RegisterGreetingServiceServer(server, greetingServer)
	go func() {
		if err := server.Serve(greetingListener); err != nil {
			log.Fatalf("Failed to serve GreetingService: %v", err)
		}
	}()

	select {}
}