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
	dataNodeClient1 pb.DataNodeServiceClient
	dataNodeClient2 pb.DataNodeServiceClient
}

type DataServiceServer struct {
	pb.DataServiceServer
}


func readDataFromFile() ([]string, error) {
	file, err := os.Open("DATA.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return nil, nil
}

func (s *DataServiceServer) GetData(ctx context.Context, req *pb.DataRequest) (*pb.DataResponse, error) {
	data, err := readDataFromFile()
	if err != nil {
		return nil, err
	}
	return &pb.DataResponse{Data: data}, nil
}

func writeLine(Id string, cualData string, Estado string){
	line := []byte(Id + " " + Estado + " " + cualData +"\n")
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
	var Estado string
	if req.EstaMuerto {
		Estado = "muerto"
	} else {
		Estado = "infectado"
	}

	uniqueID := generateUniqueID()
	fmt.Println(uniqueID)

	dataNodeRequest := &pb.DataNodeServiceStorage{
		Id:        uniqueID,
		Nombre:    req.Nombre,
		Apellido:  req.Apellido,
	}

	

	var dataNodeClient pb.DataNodeServiceClient
	if strings.ToLower(req.Apellido) < "m" {
		writeLine(uniqueID, "1", Estado)
		dataNodeClient = s.dataNodeClient1
	} else {
		writeLine(uniqueID, "2", Estado)
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