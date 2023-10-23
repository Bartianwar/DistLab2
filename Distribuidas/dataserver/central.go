package main

import (
	"context"
	"fmt"
	"main/pb"
	"log"
	"net"
	"strings"
	"os"
	"io"
	"bufio"

	"google.golang.org/grpc"
	"github.com/google/uuid"
)

type GreetingServer struct {
	pb.GreetingServiceServer
	dataNodeClient1 pb.DataNodeServiceClient
	dataNodeClient2 pb.DataNodeServiceClient
}

func readDataFromFile() ([]string, error) {
	file, err := os.Open("DATA.txt")
    if err != nil {
        fmt.Println("Error opening file:", err)
    }
    defer file.Close()

    var lines []string

    scanner := bufio.NewScanner(file)
	fmt.Println("Esta antes de scanner")
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
    }
    if err := scanner.Err(); err != nil {
        fmt.Println("Error reading file:", err)
    }

	return lines, nil

}

func getList(Estado string) ([]string, []string) {
	data, err := readDataFromFile()
	if err != nil {
		log.Fatal(err)
	}

	var dn1 []string
	var dn2 []string

	for _, line := range data {

		parts := strings.Split(line, " ")
		if parts[1] == Estado {
			if parts[2] == "1"{
				dn1 = append(dn1, parts[0])
			} else {
				dn2 = append(dn2, parts[0])
			}
		}
	}

	return dn1, dn2
}

func (s *GreetingServer) GetNames(ctx context.Context, req *pb.DataState) (*pb.DataNames, error) {
	db1, db2 := getList(req.Data)
	fmt.Println(db1)
	fmt.Println(db2)

	stream, err := s.dataNodeClient1.GetData(ctx)
	
	if err != nil {
		log.Fatalf("Failed to call DataNodeService.GetData: %v", err)
	}
	for _, data := range db1 {
		stream.Send(&pb.DataRequest{Data: data})
	}

	if err := stream.CloseSend(); err != nil {
		log.Fatalf("Failed to close stream: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to receive stream response: %v", err)
		}
		fmt.Println(resp.Data)
	}

	stream, err = s.dataNodeClient2.GetData(ctx)
	
	if err != nil {
		log.Fatalf("Failed to call DataNodeService.GetData: %v", err)
	}
	for _, data := range db2 {
		stream.Send(&pb.DataRequest{Data: data})
	}

	if err := stream.CloseSend(); err != nil {
		log.Fatalf("Failed to close stream: %v", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Failed to receive stream response: %v", err)
		}
		fmt.Println(resp.Data)
	}

    return &pb.DataNames{Data: "WowFunciono"}, nil
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

	getNamesListener, err := net.Listen("tcp", ":8085")
	if err != nil {
		log.Fatalf("Failed to create listener for GetNames: %v", err)
	}
	defer getNamesListener.Close()

	go func() {
		if err := server.Serve(greetingListener); err != nil {
			log.Fatalf("Failed to serve GreetingService: %v", err)
		}
	}()

	go func() {
		if err := server.Serve(getNamesListener); err != nil {
			log.Fatalf("Failed to serve GetNames: %v", err)
		}
	}()

	select {}
}