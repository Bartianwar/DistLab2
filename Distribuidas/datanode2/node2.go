package main

import (
	"context"
	"fmt"
	"main/pb"
	"log"
	"net"
	"os"
	"google.golang.org/grpc"
	"io"
	"bufio"
	"strings"
)

type server struct {
	pb.DataNodeServiceServer
}

func writeLine(Id string , Nombre string, Apellido string) {
	line := []byte(Id + " " + Nombre + " " + Apellido + "\n")
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
	fmt.Println(req.Nombre)
	fmt.Println(req.Apellido)
	writeLine(req.Id, req.Nombre, req.Apellido)
	return &pb.GreetingServiceReply{
		Message: fmt.Sprintf("Hello"),
	}, nil
}

func (s *server) GetData(stream pb.DataNodeService_GetDataServer) error {
    for {
        req, err := stream.Recv()
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }

        foundData, err := searchDataInFile(req.Data)
        if err != nil {
            return err
        }

        resp := &pb.DataResponse{Data: foundData}
        if err := stream.Send(resp); err != nil {
            return err
        }
    }
}

func searchDataInFile(searchData string) (string, error) {
    file, err := os.Open("DATA.txt")
    if err != nil {
        return "", err
    }
    defer file.Close()
    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.Split(line, " ")
		if parts[0] == searchData {
			line = parts [1] + " " + parts[2]
			return line, nil
		}
    }
    if err := scanner.Err(); err != nil {
        return "", err
    }
    return "Data not found", nil
}

func main() {
	listener, err := net.Listen("tcp", ":8071")
	if err != nil {
		panic(err)
	}
	fmt.Println("Server running on port :8071")

	s := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(s, &server{})
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}