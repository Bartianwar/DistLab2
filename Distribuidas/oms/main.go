package main

import (
	"context"
	"log"
	"google.golang.org/grpc"
	"fmt"
	"main/pb"
)

func sendMessage(client pb.GreetingServiceClient, request *pb.DataState) (*pb.DataNames, error) {
    resp, err := client.GetNames(context.Background(), request)
    if err != nil {
        log.Fatal(err)
    }
    return resp, err
}

func main() {
    opts := grpc.WithInsecure()
    cc, err := grpc.Dial("localhost:8085", opts)
    if err != nil {
        log.Fatal(err)
    }
    defer cc.Close()

    client := pb.NewGreetingServiceClient(cc)

    result, err := sendMessage(client, &pb.DataState{Data: "muerto"})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result.Data)
}