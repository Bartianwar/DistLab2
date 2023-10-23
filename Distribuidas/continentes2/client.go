package main

import (
	"strings"
	"context"
	"fmt"
	"main/pb"
	"log"
	"google.golang.org/grpc"
	"bufio"
	"io/ioutil"
	"math/rand"
	"os"
	"time"
)

func readLines(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func writeLines(filename string, lines []string) error {
	content := []byte(strings.Join(lines, "\n"))
	return ioutil.WriteFile(filename, content, 0644)
}

func sendMessage(client pb.GreetingServiceClient, request *pb.GreetingServiceRequest) (*pb.GreetingServiceReply, error) {
	resp, err := client.Greeting(context.Background(), request)
	if err != nil {
		log.Fatal(err)
	}
	return resp, err
}

func randDeath() bool {
	rand.Seed(time.Now().UnixNano())
	if rand.Float64() < 0.45 {	
		return true
	} else {
		return false
	}
}

func processLine(cc *grpc.ClientConn, line string) {
	lineSplit := strings.Split(line, " ")
	client := pb.NewGreetingServiceClient(cc)
	request := &pb.GreetingServiceRequest{
		Nombre:     lineSplit[0],
		Apellido:   lineSplit[1],
		EstaMuerto: randDeath(),
	}
	resp, err := sendMessage(client, request)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Message)
}

func main() {
	opts := grpc.WithInsecure()
	cc, err := grpc.Dial("localhost:8080", opts)
	if err != nil {
		log.Fatal(err)
	}
	defer cc.Close()

	lines, err := readLines("names.txt")
	if err != nil {
		fmt.Println("Error reading the file:", err)
		return
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(lines), func(i, j int) {
		lines[i], lines[j] = lines[j], lines[i]
	})

	numLinesToRead := 5
	if len(lines) < numLinesToRead {
		numLinesToRead = len(lines)
	}

	for currLine := 0; currLine < numLinesToRead; currLine++ {
		processLine(cc, lines[currLine])
	}

	for currLine := numLinesToRead; currLine < len(lines); currLine++ {
		time.Sleep(3 * time.Second)
		processLine(cc, lines[currLine])
	}
}