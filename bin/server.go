package main

import (
	"flag"
	"github.com/yydzero/mnt/libpq"
	"log"
	"net"
	"strconv"
	"sync"
	"io"
)

var port string
var count int

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)

	flag.StringVar(&port, "p", "5432", "port to listen on")
	flag.IntVar(&count, "c", 10, "Default port to connect")

	flag.Parse()

	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		p, err := strconv.Atoi(port)
		if err != nil {
			log.Printf("Failed to convert port to integer: %s\n", err)
			panic(err)
		}

		wg.Add(1)

		go startServer(&wg, strconv.Itoa(p + i))
	}

	wg.Wait()

	log.Println("Terminated.")
}

func startServer(wg *sync.WaitGroup, port string) {
	defer wg.Done()

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	log.Println("Listening on :" + port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	log.Printf("get a new connection: %v\n", conn)
	s := libpq.NewServer()
	err := s.Serve(conn)
	if err != io.EOF {
		log.Printf("failed to handle a connection: %s\n", err.Error())
	} else {
		log.Println("Client closed connection.")
	}
}
