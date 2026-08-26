package main

import (
	"flag"
	"log"

	"github.com/sagnikc395/sudoku/client"
	"github.com/sagnikc395/sudoku/server"
)

func main() {
	mode := flag.String("mode", "client", "run as client or server")
	addr := flag.String("addr", "localhost:8080", "server address")
	flag.Parse()

	switch *mode {
	case "server":
		log.Printf("Sudoku server listening on %s", *addr)
		log.Fatal(server.ListenAndServe(*addr))
	case "client":
		client.Run("ws://" + *addr + "/ws")
	default:
		log.Fatal("-mode must be either client or server")
	}
}
