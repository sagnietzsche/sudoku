package server

import (
	"log"
	"net/http"
	"sync"

	"github.com/sagnikc395/sudoku/game"
	"golang.org/x/net/websocket"
)

type Move struct {
	Cell  uint8
	Value uint8
}

type BoardState struct {
	Grid [81]uint8
}

type Server struct {
	mu      sync.Mutex
	game    game.Game
	clients map[*websocket.Conn]struct{}
}

func ListenAndServe(addr string) error {
	s := &Server{clients: make(map[*websocket.Conn]struct{})}
	mux := http.NewServeMux()
	mux.Handle("/ws", websocket.Handler(s.handleClient))
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleClient(ws *websocket.Conn) {
	s.mu.Lock()
	s.clients[ws] = struct{}{}
	state := BoardState{Grid: s.game.Grid}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clients, ws)
		s.mu.Unlock()
		ws.Close()
	}()

	if err := websocket.JSON.Send(ws, state); err != nil {
		return
	}

	for {
		var move Move
		if err := websocket.JSON.Receive(ws, &move); err != nil {
			return
		}

		s.mu.Lock()
		accepted := s.game.SetCell(move.Cell, move.Value)
		state = BoardState{Grid: s.game.Grid}
		s.mu.Unlock()
		if accepted {
			s.broadcast(state)
		}
	}
}

func (s *Server) broadcast(state BoardState) {
	s.mu.Lock()
	clients := make([]*websocket.Conn, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	for _, client := range clients {
		if err := websocket.JSON.Send(client, state); err != nil {
			log.Printf("broadcast failed: %v", err)
		}
	}
}
