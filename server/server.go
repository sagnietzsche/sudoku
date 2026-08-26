package server

import (
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sagnikc395/sudoku/game"
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
	clients map[*client]struct{}
}

// client serializes writes because gorilla/websocket permits one concurrent
// reader and one concurrent writer per connection.
type client struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (c *client) writeJSON(value any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(value)
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func ListenAndServe(addr string) error {
	s := &Server{clients: make(map[*client]struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleClient)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleClient(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade websocket: %v", err)
		return
	}
	client := &client{conn: ws}

	s.mu.Lock()
	s.clients[client] = struct{}{}
	state := BoardState{Grid: s.game.Grid}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.clients, client)
		s.mu.Unlock()
		ws.Close()
	}()

	if err := client.writeJSON(state); err != nil {
		return
	}

	for {
		var move Move
		if err := ws.ReadJSON(&move); err != nil {
			return
		}

		s.mu.Lock()
		accepted := s.game.SetCell(move.Cell, move.Value)
		log.Printf("sending the message %v", accepted)
		state = BoardState{Grid: s.game.Grid}
		s.mu.Unlock()
		if accepted {
			s.broadcast(state)
		}
	}
}

func (s *Server) broadcast(state BoardState) {
	s.mu.Lock()
	clients := make([]*client, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.Unlock()

	for _, client := range clients {
		if err := client.writeJSON(state); err != nil {
			log.Printf("broadcast failed: %v", err)
		}
	}
}
