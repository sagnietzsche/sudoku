# sudoku-netcode

A small two-player Sudoku game in Go. The first goal is a clear, playable
multiplayer game—not a full networking experiment.

The server owns the board. Clients send moves, the server accepts or rejects
them, and accepted board states are sent back to every client.

## Keep the architecture small

```text
Fyne UI -> client -> WebSocket -> server -> WebSocket -> client UI
                              |
                              v
                        Sudoku rules
```

There is one source of truth: the server's game state. A client should not
permanently change the shared board until the server broadcasts the accepted
state back.

## Suggested layout

```text
.
├── main.go              # starts a server or client from a command-line flag
├── game/
│   ├── state.go          # Game, puzzle, and current grid
│   └── rules.go          # SetCell, ClearCell, and Sudoku validation
├── server/
│   └── server.go         # WebSocket clients and board broadcasts
└── client/
    └── app.go            # Fyne board, input, and server messages
```

Use fewer packages until a real need for another one appears. A package should
exist because it owns a clear responsibility, not because it was listed in an
ideal architecture diagram.

## Core game model

Keep the game state simple and easy to copy:

```go
type Game struct {
    Puzzle [81]uint8 // original clues; 0 means editable
    Grid   [81]uint8 // current values; 0 means empty
}
```

Put all Sudoku rules in one place:

```go
func (g *Game) SetCell(cell, value uint8) bool {
    if g.Puzzle[cell] != 0 || !validMove(*g, cell, value) {
        return false
    }
    g.Grid[cell] = value
    return true
}
```

The server and client can both use these rules for display and validation, but
only the server decides which move becomes shared state.

## Minimal message flow

Start with just two WebSocket message types:

```go
type Move struct {
    Cell  uint8
    Value uint8
}

type BoardState struct {
    Grid [81]uint8
}
```

The sequence for a move is:

1. The player clicks a cell and chooses a digit.
2. The client sends `Move` to the server.
3. The server calls `game.SetCell`.
4. If the move is valid, the server broadcasts `BoardState` to all clients.
5. Each client replaces its displayed grid with that state.

This is event-driven: no game tick, input queue, prediction history, or
rollback is needed for a normal Sudoku game.

## What not to build yet

Avoid these until the simple version works and you have a reason to add them:

- Fixed-rate server ticks
- Client-side prediction and rollback/replay
- Per-input sequence acknowledgements
- Snapshot history and diffing
- Cell lock timeouts and freeze timers
- Separate transport simulation or protocol packages
- Multiple networking goroutines beyond what the WebSocket library requires

These are useful for a netcode learning project, but they hide the actual game
behind a lot of machinery.

## Incremental roadmap

Build the game in this order:

1. Render one local Sudoku board in Fyne.
2. Implement `SetCell`, clearing cells, and row/column/box validation.
3. Run one server and connect one client.
4. Send a move to the server and redraw after a returned board state.
5. Connect a second client and broadcast accepted moves to both.
6. Add polish: player names, a reset button, puzzle selection, and completion.

Only then consider advanced networking features:

- Add optimistic local updates if network delay makes the board feel sluggish.
- Add cell ownership/locks if simultaneous edits are confusing.
- Add explicit acknowledgements if optimistic updates are added.
- Add deterministic ticks and rollback only if the goal becomes learning those
  netcode techniques specifically.

## Design rule

Prefer the simplest code that makes the current feature correct. The server is
authoritative, `game` owns Sudoku rules, and the UI renders the latest server
state. That is enough for a solid multiplayer Sudoku game.

## Run 

1. Run the server first as 
```shell
go run . -mode server
```
2. Run the client 
```shell
go run . -mode client
```
