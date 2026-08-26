package client

import (
	"log"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/gorilla/websocket"
	"github.com/sagnikc395/sudoku/server"
)

// Run opens the GUI and connects it to one Sudoku server.
func Run(serverURL string) {
	conn, _, err := websocket.DefaultDialer.Dial(serverURL, nil)
	if err != nil {
		log.Fatal("connect to server: ", err)
	}
	defer conn.Close()
	var writeMu sync.Mutex
	writeMove := func(move server.Move) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(move)
	}

	a := app.New()
	w := a.NewWindow("Sudoku")
	w.Resize(fyne.NewSize(450, 450))

	buttons := make([]*widget.Button, 81)
	items := make([]fyne.CanvasObject, 81)
	for cell := range buttons {
		cell := cell
		buttons[cell] = widget.NewButton("", func() {
			// GUI event -> backend event. The server sends the new board back.
			if err := writeMove(server.Move{Cell: uint8(cell), Value: 1}); err != nil {
				log.Println("send move:", err)
			}
		})
		items[cell] = buttons[cell]
	}
	w.SetContent(container.NewGridWithColumns(9, items...))

	// Backend event -> GUI render. Schedule updates on Fyne's UI thread.
	go func() {
		for {
			var state server.BoardState
			if err := conn.ReadJSON(&state); err != nil {
				return
			}
			fyne.Do(func() {
				for cell, value := range state.Grid {
					if value == 0 {
						buttons[cell].SetText("")
					} else {
						buttons[cell].SetText(string(rune('0' + value)))
					}
				}
			})
		}
	}()

	w.ShowAndRun()
}
