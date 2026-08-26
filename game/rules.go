package game

func validMove(g Game, cell, value uint8) bool {
	if cell >= 81 || value == 0 || value > 9 {
		return false
	}

	row, col := int(cell)/9, int(cell)%9
	for i := 0; i < 9; i++ {
		if g.Grid[row*9+i] == value || g.Grid[i*9+col] == value {
			return false
		}
	}

	boxRow, boxCol := row/3*3, col/3*3
	for r := boxRow; r < boxRow+3; r++ {
		for c := boxCol; c < boxCol+3; c++ {
			if g.Grid[r*9+c] == value {
				return false
			}
		}
	}
	return true
}

func (g *Game) SetCell(cell, value uint8) bool {
	if cell >= 81 || g.Puzzle[cell] != 0 || !validMove(*g, cell, value) {
		return false
	}
	g.Grid[cell] = value
	return true
}
