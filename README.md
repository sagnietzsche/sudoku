# sudoku

a tiny multiplayer game where 2 players connect over a network and stay in sync (even with lag)
like netcode (websockets , then add client side prediction(tui) + server reconciliation so player movement 
doesnt rubber-band, and a fixed-timestep tick loop on the server as the souce of truth)
