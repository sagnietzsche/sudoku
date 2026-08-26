# sudoku-netcode

A two-player Sudoku in Go with a Fyne GUI, built as a netcode teaching rig: authoritative
fixed-timestep server, client-side prediction, and server reconciliation over WebSockets.

The Sudoku is the excuse. The point is a correct, testable implementation of the
predict/rollback/replay loop, with a game whose rules are cheap enough that any desync you
see is a networking bug and not a physics bug.

**Stack:** Go 1.22+, `coder/websocket` (or `gorilla/websocket`), Fyne v2.6+.

---

## Table of contents

- [Why Sudoku](#why-sudoku)
- [Architecture](#architecture)
- [The netcode model](#the-netcode-model)
- [Determinism contract](#determinism-contract)
- [State is a value type](#state-is-a-value-type)
- [Conflict resolution](#conflict-resolution)
- [Protocol](#protocol)
- [Fyne client architecture](#fyne-client-architecture)
- [Goroutine topology](#goroutine-topology)
- [Tunable constants](#tunable-constants)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [Debug tooling](#debug-tooling)
- [Testing](#testing)
- [Non-goals](#non-goals)
- [Roadmap](#roadmap)
- [References](#references)

---

## Why Sudoku

Most prediction/reconciliation tutorials use a moving square, which entangles two separate
problems: keeping two machines in agreement, and smoothing continuous motion. Sudoku
isolates the first.

What this buys you:

- **Free determinism.** Every operation is integer and discrete. No float drift, no
  accumulator ordering hazards. Replay after a correction is bit-exact, so correctness is
  verifiable by assertion rather than by eyeballing a screen.
- **Free snapshots.** Full state is 81 cells plus pencil marks, locks, and timers, roughly
  300 bytes packed. Full snapshots ship every tick. No delta encoding, no interest
  management, no priority accumulator.
- **Free history.** `State` is a fixed-size value type (see below), so 200 ticks of
  snapshot history is one array and zero allocations.

What it costs you:

- **No rubber-banding, only flicker.** Discrete state does not slide back to a corrected
  position, it swaps. Same bug class, different visual signature.
- **No entity interpolation.** There is no remote entity moving through space, so the
  interpolation half of the standard model is not exercised. See
  [Roadmap](#roadmap) for the continuous-cursor extension that restores it.
- **Nothing to tick, by default.** A plain Sudoku server is event driven. If tick `N` does
  not depend on tick `N-1`, replay is vacuous and rollback is never actually tested.

That last point decides whether this project teaches you anything, so the rules
deliberately include time-varying, server-owned state:

| Mechanic | Why it exists |
|---|---|
| Cell locks with TTL | Two clients both predict "I claimed it." Exactly one is wrong. This is the only real race in the game, and only the authoritative tick order can settle it. |
| Freeze timers on a wrong digit | Must be predicted locally to feel responsive, must be server-owned to be uncheatable. |
| Shared match countdown | Makes tick `N` genuinely a function of tick `N-1`. |

---

## Architecture

```
              internal/game  (State, Op, Step)  <- stdlib-only, no I/O, no Fyne
                     |                    |
        +------------+                    +------------+
        |                                              |
   +----v----------------------+         +-------------v-------+
   |  cmd/client               |  Input  |  cmd/server         |
   |                           | ------> |                     |
   |  net goroutine:           |         |  tick goroutine:    |
   |    predict, replay, diff  |         |    30 Hz ticker     |
   |         | fyne.Do(copy)   |         |    drain + sort      |
   |         v                 | <------ |    Step() = truth   |
   |  Fyne goroutine: refresh  | Snapshot|    broadcast        |
   +---------------------------+         +---------------------+
```

Three layers:

- **`internal/game`** owns `State`, `Op`, and `Step`. It imports nothing outside the
  standard library. A CI test enforces this (see [Testing](#testing)).
- **`cmd/server`** owns the clock. It queues inputs as they arrive and consumes them on a
  fixed tick. It never trusts a client-supplied tick or timestamp.
- **`cmd/client`** owns responsiveness. It applies input locally through the same `Step`,
  keeps unacknowledged inputs in a pending buffer, replays them on top of each
  authoritative snapshot, and hands the result to Fyne as an immutable copy.

**The single most important structural rule:** `Step` exists exactly once and both binaries
import it. If the rules logic is written twice, the project becomes a hunt for divergence
between two implementations of the same function.

---

## The netcode model

### Server: fixed timestep

The tick loop is decoupled from network arrival. Inputs are queued on receipt and consumed
by the tick, never applied on receipt.

```go
ticker := time.NewTicker(config.TickDuration)
defer ticker.Stop()

for {
    select {
    case in := <-inbox:
        queue = append(queue, in)          // arrival order preserved

    case <-ticker.C:
        sortInputs(queue)                  // see Conflict resolution
        state = game.Step(state, queue, tick)
        hub.Broadcast(wire.Snapshot{Tick: tick, State: state, Acked: acked})
        queue = queue[:0]
        tick++
    }
}
```

`time.Ticker` drops ticks rather than accumulating them if the loop stalls, which is
usually what you want on a server. If you need catch-up semantics instead, track a
`nextTick time.Time` and run additional iterations until you have caught up. Either way,
`dt` is a constant and never appears in `Step` as a variable.

### Client: predict, then reconcile

```go
func (c *Client) onLocalInput(op game.Op) {
    c.seq++
    in := game.Input{Seq: c.seq, Player: c.id, Op: op}
    c.pending = append(c.pending, in)
    c.out <- wire.Input{Seq: in.Seq, ClientTick: c.tick, Op: op}

    c.predicted = game.Step(c.predicted, []game.Input{in}, c.tick)
    c.pushToUI()
}

func (c *Client) onSnapshot(s wire.Snapshot) {
    c.predicted = s.State                                  // adopt authority

    acked := s.Acked[c.id]
    c.pending = slices.DeleteFunc(c.pending, func(i game.Input) bool {
        return i.Seq <= acked
    })

    for _, in := range c.pending {                         // replay the unacknowledged
        c.predicted = game.Step(c.predicted, []game.Input{in}, s.Tick)
    }

    if h, ok := c.history.At(s.Tick); ok && h != s.State {  // struct comparison, no reflect
        c.mispredicts++
    }
    c.history.Put(s.Tick, s.State)
    c.pushToUI()
}
```

`Acked` is per-player and carries the highest sequence number the server has consumed from
that player. It is what allows pruning the pending buffer without per-message round trips.

Note `h != s.State`: because `State` contains only comparable fixed-size fields, Go's `==`
is a correct deep comparison. No `reflect.DeepEqual`, no hand-written `Equal` method that
can drift out of sync with the struct.

### What the client does not predict

- Other players' inputs. Remote actions appear only when a snapshot carries them.
- Lock acquisition is predicted optimistically but expected to fail sometimes. The UI
  treats a lost lock as a normal event, not an error.
- Countdowns are predicted by extrapolating from the last snapshot's tick, and corrected on
  every snapshot.

---

## Determinism contract

`Step` must satisfy all of the following. These are enforced by tests, not by convention.

```go
func Step(s State, inputs []Input, tick uint64) State
```

1. **Pure.** No I/O, no logging, no goroutines, no channels.
2. **No wall clock.** Time is the `tick` argument. `time.Now()` inside `Step` is a bug.
3. **No floats.** Integers only. Use fixed-point if a fraction is ever needed.
4. **No maps. At all.** This is the big one in Go.
5. **No ambient randomness.** `math/rand` global functions are seeded nondeterministically
   in modern Go. If randomness is needed, the seed lives in `State` and you advance a
   `*rand.Rand` you own.
6. **Inputs arrive pre-sorted.** Sorting is the caller's job so that the server and the
   client replay path cannot sort differently.

### On rule 4

Go **deliberately randomizes map iteration order** at runtime. It is not incidental, it is
a design decision to stop people depending on it. A `for k, v := range m` inside `Step`
will produce a different result on the client than on the server, intermittently, and the
bug will look like a network problem.

If you need keyed lookup inside `Step`, use a fixed-size array indexed by ID, or a sorted
slice plus binary search. For a 2-player, 81-cell game, arrays cover every case.

```go
// wrong: order varies per run, per process
for id, p := range s.Players { ... }

// right: Players is [2]PlayerState, index is the player ID
for id := range s.Players { ... }
```

Run `go test -run TestDeterminism -count=20` to shake out iteration-order bugs; a single
run will often pass by luck.

---

## State is a value type

This is the design decision that makes everything else cheap in Go.

```go
type State struct {
    Grid      [81]uint8       // 0 = empty, 1..9 = digit
    Pencil    [81]uint16      // bitset, bit k = candidate k+1
    Locks     [81]Lock        // {Owner uint8, ExpiresTick uint64}
    Players   [2]PlayerState
    Countdown uint32
}
```

No slices, no maps, no pointers, no interfaces. Consequences:

| Property | Why it follows |
|---|---|
| `b := a` is a full deep copy | Fixed-size arrays copy by value; slices would not |
| `a == b` is a correct deep comparison | All fields are comparable |
| Snapshot history is `[200]State`, zero allocations | No indirection to chase |
| Handing state to another goroutine is race-free | The receiver gets a copy, not a reference |
| `Step` cannot accidentally mutate its input | The parameter is already a copy |

That fourth row is what makes the Fyne handoff safe without a mutex. It is worth accepting
some awkwardness elsewhere to preserve it.

**Do not** "optimize" this into `Grid []uint8`. A slice makes `b := a` an aliasing bug, and
your replay will start mutating the history buffer you are comparing against.

---

## Conflict resolution

Two clients writing to cell 43 inside one RTT is the central case. The server imposes a
total order:

1. **Arrival tick.** An input queued during tick 100 precedes one queued during tick 101.
2. **Arrival index within the tick.** FIFO by receipt order into that tick's queue.
3. **Player ID.** Final tiebreak, only reachable when replaying a recorded log where two
   inputs share a tick and index.

```go
func sortInputs(q []Input) {
    sort.SliceStable(q, func(i, j int) bool {
        if q[i].ArrivalTick != q[j].ArrivalTick {
            return q[i].ArrivalTick < q[j].ArrivalTick
        }
        if q[i].ArrivalIdx != q[j].ArrivalIdx {
            return q[i].ArrivalIdx < q[j].ArrivalIdx
        }
        return q[i].Player < q[j].Player       // never omit this
    })
}
```

Rule 3 is not decorative. Without a deterministic final tiebreak, a recorded input log can
replay to two different states, which silently invalidates every determinism test. Use
`sort.SliceStable` or `slices.SortStableFunc`; the unstable variants are free to reorder
equal elements differently between runs.

`ClientTick` is carried in the `Input` message for latency diagnostics only. It is never
used for ordering, because it is trivially forgeable.

### Losing a write

When a snapshot overwrites a locally predicted digit:

- The cell flashes `contested` for `ContestFlashTicks`.
- The losing digit is shown struck through for the duration of the flash, then removed.

Silent overwrite reads as a bug to the player even when the netcode is correct.

### Undo

Undo is an **inverse operation submitted through the normal pipeline**, not a pop from a
local stack.

```
Undo -> resolves to OpClearCell{Cell} or OpSetCell{Cell, previous}
```

A local stack desyncs the first time a server correction lands between an operation and its
undo. Routing undo through predict/ack/replay keeps one source of truth.

---

## Protocol

Transport is WebSocket, one connection per client.

Start with `encoding/json` so you can read frames in the debug proxy. Move to a hand-rolled
`encoding/binary` codec when you want the bytes down: because `State` is a fixed-layout
value type, the encoder is roughly thirty lines and has no allocation.

Avoid `encoding/gob` here. It is stream-stateful and Go-only, which makes single-frame
inspection and any future non-Go client painful for no gain at this size.

### Client to server

```go
type Envelope struct {
    Hello *Hello `json:"hello,omitempty"`
    Input *Input `json:"input,omitempty"`
    Ping  *Ping  `json:"ping,omitempty"`
}

type Hello struct { Name string }
type Input struct { Seq uint32; ClientTick uint64; Op game.Op }
type Ping  struct { TClient int64 }
```

```go
type OpKind uint8

const (
    OpSetCell OpKind = iota   // Cell, Value (1..9)
    OpClearCell               // Cell
    OpPencil                  // Cell, Mask
    OpMoveCursor              // Cell, implicitly requests the lock
    OpReleaseLock
    OpUndo
)

type Op struct {
    Kind  OpKind
    Cell  uint8    // 0..80, row*9+col
    Value uint8
    Mask  uint16
}
```

`Op` is a flat comparable struct rather than an interface. Interfaces in the input path
mean type assertions in `Step`, which is a determinism hazard and blocks `==`.

### Server to client

```go
type Welcome struct {
    PlayerID uint8
    Tick     uint64
    PuzzleID uint64
    Givens   [81]uint8
    TickRate uint16
}

type Snapshot struct {
    Tick  uint64
    State game.State
    Acked [2]uint32
}

type Pong struct { TClient int64; ServerTick uint64 }
```

Notes:

- `Givens` ships once in `Welcome`; snapshots carry only mutable state.
- Snapshots are sent every tick and are self-contained. A dropped snapshot needs no recovery
  logic; the next one supersedes it.
- `Pong` echoes `TClient` unmodified so RTT needs no clock synchronization.
- `TickRate` in `Welcome` lets a mismatched client fail loudly at connect instead of
  desyncing slowly.

---

## Fyne client architecture

Fyne is retained mode with a single-goroutine threading model. Both facts change the client
design relative to an immediate-mode or terminal renderer.

### The threading rule

Since Fyne v2.6, all Fyne events, callbacks, and rendering run on one goroutine. Any Fyne
API call from a goroutine you created must be wrapped in `fyne.Do` (fire and forget) or
`fyne.DoAndWait` (block until applied). Calls queued this way run sequentially in the order
received. Calling `fyne.Do` *from* the main goroutine is itself an error, and Fyne logs it.

Two rules follow:

1. **Never run `Step` or replay inside `fyne.Do`.** Reconciliation is pure computation and
   belongs on the net goroutine. Only the resulting refresh is marshalled.
2. **Never bury `fyne.Do` inside a widget's `Refresh`.** Renderer methods are already called
   on the Fyne goroutine. Fix the caller, not the widget.

See <https://docs.fyne.io/started/goroutines/>.

### The handoff

Because `State` is a value type, the handoff needs no mutex. The net goroutine owns
`lastRendered`, computes the diff, and lets the closure capture a copy:

```go
func (c *Client) pushToUI() {
    d := game.Diff(c.lastRendered, c.predicted)   // [81]bool + flags
    if !d.Any() {
        return
    }
    c.lastRendered = c.predicted

    if !c.refreshPending.CompareAndSwap(false, true) {
        return                                     // coalesce; next push carries newer state
    }
    snap := c.predicted                            // copied into the closure

    fyne.Do(func() {
        c.board.Apply(snap, d)
        c.refreshPending.Store(false)
    })
}
```

The UI goroutine only ever reads a copy that nothing else holds a reference to. `go test
-race` stays clean without a single lock.

The `refreshPending` coalescing matters. `fyne.Do` queues, and the queue drains at the
Fyne goroutine's pace. At 30 snapshots per second under load you can enqueue faster than it
drains and build an unbounded backlog of stale closures. Coalescing means the UI always
renders the newest state and never queues more than one pending refresh.

### Board widget

Use **one custom widget for the whole board**, not 81 stock widgets.

```go
type Board struct {
    widget.BaseWidget
    cells [81]cellVisual   // canvas.Rectangle + canvas.Text per cell
}

func (b *Board) CreateRenderer() fyne.WidgetRenderer
func (b *Board) Apply(s game.State, d game.DiffSet)   // Fyne goroutine only
```

`Apply` touches only the cells flagged in `d` and calls `Refresh` once on the board, not
once per cell. With 81 independently refreshed widgets at 30 Hz you are queueing up to
2,430 refresh operations per second through a single goroutine, and Fyne will spend more
time in layout than your netcode spends on everything else.

### Refresh policy

There is no render loop. Fyne draws when something changes, so:

- **Cell and lock state:** event driven, refreshed only on a non-empty diff.
- **Countdowns and freeze timers:** these advance locally between snapshots, so drive them
  from a separate `time.Ticker` at `TimerRefreshRate` (10 Hz is plenty for a display that
  shows tenths). Do not raise the snapshot rate to animate a clock.
- **Debug HUD:** same 10 Hz ticker.

### Input

Keyboard handlers fire on the Fyne goroutine, so they need no `fyne.Do`. They should do as
little as possible: translate to an `Op` and send it down a channel to the net goroutine,
which owns prediction.

```go
w.Canvas().SetOnTypedKey(func(e *fyne.KeyEvent) {
    if op, ok := keyToOp(e.Name); ok {
        select {
        case c.uiInput <- op:            // non-blocking: never stall the UI goroutine
        default:
        }
    }
})
w.Canvas().SetOnTypedRune(func(r rune) { ... })   // digits 1-9
```

`SetOnTypedKey` is a typed-key API driven by OS key repeat. That is correct for discrete
Sudoku input and insufficient for held-key continuous movement; see [Roadmap](#roadmap).

---

## Goroutine topology

**Server:** one accept loop, two goroutines per connection, one tick loop.

```
accept ──> per conn: readPump  ──chan──> tick loop ──> hub.Broadcast
                     writePump <──chan──┘
```

**Client:** the Fyne goroutine, plus read/write pumps, plus a reconcile loop.

```
Fyne goroutine ──chan──> reconcile loop ──chan──> writePump
      ^                        ^
      └──── fyne.Do(copy) ─────┴──chan── readPump
```

Both mainstream WebSocket libraries permit **at most one concurrent reader and one
concurrent writer** per connection. That constraint is what forces the read pump / write
pump split; do not write to a connection from the tick loop and a ping loop at the same
time.

The reconcile loop owns `predicted`, `pending`, `history`, and `lastRendered`. Nothing else
touches them. Every cross-goroutine transfer is a channel send or a value copy into a
`fyne.Do` closure, which is why `-race` should never fire on this codebase.

---

## Tunable constants

Defined once in `internal/config`, imported by both binaries. `TickRate` also ships in
`Welcome` so a version mismatch fails at connect time.

| Constant | Default | Notes |
|---|---|---|
| `TickRate` | 30 Hz | 33.3 ms. Discrete ops plus prediction make 60 Hz unnecessary. |
| `SnapshotRate` | every tick | State is small enough that decimation is not worth it. |
| `SnapshotHistory` | 200 ticks | `[200]State`, about 60 KB, zero allocations. |
| `PendingInputCap` | 256 | Exceeding this means the server is unreachable; enter a reconnect state. |
| `LockTTLTicks` | 60 | 2 s. Also released explicitly on cursor move. |
| `FreezeTicks` | 90 | 3 s penalty for a digit conflicting with the solution. |
| `ContestFlashTicks` | 12 | Cosmetic. |
| `TimerRefreshRate` | 10 Hz | Countdown display only, independent of snapshots. |

---

## Repository layout

```
.
├── go.mod
├── cmd/
│   ├── server/main.go
│   └── client/main.go
├── internal/
│   ├── game/                 # stdlib only, enforced by test
│   │   ├── state.go          # State, PlayerState, Lock
│   │   ├── op.go             # Op, OpKind, validation
│   │   ├── step.go           # THE Step function
│   │   ├── diff.go           # DiffSet for the UI layer
│   │   └── step_test.go
│   ├── wire/                 # Envelope, Snapshot, codec
│   ├── config/               # constants above
│   ├── server/
│   │   ├── hub.go            # connection registry, broadcast
│   │   ├── tick.go           # fixed-timestep loop
│   │   └── queue.go          # per-tick input queue, ordering
│   ├── client/
│   │   ├── net.go            # read/write pumps, RTT
│   │   └── reconcile.go      # pending buffer, replay, diff, fyne.Do handoff
│   ├── ui/                   # Fyne: Board widget, renderer, HUD
│   └── transportsim/         # in-process latency, jitter, loss
└── tests/
    ├── determinism_test.go
    ├── convergence_test.go
    └── contention_test.go
```

`internal/game` importing `fyne.io/fyne/v2` or `net/http` is the failure mode this layout
exists to prevent, and the dependency test below makes it a build failure rather than a
code review comment.

---

## Getting started

Fyne uses cgo for its OpenGL bindings, so you need a C toolchain and the platform GL and
X11 development headers. See <https://docs.fyne.io/started/> for the per-OS package lists.
Cross-compiling is correspondingly awkward; use `fyne-cross` rather than fighting
`GOOS`/`GOARCH` directly.

```bash
go mod download

# terminal 1
go run ./cmd/server -bind 127.0.0.1:9000 -puzzle-seed 42

# terminals 2 and 3
go run ./cmd/client -connect ws://127.0.0.1:9000 -name alice
go run ./cmd/client -connect ws://127.0.0.1:9000 -name bob
```

During development, always run the client with the race detector on:

```bash
go run -race ./cmd/client -connect ws://127.0.0.1:9000 -name alice
```

Fyne v2.6 and later pass Go's race checks internally, so anything `-race` reports in this
app is yours, and it is almost always a missing `fyne.Do` or a `State` field that stopped
being a value type.

### Keybindings

| Key | Action |
|---|---|
| arrows | move cursor (claims the cell lock) |
| `1`-`9` | write digit |
| `p` then `1`-`9` | toggle pencil mark |
| `x` / `Delete` | clear cell |
| `u` | undo |
| `F1` | toggle debug HUD |
| `F2` | cycle latency profile |

---

## Debug tooling

### Latency injection

Latency is simulated **in `internal/transportsim`, inside the process**, not with
`tc`/`netem`. The reason is iteration speed: an in-process delay queue can be reconfigured
from a keybinding mid-game, so you can watch a correction land the instant you raise RTT.

Implementation is a goroutine per direction holding a `container/heap` of
`(deliverAt, payload)`, driven by a `time.Timer` reset to the head of the heap.

```
-latency-profile lan       # 5 ms RTT, 1 ms jitter, 0% loss
-latency-profile regional  # 60 ms RTT, 10 ms jitter, 0.5% loss
-latency-profile bad       # 250 ms RTT, 60 ms jitter, 5% loss
-latency-profile awful     # 400 ms RTT, 150 ms jitter, 10% loss, 2% reorder
```

Use `netem` afterwards as a sanity check, not as the primary tool.

### Debug HUD

`F1` overlays, refreshed on the `TimerRefreshRate` ticker:

```
rtt 247ms  jitter 58ms   server_tick 4182   predicted_tick 4189   drift +7
pending 6   mispredicts 23   snapshots/s 29.4   bytes/s 8.9k   drops 12
fyne.Do queued 1   coalesced 341   goroutines 7
```

**The `mispredicts` counter is the most important number in this project.** If it stays at
zero on the `bad` profile with lock contention active, prediction is not being exercised
and something is wrong: usually the client is applying snapshots without replaying, or
inputs are being blocked until acknowledged.

`goroutines` is `runtime.NumGoroutine()`. If it climbs across reconnects, a pump is leaking.

---

## Testing

```bash
go test ./...                                  # unit plus determinism
go test -race ./...                            # mandatory before any commit
go test -run TestDeterminism -count=20 ./...    # shake out map-iteration bugs
go test -run TestConvergence -timeout 5m ./tests
go test -fuzz FuzzStepDeterminism ./internal/game
go run ./cmd/soak -minutes 30 -profile bad
```

Five properties worth asserting:

1. **Determinism.** Replay a recorded input log twice from the same initial state; the final
   states must be `==`. Run with `-count=20`, because a single pass can get lucky with map
   ordering. Go's native fuzzing generates the input logs for you.
2. **No I/O in the core.** Assert that `internal/game` depends on nothing outside the
   standard library:

   ```go
   func TestGameCoreIsPure(t *testing.T) {
       out, err := exec.Command("go", "list", "-deps", "./internal/game").Output()
       if err != nil { t.Fatal(err) }
       for _, p := range strings.Fields(string(out)) {
           // non-stdlib import paths have a dot in their first segment
           if strings.Contains(strings.SplitN(p, "/", 2)[0], ".") {
               t.Errorf("internal/game must not depend on %s", p)
           }
       }
   }
   ```

3. **Convergence.** For a randomized input log under randomized latency, the client's
   predicted state must equal the server's state at tick `T` once all inputs up to `T` are
   acknowledged. This is the actual definition of a correct reconciliation loop.
4. **Contention.** Two clients writing the same cell within one RTT: exactly one write
   survives, and both clients agree on which.
5. **Soak.** Thirty minutes on `bad`. `mispredicts > 0`, final states equal, pending buffer
   never saturates, `runtime.NumGoroutine()` flat, snapshot history not growing.

`internal/game` has no I/O, so its tests need no fixtures, no network, and no Fyne. They
run in milliseconds. That is the payoff for the layering.

---

## Non-goals

Deliberately out of scope, to keep the netcode legible:

- Matchmaking, lobbies, persistence
- Authentication and accounts
- More than two players (the protocol generalizes, the UI does not)
- Anti-cheat beyond server authority
- NAT traversal and relays
- Mobile builds (Fyne supports them; the keyboard-driven input model does not)
- Delta-compressed snapshots

---

## Roadmap

- **Continuous cursor.** Give the cursor a sub-cell position with velocity instead of
  snapping between cells. This restores everything Sudoku removed: prediction of continuous
  motion, remote entity interpolation, and genuine rubber-banding when a correction lands.
  Two changes are needed. First, position must be fixed-point (`int32` at 1/1024 cell), not
  `float64`, to keep `State` comparable and replay bit-exact. Second, `SetOnTypedKey` is
  driven by OS key repeat and cannot express "held," so you need `desktop.Canvas`'s
  `SetOnKeyDown` / `SetOnKeyUp` from `fyne.io/fyne/v2/driver/desktop` to track key state and
  derive a velocity vector per tick.
- **Lag compensation.** Rewind lock state to the requester's view tick before adjudicating a
  claim, so the higher-latency player stops losing every race.
- **Binary codec.** Replace JSON with `encoding/binary` over the fixed-layout `State`. Cuts
  the frame to roughly 300 bytes with no allocation.
- **Delta snapshots** against the last acknowledged tick, once the continuous cursor makes
  full snapshots wasteful.
- **Replay viewer.** The input log plus a seed already fully determines a match, so the file
  format needed to scrub through one is essentially free.

---

## References

**Netcode**

- Glenn Fiedler, *Fix Your Timestep!* <https://gafferongames.com/post/fix_your_timestep/>
- Glenn Fiedler, *Snapshot Interpolation* <https://gafferongames.com/post/snapshot_interpolation/>
- Glenn Fiedler, *Deterministic Lockstep* <https://gafferongames.com/post/deterministic_lockstep/>
- Gabriel Gambetta, *Fast-Paced Multiplayer* (the clearest diagrams of client-side prediction
  and server reconciliation) <https://www.gabrielgambetta.com/client-server-game-architecture.html>
- Valve Developer Community, *Source Multiplayer Networking*
  <https://developer.valvesoftware.com/wiki/Source_Multiplayer_Networking>
- Timothy Ford, *Overwatch Gameplay Architecture and Netcode*, GDC 2017 (the standard
  reference for rollback plus ECS in a shipped title; available via GDC Vault and the GDC
  YouTube channel)

**Go and Fyne**

- Fyne, *Using Goroutines* <https://docs.fyne.io/started/goroutines/>
- Fyne v2.6 release notes, threading model change
  <https://github.com/fyne-io/fyne/releases/tag/v2.6.0>
- Fyne, *Getting Started* (cgo prerequisites per platform) <https://docs.fyne.io/started/>
- Go Blog, *Go Fuzzing* <https://go.dev/doc/security/fuzz/>
- Go Blog, *Introducing the Go Race Detector* <https://go.dev/blog/race-detector>

These are engineering write-ups and documentation, not peer-reviewed literature. For the
academic treatment, the relevant search terms are *distributed virtual environments*, *dead
reckoning*, and *time warp / optimistic synchronization*, the last of which is the
simulation-theory ancestor of what game developers call rollback.
