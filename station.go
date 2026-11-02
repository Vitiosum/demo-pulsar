package main

// The station: what a departure is, how the board folds events into rows,
// and the simulator that plays the station clock. Nothing here knows about
// Pulsar: the board only ever changes through events read from the topic.

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

type Status string

const (
	StatusOnTime    Status = "on_time"
	StatusDelayed   Status = "delayed"
	StatusBoarding  Status = "boarding"
	StatusDeparted  Status = "departed"
	StatusCancelled Status = "cancelled"
)

type Kind string

const (
	KindAnnounced Kind = "announced"
	KindPlatform  Kind = "platform" // platform assigned or changed
	KindDelayed   Kind = "delayed"
	KindBoarding  Kind = "boarding"
	KindDeparted  Kind = "departed"
	KindCancelled Kind = "cancelled"
)

// Train is one row of the board. Every event carries the full row after the
// change, so the board is simply "the newest version of each train".
type Train struct {
	ID          string    `json:"id"`
	Service     string    `json:"service"`
	Number      string    `json:"number"`
	Destination string    `json:"destination"`
	Scheduled   time.Time `json:"scheduled"`
	Delay       int       `json:"delay"` // minutes
	Platform    string    `json:"platform"`
	Status      Status    `json:"status"`
	UpdatedAt   time.Time `json:"updatedAt"` // time of the event that produced this version
}

// Departure is the expected departure time, delay included.
func (t Train) Departure() time.Time { return t.Scheduled.Add(time.Duration(t.Delay) * time.Minute) }

func (t Train) final() bool { return t.Status == StatusDeparted || t.Status == StatusCancelled }

// Event is the payload of one Pulsar message on the departures topic.
type Event struct {
	Kind  Kind   `json:"kind"`
	By    string `json:"by"` // "clock" (the simulator) or "station-master" (a visitor)
	Train Train  `json:"train"`
}

// ---------------------------------------------------------------- board

type Board struct {
	mu     sync.RWMutex
	trains map[string]Train
}

func NewBoard() *Board { return &Board{trains: map[string]Train{}} }

// Apply folds one event into the board. Older versions of a row are ignored,
// so replaying or receiving a message twice is harmless.
func (b *Board) Apply(e Event) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur, ok := b.trains[e.Train.ID]
	if ok && !e.Train.UpdatedAt.After(cur.UpdatedAt) {
		return false
	}
	b.trains[e.Train.ID] = e.Train
	for id, t := range b.trains { // forget rows that left the board long ago
		if e.Train.UpdatedAt.Sub(t.UpdatedAt) > 3*time.Hour {
			delete(b.trains, id)
		}
	}
	return true
}

func (b *Board) Get(id string) (Train, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.trains[id]
	return t, ok
}

// Rows returns the trains shown on the board at a given time, in timetable order.
func (b *Board) Rows(now time.Time) []Train {
	b.mu.RLock()
	defer b.mu.RUnlock()
	rows := make([]Train, 0, len(b.trains))
	for _, t := range b.trains {
		if visible(t, now) {
			rows = append(rows, t)
		}
	}
	slices.SortFunc(rows, func(a, c Train) int {
		if d := a.Scheduled.Compare(c.Scheduled); d != 0 {
			return d
		}
		return compareStrings(a.ID, c.ID)
	})
	return rows
}

func visible(t Train, now time.Time) bool {
	switch t.Status {
	case StatusDeparted:
		return now.Sub(t.UpdatedAt) < time.Minute
	case StatusCancelled:
		return now.Sub(t.UpdatedAt) < 3*time.Minute
	default: // safety net if no station clock is running
		return now.Sub(t.Departure()) < 10*time.Minute
	}
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// ---------------------------------------------------------------- simulator

const (
	targetUpcoming = 12               // trains announced ahead
	platformLead   = 4 * time.Minute  // platform shown 4 min before departure
	boardingLead   = 90 * time.Second // boarding starts 90 s before departure
	maxPerTick     = 3
	platforms      = 12
)

type route struct{ service, destination string }

var routes = []route{
	{"IC", "LYON PART-DIEU"}, {"IC", "MARSEILLE"}, {"IC", "BORDEAUX"}, {"IC", "LILLE EUROPE"},
	{"EC", "BRUXELLES-MIDI"}, {"EC", "AMSTERDAM"}, {"EC", "GENEVE"}, {"EC", "LAUSANNE"},
	{"EC", "ZURICH"}, {"EC", "MILAN"}, {"EC", "TURIN"}, {"EC", "BARCELONE"},
	{"EC", "FRANCFORT"}, {"EC", "MUNICH"}, {"NJ", "BERLIN"}, {"NJ", "VARSOVIE"},
	{"RE", "ROUBAIX"}, {"IC", "STRASBOURG"}, {"IC", "NANTES"}, {"IC", "RENNES"},
	{"IC", "MONTPELLIER"}, {"IC", "NICE VILLE"}, {"IC", "TOULOUSE"}, {"RE", "DIJON"},
	{"RE", "METZ"}, {"IC", "GRENOBLE"}, {"RE", "ANNECY"}, {"IC", "LA ROCHELLE"},
	{"IC", "CLERMONT-FD"}, {"IC", "BREST"}, {"RE", "LE HAVRE"}, {"RE", "REIMS"},
}

// Plan decides what happens next at the station, given the current board.
// It returns at most a few events; the caller publishes them to Pulsar.
func Plan(rows []Train, now time.Time, rnd *rand.Rand, incidents bool) []Event {
	var events []Event
	upcoming := 0
	for _, t := range rows {
		if t.final() {
			continue
		}
		upcoming++
		if len(events) == maxPerTick {
			continue
		}
		left := t.Departure().Sub(now)
		switch {
		case left <= 0:
			t.Status = StatusDeparted
			events = append(events, clockEvent(KindDeparted, t, now))
		case t.Platform == "" && left <= platformLead:
			t.Platform = pickPlatform(rows, t, rnd)
			events = append(events, clockEvent(KindPlatform, t, now))
		case t.Platform != "" && t.Status != StatusBoarding && left <= boardingLead:
			t.Status = StatusBoarding
			events = append(events, clockEvent(KindBoarding, t, now))
		}
	}
	if len(events) < maxPerTick && upcoming < targetUpcoming {
		events = append(events, clockEvent(KindAnnounced, newTrain(rows, now, rnd), now))
	}
	if incidents && len(events) == 0 && rnd.Float64() < 0.045 {
		if e, ok := incident(rows, now, rnd); ok {
			events = append(events, e)
		}
	}
	return events
}

// Seed fills an empty board in one go, so the first screen is never empty.
// It plays Plan on a scratch copy until nothing more is due.
func Seed(now time.Time, rnd *rand.Rand) []Event {
	scratch := NewBoard()
	var out []Event
	for i := range 60 {
		at := now.Add(time.Duration(i) * time.Millisecond)
		events := Plan(scratch.Rows(at), at, rnd, false)
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			scratch.Apply(e)
		}
		out = append(out, events...)
	}
	return out
}

func clockEvent(k Kind, t Train, now time.Time) Event {
	t.UpdatedAt = now
	return Event{Kind: k, By: "clock", Train: t}
}

func newTrain(rows []Train, now time.Time, rnd *rand.Rand) Train {
	// Next departure: in the same minute as the last announced one or the
	// minute after, at most two per minute, never sooner than now + 2 minutes.
	next := now.Truncate(time.Minute).Add(2 * time.Minute)
	for _, t := range rows {
		if !t.final() && t.Scheduled.After(next) {
			next = t.Scheduled
		}
	}
	sameMinute := 0
	for _, t := range rows {
		if !t.final() && t.Scheduled.Equal(next) {
			sameMinute++
		}
	}
	if sameMinute >= 2 || (sameMinute == 1 && rnd.IntN(2) == 0) {
		next = next.Add(time.Minute)
	}
	r := routes[rnd.IntN(len(routes))]
	for range 6 { // avoid two upcoming trains to the same city
		if !slices.ContainsFunc(rows, func(t Train) bool { return !t.final() && t.Destination == r.destination }) {
			break
		}
		r = routes[rnd.IntN(len(routes))]
	}
	number := fmt.Sprintf("%04d", 1000+rnd.IntN(9000))
	return Train{
		ID:          r.service + number + "-" + next.Format("0601021504"),
		Service:     r.service,
		Number:      number,
		Destination: r.destination,
		Scheduled:   next,
		Status:      StatusOnTime,
	}
}

func pickPlatform(rows []Train, t Train, rnd *rand.Rand) string {
	busy := map[string]bool{}
	for _, o := range rows {
		if o.ID != t.ID && !o.final() && o.Platform != "" && absDuration(o.Departure().Sub(t.Departure())) < 5*time.Minute {
			busy[o.Platform] = true
		}
	}
	p := ""
	for range 12 {
		p = fmt.Sprint(1 + rnd.IntN(platforms))
		if !busy[p] && p != t.Platform {
			break
		}
	}
	return p
}

func incident(rows []Train, now time.Time, rnd *rand.Rand) (Event, bool) {
	var eligible []Train
	for _, t := range rows {
		if !t.final() && t.Status != StatusBoarding && t.Departure().Sub(now) > 3*time.Minute {
			eligible = append(eligible, t)
		}
	}
	if len(eligible) == 0 {
		return Event{}, false
	}
	t := eligible[rnd.IntN(len(eligible))]
	switch roll := rnd.Float64(); {
	case roll < 0.6 && t.Delay < 45:
		t.Delay += []int{5, 10, 15}[rnd.IntN(3)]
		t.Status = StatusDelayed
		return clockEvent(KindDelayed, t, now), true
	case roll < 0.9 && t.Platform != "":
		t.Platform = pickPlatform(rows, t, rnd)
		return clockEvent(KindPlatform, t, now), true
	case roll >= 0.9:
		t.Status = StatusCancelled
		return clockEvent(KindCancelled, t, now), true
	}
	return Event{}, false
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// ---------------------------------------------------------------- station master

// Order is what a visitor asks for from the station master console.
type Order struct {
	Train    string `json:"train"`
	Action   string `json:"action"` // "delay", "platform" or "cancel"
	Minutes  int    `json:"minutes,omitempty"`
	Platform string `json:"platform,omitempty"`
}

var errOrder = errors.New("invalid order")

// StationMaster turns a visitor's order into an event, or explains why not.
func StationMaster(t Train, o Order, now time.Time) (Event, error) {
	if t.final() {
		return Event{}, fmt.Errorf("%w: le train %s %s est déjà parti ou supprimé", errOrder, t.Service, t.Number)
	}
	var kind Kind
	switch o.Action {
	case "delay":
		if o.Minutes != 5 && o.Minutes != 10 && o.Minutes != 15 {
			return Event{}, fmt.Errorf("%w: retard de 5, 10 ou 15 minutes", errOrder)
		}
		if t.Delay+o.Minutes > 90 {
			return Event{}, fmt.Errorf("%w: retard maximum de 90 minutes atteint", errOrder)
		}
		kind, t.Delay, t.Status = KindDelayed, t.Delay+o.Minutes, StatusDelayed
	case "platform":
		var n int
		if _, err := fmt.Sscan(o.Platform, &n); err != nil || n < 1 || n > platforms || fmt.Sprint(n) != o.Platform {
			return Event{}, fmt.Errorf("%w: voie de 1 à %d", errOrder, platforms)
		}
		if o.Platform == t.Platform {
			return Event{}, fmt.Errorf("%w: le train est déjà voie %s", errOrder, t.Platform)
		}
		kind, t.Platform = KindPlatform, o.Platform
	case "cancel":
		kind, t.Status = KindCancelled, StatusCancelled
	default:
		return Event{}, fmt.Errorf("%w: action inconnue", errOrder)
	}
	t.UpdatedAt = now
	return Event{Kind: kind, By: "station-master", Train: t}, nil
}

// ---------------------------------------------------------------- replay

// Frame is the board at one instant of a replay.
type Frame struct {
	At     time.Time `json:"at"`
	Rows   []Train   `json:"rows"`
	Events int       `json:"events"` // messages read since the previous frame
	LastID string    `json:"lastId,omitempty"`
}

// Frames rebuilds the board from scratch out of a replayed slice of the topic,
// and samples it every step: this is what a late subscriber sees.
func Frames(msgs []Delivery, from, to time.Time, step time.Duration) []Frame {
	board := NewBoard()
	var frames []Frame
	i := 0
	for at := from.Add(step); ; at = at.Add(step) {
		if at.After(to) {
			at = to
		}
		f := Frame{At: at}
		for ; i < len(msgs) && !msgs[i].PublishTime.After(at); i++ {
			board.Apply(msgs[i].Event)
			f.Events++
			f.LastID = msgs[i].ID
		}
		f.Rows = board.Rows(at)
		frames = append(frames, f)
		if !at.Before(to) {
			return frames
		}
	}
}
