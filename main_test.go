package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 1, 21, 0, 0, 0, time.UTC)

func train(id string, sched time.Time, st Status, at time.Time) Train {
	return Train{ID: id, Service: "IC", Number: "4512", Destination: "LYON PART-DIEU", Scheduled: sched, Status: st, UpdatedAt: at}
}

func TestBoardKeepsTheNewestVersion(t *testing.T) {
	b := NewBoard()
	newer := train("A", t0.Add(10*time.Minute), StatusDelayed, t0.Add(2*time.Second))
	older := train("A", t0.Add(10*time.Minute), StatusOnTime, t0.Add(time.Second))
	if !b.Apply(Event{Train: newer}) {
		t.Fatal("first event should apply")
	}
	if b.Apply(Event{Train: older}) || b.Apply(Event{Train: newer}) {
		t.Fatal("older or duplicate events must be ignored")
	}
	if got, _ := b.Get("A"); got.Status != StatusDelayed {
		t.Fatalf("status = %s, want delayed", got.Status)
	}
}

func TestRowsOrderAndExpiry(t *testing.T) {
	b := NewBoard()
	b.Apply(Event{Train: train("late", t0.Add(9*time.Minute), StatusOnTime, t0)})
	b.Apply(Event{Train: train("early", t0.Add(3*time.Minute), StatusOnTime, t0)})
	b.Apply(Event{Train: train("gone", t0, StatusDeparted, t0.Add(-2*time.Minute))})
	rows := b.Rows(t0)
	if len(rows) != 2 || rows[0].ID != "early" || rows[1].ID != "late" {
		t.Fatalf("rows = %+v", rows)
	}
}

func TestSeedOpensTheStation(t *testing.T) {
	events := Seed(t0, rand.New(rand.NewPCG(1, 2)))
	b := NewBoard()
	for _, e := range events {
		if e.By != "clock" {
			t.Fatalf("seed event by %q", e.By)
		}
		b.Apply(e)
	}
	rows := b.Rows(t0)
	if len(rows) != targetUpcoming {
		t.Fatalf("seeded %d trains, want %d", len(rows), targetUpcoming)
	}
	withPlatform := 0
	for _, r := range rows {
		if r.Platform != "" {
			withPlatform++
		}
	}
	if withPlatform == 0 {
		t.Fatal("the first trains should already have a platform")
	}
	perMinute := map[time.Time]int{}
	for _, r := range rows {
		if perMinute[r.Scheduled]++; perMinute[r.Scheduled] > 2 {
			t.Fatalf("more than two departures at %s", r.Scheduled.Format("15:04"))
		}
	}
}

func TestPlanMovesTrainsAlong(t *testing.T) {
	rnd := rand.New(rand.NewPCG(3, 4))
	rows := []Train{
		train("due", t0.Add(-time.Second), StatusBoarding, t0),
		train("soon", t0.Add(80*time.Second), StatusOnTime, t0),
	}
	rows[1].Platform = "4"
	kinds := map[string]Kind{}
	for _, e := range Plan(rows, t0, rnd, false) {
		kinds[e.Train.ID] = e.Kind
	}
	if kinds["due"] != KindDeparted || kinds["soon"] != KindBoarding {
		t.Fatalf("plan = %v", kinds)
	}
}

func TestStationMasterOrders(t *testing.T) {
	tr := train("A", t0.Add(10*time.Minute), StatusBoarding, t0)
	e, err := StationMaster(tr, Order{Action: "delay", Minutes: 10}, t0)
	if err != nil || e.Train.Delay != 10 || e.Train.Status != StatusDelayed || e.By != "station-master" {
		t.Fatalf("delay: %+v %v", e, err)
	}
	for _, o := range []Order{{Action: "delay", Minutes: 7}, {Action: "platform", Platform: "13"}, {Action: "platform", Platform: "04"}, {Action: "fly"}} {
		if _, err := StationMaster(tr, o, t0); !errors.Is(err, errOrder) {
			t.Fatalf("order %+v should be refused", o)
		}
	}
	tr.Status = StatusDeparted
	if _, err := StationMaster(tr, Order{Action: "cancel"}, t0); err == nil {
		t.Fatal("a departed train cannot be cancelled")
	}
}

func TestFramesRebuildTheBoardFromScratch(t *testing.T) {
	msgs := []Delivery{
		{ID: "1:0:0", PublishTime: t0.Add(10 * time.Second), Event: Event{Train: train("A", t0.Add(5*time.Minute), StatusOnTime, t0.Add(10*time.Second))}},
		{ID: "1:1:0", PublishTime: t0.Add(70 * time.Second), Event: Event{Train: train("B", t0.Add(6*time.Minute), StatusOnTime, t0.Add(70*time.Second))}},
	}
	frames := Frames(msgs, t0, t0.Add(2*time.Minute), time.Minute)
	if len(frames) != 2 || len(frames[0].Rows) != 1 || len(frames[1].Rows) != 2 || frames[1].LastID != "1:1:0" {
		t.Fatalf("frames = %+v", frames)
	}
}

func TestLimiter(t *testing.T) {
	l := newLimiter()
	if !l.allow("1.2.3.4", t0) || l.allow("1.2.3.4", t0.Add(time.Second)) || !l.allow("1.2.3.4", t0.Add(3*time.Second)) {
		t.Fatal("one order every 2 s per visitor")
	}
}

// ---------------------------------------------------------------- HTTP, with a fake bus

type fakeBus struct {
	published []Event
	down      bool
}

func (f *fakeBus) Publish(_ context.Context, e Event) (Receipt, error) {
	f.published = append(f.published, e)
	return Receipt{ID: "42:7:0", Producer: "station-master-test"}, nil
}
func (f *fakeBus) Replay(context.Context, time.Time) ([]Delivery, error) { return nil, nil }
func (f *fakeBus) FirstMessage(context.Context) (*Delivery, error)       { return nil, nil }
func (f *fakeBus) Stats(context.Context) (TopicStats, error)             { return TopicStats{}, nil }
func (f *fakeBus) Ping(context.Context) error {
	if f.down {
		return errors.New("broker unreachable")
	}
	return nil
}

func testApp(t *testing.T, bus Bus) (*App, *httptest.Server) {
	t.Helper()
	cfg := Config{Tenant: "orga_x", Namespace: "pulsar_y", Instance: "test-0000"}
	app := NewApp(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), bus)
	srv := httptest.NewServer(app.Routes())
	t.Cleanup(srv.Close)
	return app, srv
}

func TestHealthRoutes(t *testing.T) {
	bus := &fakeBus{}
	_, srv := testApp(t, bus)
	for path, want := range map[string]int{"/health": 200, "/health/deep": 200} {
		if res, _ := http.Get(srv.URL + path); res.StatusCode != want {
			t.Fatalf("%s = %d", path, res.StatusCode)
		}
	}
	bus.down = true
	if res, _ := http.Get(srv.URL + "/health/deep"); res.StatusCode != 503 {
		t.Fatalf("deep health with Pulsar down = %d", res.StatusCode)
	}
	if res, _ := http.Get(srv.URL + "/health"); res.StatusCode != 200 {
		t.Fatal("/health must not depend on Pulsar")
	}
}

func TestOrderIsPublished(t *testing.T) {
	bus := &fakeBus{}
	app, srv := testApp(t, bus)
	now := time.Now()
	app.board.Apply(Event{Train: train("IC4512-x", now.Add(10*time.Minute), StatusOnTime, now)})

	post := func(body string, ip string) *http.Response {
		req, _ := http.NewRequest("POST", srv.URL+"/api/orders", strings.NewReader(body))
		req.Header.Set("X-Forwarded-For", ip)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := post(`{"train":"IC4512-x","action":"delay","minutes":15}`, "10.0.0.1")
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("order = %d", res.StatusCode)
	}
	var out struct{ Receipt Receipt }
	_ = json.NewDecoder(res.Body).Decode(&out)
	if out.Receipt.ID != "42:7:0" || len(bus.published) != 1 || bus.published[0].Train.Delay != 15 {
		t.Fatalf("receipt %+v, published %+v", out.Receipt, bus.published)
	}
	if res := post(`{"train":"IC4512-x","action":"cancel"}`, "10.0.0.1"); res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second order within 2 s = %d", res.StatusCode)
	}
	if res := post(`{"train":"nope","action":"cancel"}`, "10.0.0.2"); res.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown train = %d", res.StatusCode)
	}
	if res := post(`{"train":"IC4512-x","action":"delay","minutes":60}`, "10.0.0.3"); res.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("invalid delay = %d", res.StatusCode)
	}
}

func TestPageShowsOnlyPlatformVariables(t *testing.T) {
	t.Setenv("CC_APP_NAME", "demo-pulsar")
	t.Setenv("CC_COMMIT_ID", "0123456789abcdef")
	t.Setenv("ADDON_PULSAR_TOKEN", "do-not-print-me")
	_, srv := testApp(t, &fakeBus{})
	res, _ := http.Get(srv.URL + "/")
	body, _ := io.ReadAll(res.Body)
	page := string(body)
	if !strings.Contains(page, "demo-pulsar") || !strings.Contains(page, "0123456") || strings.Contains(page, "0123456789") {
		t.Fatal("platform panel should show the app name and a short commit")
	}
	if strings.Contains(page, "do-not-print-me") {
		t.Fatal("the Pulsar token must never reach the page")
	}
	if !strings.Contains(res.Header.Get("Content-Security-Policy"), "script-src 'self'") {
		t.Fatal("missing CSP")
	}
}

func TestMessageReadTwiceIsShownOnce(t *testing.T) {
	app, _ := testApp(t, &fakeBus{})
	d := Delivery{ID: "9:1:0", PublishTime: time.Now(), Event: Event{Train: train("A", time.Now().Add(5*time.Minute), StatusOnTime, time.Now())}}
	app.rebuilt([]Delivery{d}, time.Millisecond)
	app.received(d)
	if got := app.snapshot().Recent; len(got) != 1 {
		t.Fatalf("recent = %d messages, want 1", len(got))
	}
}
