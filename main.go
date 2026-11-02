// demo-pulsar — "Departures": a split-flap departures board fed by Apache Pulsar.
//
//	station clock (simulator) ──┐                                    ┌──► board-<instance>  (live board, SSE to browsers)
//	                            ├──► persistent://<tenant>/<ns>/departures ──► archives   (durable, acks in batches)
//	station master (visitors) ──┘                                    └──► late-reader-… (replay on demand)
package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const (
	TopicName           = "departures"
	ArchiveSubscription = "archives"
	rebuildWindow       = 2 * time.Hour    // the board rebuilds itself from the last 2 hours
	replayWindow        = 30 * time.Minute // what the "late subscriber" replays on screen
	archiveEvery        = 45 * time.Second
)

// Config comes from the variables the Pulsar add-on injects (never copied elsewhere).
type Config struct {
	Port      string
	BinaryURL string // ADDON_PULSAR_BINARY_URL
	HTTPURL   string // ADDON_PULSAR_HTTP_URL, admin REST API
	Token     string // ADDON_PULSAR_TOKEN
	Tenant    string // ADDON_PULSAR_TENANT
	Namespace string // ADDON_PULSAR_NAMESPACE
	Instance  string // names this instance's producers and subscription
}

func loadConfig() Config {
	return Config{
		Port:      envOr("PORT", "8080"),
		BinaryURL: os.Getenv("ADDON_PULSAR_BINARY_URL"),
		HTTPURL:   os.Getenv("ADDON_PULSAR_HTTP_URL"),
		Token:     os.Getenv("ADDON_PULSAR_TOKEN"),
		Tenant:    envOr("ADDON_PULSAR_TENANT", "public"),
		Namespace: envOr("ADDON_PULSAR_NAMESPACE", "default"),
		Instance:  instanceName(),
	}
}

func (c Config) Topic() string { return c.TopicNamed(TopicName) }
func (c Config) TopicNamed(name string) string {
	return "persistent://" + c.Tenant + "/" + c.Namespace + "/" + name
}
func (c Config) BoardSubscription() string { return "board-" + c.Instance }

// instanceName is short, readable and unique: "fluffy-wolf-3f2a" on Clever Cloud, "local-3f2a" elsewhere.
func instanceName() string {
	name := strings.ToLower(strings.Join(strings.Fields(os.Getenv("CC_PRETTY_INSTANCE_NAME")), "-"))
	if name == "" {
		name = "local"
	}
	return name + "-" + randomSuffix()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	var bus *PulsarBus
	if cfg.BinaryURL != "" {
		var err error
		if bus, err = NewPulsarBus(cfg, log); err != nil {
			log.Error("pulsar client", "err", err)
			os.Exit(1)
		}
	} else {
		log.Warn("ADDON_PULSAR_BINARY_URL is not set: the board stays empty (link a Pulsar add-on)")
	}

	var app *App
	if bus != nil {
		app = NewApp(cfg, log, bus)
		go runStation(ctx, app, bus)
	} else {
		app = NewApp(cfg, log, nil)
	}
	go app.broadcastTicks(ctx)

	srv := &http.Server{Addr: ":" + cfg.Port, Handler: app.Routes(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		log.Info("stopping: closing browser streams and Pulsar client")
		app.hub.CloseAll()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	log.Info("departures board listening", "port", cfg.Port, "topic", cfg.Topic(), "instance", cfg.Instance)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server", "err", err)
		os.Exit(1)
	}
	if bus != nil {
		bus.Close() // releases the station clock: the next instance in line takes it
	}
	log.Info("stopped")
}

// runStation starts the three Pulsar loops; each one retries on its own.
func runStation(ctx context.Context, app *App, bus *PulsarBus) {
	if r, err := bus.EnsureRetention(ctx, Retention{Minutes: 24 * 60, SizeMB: 256}); err != nil {
		app.log.Warn("retention policy", "err", err)
	} else {
		app.setRetention(r)
	}
	ready := make(chan struct{})
	go retry(ctx, app, "board", func() error {
		return bus.Follow(ctx, time.Now().Add(-rebuildWindow), func(msgs []Delivery, took time.Duration) {
			app.rebuilt(msgs, took)
			select {
			case <-ready:
			default:
				close(ready)
			}
		}, app.received)
	})
	go retry(ctx, app, "archive", func() error { return bus.Archive(ctx, archiveEvery) })
	go func() {
		select { // the clock only starts once the board knows the current state
		case <-ready:
		case <-ctx.Done():
			return
		}
		retry(ctx, app, "clock", func() error { return bus.Lead(ctx, app.runClock) })
	}()
}

func retry(ctx context.Context, app *App, name string, loop func() error) {
	for ctx.Err() == nil {
		err := loop()
		if ctx.Err() != nil {
			return
		}
		app.log.Warn("pulsar loop stopped, retrying in 5 s", "loop", name, "err", err)
		app.setLinkError(err)
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
		}
	}
}

// runClock is the station simulator. It reads the board (built from Pulsar)
// and publishes what happens next. It runs on one instance only (see Lead).
func (a *App) runClock(ctx context.Context, publish func(context.Context, Event) (Receipt, error)) {
	a.setClock(true)
	defer a.setClock(false)
	a.log.Info("this instance now runs the station clock (exclusive producer acquired)")
	rnd := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))
	emit := func(events []Event) {
		for _, e := range events {
			if _, err := publish(ctx, e); err != nil {
				a.log.Warn("clock publish", "err", err)
				return
			}
		}
	}
	if upcoming(a.board.Rows(time.Now())) == 0 { // empty topic: open the station
		emit(Seed(time.Now(), rnd))
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(2500+rnd.IntN(2500)) * time.Millisecond):
			emit(Plan(a.board.Rows(time.Now()), time.Now(), rnd, true))
		}
	}
}

func upcoming(rows []Train) int {
	n := 0
	for _, t := range rows {
		if !t.final() {
			n++
		}
	}
	return n
}
