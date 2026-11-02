package main

// HTTP side: the page, the live stream (Server-Sent Events), the station
// master's orders, the replay, and the health routes.

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

//go:embed static
var staticFS embed.FS

// Link is the state of this instance's connection to Pulsar, shown on the board.
type Link struct {
	Configured bool      `json:"configured"`
	Live       bool      `json:"live"`
	Clock      bool      `json:"clock"` // this instance runs the station clock
	Rebuilt    int       `json:"rebuilt"`
	RebuiltMs  float64   `json:"rebuiltMs"`
	RebuiltAt  time.Time `json:"rebuiltAt,omitzero"`
	Retention  string    `json:"retention,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type App struct {
	cfg   Config
	log   *slog.Logger
	board *Board
	hub   *Hub
	bus   Bus // nil when no Pulsar add-on is linked
	page  *template.Template
	limit *limiter

	mu     sync.RWMutex
	link   Link
	recent []Delivery // newest first
	stats  *TopicStats

	replayMu sync.Mutex
	replay   *ReplayResult
}

func NewApp(cfg Config, log *slog.Logger, bus Bus) *App {
	a := &App{cfg: cfg, log: log, board: NewBoard(), hub: NewHub(), limit: newLimiter(), bus: bus}
	a.link.Configured = bus != nil
	a.page = template.Must(template.New("index.html").Funcs(template.FuncMap{
		"svg": func(name string) template.HTML {
			b, _ := staticFS.ReadFile("static/" + name)
			return template.HTML(b) // our own files, embedded at build time
		},
	}).ParseFS(staticFS, "static/index.html"))
	return a
}

// ---------------------------------------------------------------- state fed by Pulsar

func (a *App) rebuilt(msgs []Delivery, took time.Duration) {
	for _, d := range msgs {
		a.board.Apply(d.Event)
	}
	a.mu.Lock()
	a.link.Live, a.link.Error = true, ""
	a.link.Rebuilt, a.link.RebuiltMs, a.link.RebuiltAt = len(msgs), ms(took), time.Now()
	a.recent = a.recent[:0]
	for i := len(msgs) - 1; i >= 0 && len(a.recent) < 25; i-- {
		a.recent = append(a.recent, msgs[i])
	}
	a.mu.Unlock()
	a.log.Info("board rebuilt from the topic", "messages", len(msgs), "took", took.Round(time.Millisecond))
	a.hub.Broadcast("hello", a.snapshot())
}

func (a *App) received(d Delivery) {
	a.mu.Lock()
	for _, r := range a.recent {
		if r.ID == d.ID { // already read by the rebuild that ran while the subscription was starting
			a.mu.Unlock()
			return
		}
	}
	a.board.Apply(d.Event)
	a.link.Live, a.link.Error = true, ""
	a.recent = append([]Delivery{d}, a.recent...)
	if len(a.recent) > 25 {
		a.recent = a.recent[:25]
	}
	a.mu.Unlock()
	a.hub.Broadcast("msg", map[string]any{"msg": d, "rows": a.board.Rows(time.Now())})
}

func (a *App) setClock(on bool) { a.mu.Lock(); a.link.Clock = on; a.mu.Unlock() }
func (a *App) setRetention(r Retention) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.link.Retention = "illimitée"
	if r.Minutes > 0 {
		a.link.Retention = fmt.Sprintf("%d h", r.Minutes/60)
	}
}
func (a *App) setLinkError(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.link.Live = false
	if err != nil {
		a.link.Error = err.Error()
	}
}

type Snapshot struct {
	ServerTime time.Time   `json:"serverTime"`
	Topic      string      `json:"topic"`
	Board      string      `json:"subscription"`
	Archive    string      `json:"archive"`
	Link       Link        `json:"link"`
	Rows       []Train     `json:"rows"`
	Recent     []Delivery  `json:"recent"`
	Stats      *TopicStats `json:"stats"`
}

func (a *App) snapshot() Snapshot {
	now := time.Now()
	a.mu.RLock()
	defer a.mu.RUnlock()
	return Snapshot{
		ServerTime: now, Topic: a.cfg.Topic(), Board: a.cfg.BoardSubscription(), Archive: ArchiveSubscription,
		Link: a.link, Rows: a.board.Rows(now), Recent: append([]Delivery(nil), a.recent...), Stats: a.stats,
	}
}

// broadcastTicks refreshes the admin stats and the board (rows expire with time) every 5 s.
func (a *App) broadcastTicks(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if a.bus != nil {
			sctx, cancel := context.WithTimeout(ctx, 4*time.Second)
			s, err := a.bus.Stats(sctx)
			cancel()
			a.mu.Lock()
			if err == nil {
				a.stats = &s
			} else {
				a.log.Warn("admin stats", "err", err)
			}
			a.mu.Unlock()
		}
		a.hub.Broadcast("tick", a.snapshot())
	}
}

// ---------------------------------------------------------------- routes

func (a *App) Routes() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(staticFS, "static")
	files := http.FileServerFS(static)
	mux.HandleFunc("GET /{$}", a.handleIndex)
	mux.Handle("GET /cc-brand.css", files)
	mux.Handle("GET /app.css", files)
	mux.Handle("GET /app.js", files)
	mux.HandleFunc("GET /api/stream", a.handleStream)
	mux.HandleFunc("GET /api/board", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, a.snapshot()) })
	mux.HandleFunc("POST /api/orders", a.handleOrder)
	mux.HandleFunc("GET /api/replay", a.handleReplay)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /health/deep", a.handleDeepHealth)
	return securityHeaders(mux)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}

// PlatformVar is one of the variables Clever Cloud injects, shown in the frame.
type PlatformVar struct{ Label, Name, Value string }

func platformVars() []PlatformVar {
	vars := []PlatformVar{
		{"Application", "CC_APP_NAME", ""}, {"App ID", "APP_ID", ""},
		{"Instance", "INSTANCE_NUMBER", ""}, {"Type", "INSTANCE_TYPE", ""},
		{"Nom", "CC_PRETTY_INSTANCE_NAME", ""}, {"Commit", "CC_COMMIT_ID", ""},
		{"Déploiement", "CC_DEPLOYMENT_ID", ""},
	}
	for i := range vars {
		vars[i].Value = os.Getenv(vars[i].Name)
		if vars[i].Name == "CC_COMMIT_ID" && len(vars[i].Value) > 7 {
			vars[i].Value = vars[i].Value[:7]
		}
	}
	return vars
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := a.page.Execute(w, map[string]any{
		"Topic": a.cfg.Topic(), "Platform": platformVars(), "Instance": a.cfg.Instance,
		"Subscription": a.cfg.BoardSubscription(), "Archive": ArchiveSubscription,
	})
	if err != nil {
		a.log.Error("render", "err", err)
	}
}

func (a *App) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	ch := a.hub.Subscribe()
	defer a.hub.Unsubscribe(ch)
	_, _ = w.Write(sseFrame("hello", a.snapshot()))
	flusher.Flush()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case frame, open := <-ch:
			if !open {
				return
			}
			if _, err := w.Write(frame); err != nil {
				return
			}
		case <-ping.C:
			if _, err := w.Write([]byte(": ping\n\n")); err != nil {
				return
			}
		}
		flusher.Flush()
	}
}

func (a *App) handleOrder(w http.ResponseWriter, r *http.Request) {
	if a.bus == nil {
		writeError(w, http.StatusServiceUnavailable, "Aucun add-on Pulsar n'est lié à l'application.")
		return
	}
	var o Order
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2048)).Decode(&o); err != nil {
		writeError(w, http.StatusBadRequest, "Ordre illisible : JSON attendu.")
		return
	}
	t, ok := a.board.Get(o.Train)
	if !ok || !visible(t, time.Now()) {
		writeError(w, http.StatusNotFound, "Ce train n'est plus au tableau.")
		return
	}
	e, err := StationMaster(t, o, time.Now())
	if err != nil {
		writeError(w, http.StatusUnprocessableEntity, strings.TrimPrefix(err.Error(), errOrder.Error()+": "))
		return
	}
	if !a.limit.allow(clientIP(r), time.Now()) {
		writeError(w, http.StatusTooManyRequests, "Le poste est occupé : un ordre toutes les 2 secondes.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	receipt, err := a.bus.Publish(ctx, e)
	if err != nil {
		a.log.Warn("station master publish", "err", err)
		writeError(w, http.StatusBadGateway, "Pulsar n'a pas accepté le message : "+err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"receipt": receipt, "event": e, "topic": a.cfg.Topic()})
}

// ReplayResult is what a late subscriber gets: the recent past, rebuilt from the topic.
type ReplayResult struct {
	Subscription string    `json:"subscription"`
	From         time.Time `json:"from"`
	To           time.Time `json:"to"`
	Count        int       `json:"count"`
	ReadMs       float64   `json:"readMs"`
	First        *Delivery `json:"first,omitempty"` // oldest message still kept by Pulsar
	Frames       []Frame   `json:"frames"`
	made         time.Time
}

func (a *App) handleReplay(w http.ResponseWriter, r *http.Request) {
	if a.bus == nil {
		writeError(w, http.StatusServiceUnavailable, "Aucun add-on Pulsar n'est lié à l'application.")
		return
	}
	a.replayMu.Lock() // one replay at a time, shared for 20 s: it reads the topic again
	defer a.replayMu.Unlock()
	if a.replay != nil && time.Since(a.replay.made) < 20*time.Second {
		writeJSON(w, 200, a.replay)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	now := time.Now()
	start := time.Now()
	msgs, err := a.bus.Replay(ctx, now.Add(-replayWindow))
	if err != nil {
		writeError(w, http.StatusBadGateway, "Relecture impossible : "+err.Error())
		return
	}
	read := time.Since(start)
	first, _ := a.bus.FirstMessage(ctx)
	from := now.Add(-replayWindow)
	if len(msgs) > 0 && msgs[0].PublishTime.After(from) { // young topic: start at its first message
		from = msgs[0].PublishTime.Add(-time.Second)
	}
	step := max(now.Sub(from)/40, 3*time.Second)
	a.replay = &ReplayResult{
		Subscription: "late-reader-…", From: from, To: now, Count: len(msgs), ReadMs: ms(read),
		First: first, Frames: Frames(msgs, from, now, step), made: time.Now(),
	}
	writeJSON(w, 200, a.replay)
}

func (a *App) handleDeepHealth(w http.ResponseWriter, r *http.Request) {
	if a.bus == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "down", "pulsar": "no add-on linked"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := a.bus.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "down", "pulsar": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ok", "pulsar": "ok", "topic": a.cfg.Topic(), "ms": ms(time.Since(start))})
}

// ---------------------------------------------------------------- helpers

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return strings.TrimSpace(strings.Split(fwd, ",")[0])
	}
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

// limiter: one order every 2 s per visitor, 40 orders per minute for everyone.
type limiter struct {
	mu     sync.Mutex
	last   map[string]time.Time
	recent []time.Time
}

func newLimiter() *limiter { return &limiter{last: map[string]time.Time{}} }

func (l *limiter) allow(ip string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.last[ip]) < 2*time.Second {
		return false
	}
	for len(l.recent) > 0 && now.Sub(l.recent[0]) > time.Minute {
		l.recent = l.recent[1:]
	}
	if len(l.recent) >= 40 {
		return false
	}
	if len(l.last) > 10000 {
		clear(l.last)
	}
	l.recent = append(l.recent, now)
	l.last[ip] = now
	return true
}

// ---------------------------------------------------------------- SSE hub

type Hub struct {
	mu      sync.Mutex
	clients map[chan []byte]struct{}
}

func NewHub() *Hub { return &Hub{clients: map[chan []byte]struct{}{}} }

func (h *Hub) Subscribe() chan []byte {
	ch := make(chan []byte, 32)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) Unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[ch]; ok {
		delete(h.clients, ch)
		close(ch)
	}
}

func (h *Hub) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		delete(h.clients, ch)
		close(ch)
	}
}

// Broadcast sends an event to every browser; a browser that lags behind skips it.
func (h *Hub) Broadcast(event string, v any) {
	frame := sseFrame(event, v)
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- frame:
		default:
		}
	}
}

func sseFrame(event string, v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		data, _ = json.Marshal(map[string]string{"error": err.Error()})
	}
	return []byte("event: " + event + "\ndata: " + string(data) + "\n\n")
}
