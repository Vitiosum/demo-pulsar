package main

// Everything that talks to Apache Pulsar lives here: one client, two producers
// on the departures topic, two subscriptions, an on-demand reader, an exclusive
// producer used as a lock, and a few calls to the admin REST API.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	plog "github.com/apache/pulsar-client-go/pulsar/log"
)

// Delivery is one message read from the topic.
type Delivery struct {
	ID          string    `json:"id"` // ledger:entry:partition
	Producer    string    `json:"producer"`
	PublishTime time.Time `json:"publishTime"`
	LatencyMs   float64   `json:"latencyMs,omitempty"` // publish → received by the board
	Event       Event     `json:"event"`
}

// Receipt is what the broker answers when it has stored a message.
type Receipt struct {
	ID       string    `json:"id"`
	Producer string    `json:"producer"`
	SentAt   time.Time `json:"sentAt"`
	AckMs    float64   `json:"ackMs"`
}

// TopicStats is the part of the admin API's topic stats shown in the signal box.
type TopicStats struct {
	MsgInCounter  int64               `json:"msgInCounter"`
	MsgRateIn     float64             `json:"msgRateIn"`
	StorageSize   int64               `json:"storageSize"`
	Subscriptions map[string]SubStats `json:"subscriptions"`
}

type SubStats struct {
	MsgBacklog int64      `json:"msgBacklog"`
	Type       string     `json:"type"`
	IsDurable  bool       `json:"isDurable"`
	MsgRateOut float64    `json:"msgRateOut"`
	Consumers  []struct{} `json:"consumers"`
}

// Bus is what the HTTP handlers need from Pulsar (a fake implements it in tests).
type Bus interface {
	Publish(ctx context.Context, e Event) (Receipt, error)
	Replay(ctx context.Context, since time.Time) ([]Delivery, error)
	FirstMessage(ctx context.Context) (*Delivery, error)
	Stats(ctx context.Context) (TopicStats, error)
	Ping(ctx context.Context) error
}

type PulsarBus struct {
	cfg    Config
	log    *slog.Logger
	client pulsar.Client
	topic  string
	admin  *http.Client

	mu     sync.Mutex
	master pulsar.Producer // the visitors' producer, created on first use
}

func NewPulsarBus(cfg Config, log *slog.Logger) (*PulsarBus, error) {
	quiet := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})) // the client is chatty
	opts := pulsar.ClientOptions{
		URL:               cfg.BinaryURL,
		ConnectionTimeout: 10 * time.Second,
		OperationTimeout:  15 * time.Second,
		Logger:            plog.NewLoggerWithSlog(quiet.With("lib", "pulsar")),
	}
	if cfg.Token != "" {
		opts.Authentication = pulsar.NewAuthenticationToken(cfg.Token)
	}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &PulsarBus{cfg: cfg, log: log, client: client, topic: cfg.Topic(), admin: &http.Client{Timeout: 5 * time.Second}}, nil
}

func (b *PulsarBus) Close() { b.client.Close() }

// ---------------------------------------------------------------- producers

func (b *PulsarBus) Publish(ctx context.Context, e Event) (Receipt, error) {
	b.mu.Lock()
	if b.master == nil {
		p, err := b.client.CreateProducer(pulsar.ProducerOptions{Topic: b.topic, Name: "station-master-" + b.cfg.Instance, DisableBatching: true})
		if err != nil {
			b.mu.Unlock()
			return Receipt{}, err
		}
		b.master = p
	}
	p := b.master
	b.mu.Unlock()
	return send(ctx, p, e)
}

func send(ctx context.Context, p pulsar.Producer, e Event) (Receipt, error) {
	e.Train.UpdatedAt = time.Now() // the newest version of a row wins on the board
	payload, err := json.Marshal(e)
	if err != nil {
		return Receipt{}, err
	}
	start := time.Now()
	id, err := p.Send(ctx, &pulsar.ProducerMessage{
		Payload:    payload,
		Key:        e.Train.ID, // same key, same order
		EventTime:  e.Train.UpdatedAt,
		Properties: map[string]string{"kind": string(e.Kind), "by": e.By},
	})
	if err != nil {
		return Receipt{}, err
	}
	return Receipt{ID: id.String(), Producer: p.Name(), SentAt: start, AckMs: ms(time.Since(start))}, nil
}

// Lead runs the station clock, but only on one instance at a time: the
// instance holding the exclusive producer of the "station-clock" topic.
// The others wait in line (WaitForExclusive) and take over if it goes away.
func (b *PulsarBus) Lead(ctx context.Context, run func(ctx context.Context, publish func(context.Context, Event) (Receipt, error))) error {
	lock, err := b.client.CreateProducer(pulsar.ProducerOptions{
		Topic:              b.cfg.TopicNamed("station-clock"),
		Name:               "clock-lock-" + b.cfg.Instance,
		ProducerAccessMode: pulsar.ProducerAccessModeWaitForExclusive,
	})
	if err != nil {
		return err
	}
	defer lock.Close()
	clock, err := b.client.CreateProducer(pulsar.ProducerOptions{Topic: b.topic, Name: "station-clock-" + b.cfg.Instance, DisableBatching: true})
	if err != nil {
		return err
	}
	defer clock.Close()
	run(ctx, func(ctx context.Context, e Event) (Receipt, error) { return send(ctx, clock, e) })
	return ctx.Err()
}

// ---------------------------------------------------------------- consumers

// Follow feeds the board. It subscribes first (so nothing is missed), then
// rebuilds the board by replaying the recent past with a reader, then hands
// every new message to live. Duplicates between the two are ignored by Board.Apply.
func (b *PulsarBus) Follow(ctx context.Context, since time.Time, rebuilt func([]Delivery, time.Duration), live func(Delivery)) error {
	consumer, err := b.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       b.topic,
		SubscriptionName:            b.cfg.BoardSubscription(),
		Type:                        pulsar.Exclusive,
		SubscriptionMode:            pulsar.NonDurable, // the board rebuilds itself, no cursor to keep
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	start := time.Now()
	msgs, err := b.Replay(ctx, since)
	if err != nil {
		return err
	}
	rebuilt(msgs, time.Since(start))

	for {
		m, err := consumer.Receive(ctx)
		if err != nil {
			return err
		}
		consumer.Ack(m)
		if d, ok := toDelivery(m); ok {
			d.LatencyMs = ms(time.Since(m.PublishTime()))
			live(d)
		}
	}
}

// Archive is a slow, durable subscriber: it receives everything but only
// acknowledges once per period, so its backlog visibly grows and drains.
// Its cursor is stored by Pulsar: after a restart it resumes where it stopped.
func (b *PulsarBus) Archive(ctx context.Context, every time.Duration) error {
	consumer, err := b.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            b.topic,
		SubscriptionName: ArchiveSubscription,
		Type:             pulsar.Failover, // one active archivist, the others on standby
	})
	if err != nil {
		return err
	}
	defer consumer.Close()
	tick := time.NewTicker(every)
	defer tick.Stop()
	var last pulsar.Message
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case cm := <-consumer.Chan():
			last = cm.Message
		case <-tick.C:
			if last != nil {
				if err := consumer.AckCumulative(last); err != nil {
					b.log.Warn("archive ack", "err", err)
				}
				last = nil
			}
		}
	}
}

// Replay reads the topic from a point in time to its current end, with a
// reader: a non-durable subscription that Pulsar forgets once it is closed.
func (b *PulsarBus) Replay(ctx context.Context, since time.Time) ([]Delivery, error) {
	reader, err := b.client.CreateReader(pulsar.ReaderOptions{
		Topic:             b.topic,
		Name:              "late-reader",
		SubscriptionName:  "late-reader-" + randomSuffix(),
		StartMessageID:    pulsar.LatestMessageID(),
		ReceiverQueueSize: 200,
	})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if err := reader.SeekByTime(since); err != nil {
		return nil, err
	}
	var out []Delivery
	for len(out) < 5000 && hasNext(reader) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := reader.Next(rctx)
		cancel()
		if err != nil {
			return out, err
		}
		if d, ok := toDelivery(m); ok {
			out = append(out, d)
		}
	}
	return out, nil
}

// FirstMessage returns the oldest message Pulsar still keeps on the topic.
func (b *PulsarBus) FirstMessage(ctx context.Context) (*Delivery, error) {
	reader, err := b.client.CreateReader(pulsar.ReaderOptions{
		Topic:             b.topic,
		Name:              "first-message",
		StartMessageID:    pulsar.EarliestMessageID(),
		ReceiverQueueSize: 1,
	})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	if !reader.HasNext() {
		return nil, nil
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	m, err := reader.Next(rctx)
	if err != nil {
		return nil, err
	}
	d, _ := toDelivery(m)
	return &d, nil
}

// hasNext gives a reader that has just been repositioned a moment to settle.
func hasNext(r pulsar.Reader) bool {
	if r.HasNext() {
		return true
	}
	time.Sleep(300 * time.Millisecond)
	return r.HasNext()
}

func toDelivery(m pulsar.Message) (Delivery, bool) {
	var e Event
	if err := json.Unmarshal(m.Payload(), &e); err != nil || e.Train.ID == "" {
		return Delivery{}, false
	}
	return Delivery{ID: m.ID().String(), Producer: m.ProducerName(), PublishTime: m.PublishTime(), Event: e}, true
}

// ---------------------------------------------------------------- admin REST API

func (b *PulsarBus) Stats(ctx context.Context) (TopicStats, error) {
	var s TopicStats
	path := fmt.Sprintf("/admin/v2/persistent/%s/%s/%s/partitioned-stats", b.cfg.Tenant, b.cfg.Namespace, TopicName)
	err := b.adminCall(ctx, http.MethodGet, path, nil, &s)
	return s, err
}

// Ping checks both the binary protocol (topic lookup) and the admin API.
func (b *PulsarBus) Ping(ctx context.Context) error {
	if _, err := b.client.TopicPartitions(b.topic); err != nil {
		return fmt.Errorf("broker lookup: %w", err)
	}
	if _, err := b.Stats(ctx); err != nil {
		return fmt.Errorf("admin API: %w", err)
	}
	return nil
}

// Retention is the namespace retention policy (-1 means unlimited).
type Retention struct {
	Minutes int `json:"retentionTimeInMinutes"`
	SizeMB  int `json:"retentionSizeInMB"`
}

// EnsureRetention keeps one day of history on the namespace: enough to replay
// the day, small enough to stay in the free storage tier.
func (b *PulsarBus) EnsureRetention(ctx context.Context, want Retention) (Retention, error) {
	path := fmt.Sprintf("/admin/v2/namespaces/%s/%s/retention", b.cfg.Tenant, b.cfg.Namespace)
	var cur Retention
	if err := b.adminCall(ctx, http.MethodGet, path, nil, &cur); err != nil {
		return cur, err
	}
	if cur == want {
		return cur, nil
	}
	if err := b.adminCall(ctx, http.MethodPost, path, want, nil); err != nil {
		return cur, err
	}
	return want, nil
}

func (b *PulsarBus) adminCall(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, _ := json.Marshal(in)
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(b.cfg.HTTPURL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+b.cfg.Token)
	}
	resp, err := b.admin.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, msg)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func randomSuffix() string {
	b := make([]byte, 2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
