package trafficledger

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const saveInterval = 30 * time.Second

type Config struct {
	Enabled  bool   `json:"enabled"`
	Day      int    `json:"day"`
	Hour     int    `json:"hour"`
	Minute   int    `json:"minute"`
	Timezone string `json:"timezone"`
}

type Snapshot struct {
	CycleID        string
	CycleStartedAt time.Time
	TotalUp        uint64
	TotalDown      uint64
}

type persistedState struct {
	Version        int       `json:"version"`
	Config         Config    `json:"config"`
	CycleID        string    `json:"cycle_id"`
	CycleStartedAt time.Time `json:"cycle_started_at"`
	TotalUp        uint64    `json:"total_up"`
	TotalDown      uint64    `json:"total_down"`
	LastRawUp      uint64    `json:"last_raw_up"`
	LastRawDown    uint64    `json:"last_raw_down"`
	HasRaw         bool      `json:"has_raw"`
}

type Ledger struct {
	mu        sync.Mutex
	path      string
	state     persistedState
	lastSaved time.Time
}

var global = newMemoryLedger(Config{})

func newMemoryLedger(config Config) *Ledger {
	now := time.Now()
	config = normalizeConfig(config)
	start := cycleStart(config, now)
	return &Ledger{state: persistedState{
		Version: 1, Config: config, CycleID: cycleID(config, start), CycleStartedAt: start,
	}}
}

func DefaultPath() string {
	if dir, err := os.UserConfigDir(); err == nil && dir != "" {
		return filepath.Join(dir, "monitor-agent", "traffic-ledger.json")
	}
	return "traffic-ledger.json"
}

func Initialize(path string, config Config) error {
	if path == "" {
		path = DefaultPath()
	}
	ledger, err := Open(path, config)
	if err != nil {
		return err
	}
	global = ledger
	return nil
}

func Open(path string, config Config) (*Ledger, error) {
	ledger := newMemoryLedger(config)
	ledger.path = path
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return ledger, ledger.saveLocked(time.Now())
	}
	if err := json.Unmarshal(data, &ledger.state); err != nil {
		_ = os.Rename(path, path+".bak")
		return ledger, ledger.saveLocked(time.Now())
	}
	ledger.state.Config = normalizeConfig(ledger.state.Config)
	if ledger.state.Version != 1 || ledger.state.CycleID == "" || ledger.state.CycleStartedAt.IsZero() {
		ledger = newMemoryLedger(config)
		ledger.path = path
		return ledger, ledger.saveLocked(time.Now())
	}
	return ledger, nil
}

func Observe(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	return global.Observe(rawUp, rawDown, now)
}

func SnapshotNow(now time.Time) (Snapshot, error) {
	return global.Snapshot(now)
}

func Configure(config Config, now time.Time) (Snapshot, error) {
	return global.Configure(config, now)
}

func Reset(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	return global.Reset(rawUp, rawDown, now)
}

func Close() error {
	global.mu.Lock()
	defer global.mu.Unlock()
	return global.saveLocked(time.Now())
}

func (ledger *Ledger) Observe(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()

	boundaryChanged := ledger.rotateLocked(now)
	if ledger.state.HasRaw && !boundaryChanged {
		ledger.state.TotalUp = saturatingAdd(ledger.state.TotalUp, counterDelta(rawUp, ledger.state.LastRawUp))
		ledger.state.TotalDown = saturatingAdd(ledger.state.TotalDown, counterDelta(rawDown, ledger.state.LastRawDown))
	}
	ledger.state.LastRawUp = rawUp
	ledger.state.LastRawDown = rawDown
	ledger.state.HasRaw = true

	if boundaryChanged || ledger.lastSaved.IsZero() || now.Sub(ledger.lastSaved) >= saveInterval {
		if err := ledger.saveLocked(now); err != nil {
			return ledger.snapshotLocked(), err
		}
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) Snapshot(now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.rotateLocked(now) {
		if err := ledger.saveLocked(now); err != nil {
			return ledger.snapshotLocked(), err
		}
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) Configure(config Config, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	config = normalizeConfig(config)
	if ledger.state.Config == config {
		return ledger.snapshotLocked(), nil
	}
	ledger.state.Config = config
	ledger.startCycleLocked(now)
	if err := ledger.saveLocked(now); err != nil {
		return ledger.snapshotLocked(), err
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) Reset(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.startCycleLocked(now)
	ledger.state.LastRawUp = rawUp
	ledger.state.LastRawDown = rawDown
	ledger.state.HasRaw = true
	if err := ledger.saveLocked(now); err != nil {
		return ledger.snapshotLocked(), err
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) rotateLocked(now time.Time) bool {
	if !ledger.state.Config.Enabled {
		return false
	}
	start := cycleStart(ledger.state.Config, now)
	id := cycleID(ledger.state.Config, start)
	if id == ledger.state.CycleID {
		return false
	}
	ledger.state.CycleID = id
	ledger.state.CycleStartedAt = start
	ledger.state.TotalUp = 0
	ledger.state.TotalDown = 0
	return true
}

func (ledger *Ledger) startCycleLocked(now time.Time) {
	start := now
	if ledger.state.Config.Enabled {
		start = cycleStart(ledger.state.Config, now)
	}
	ledger.state.CycleID = cycleID(ledger.state.Config, start)
	ledger.state.CycleStartedAt = start
	ledger.state.TotalUp = 0
	ledger.state.TotalDown = 0
}

func (ledger *Ledger) snapshotLocked() Snapshot {
	return Snapshot{
		CycleID: ledger.state.CycleID, CycleStartedAt: ledger.state.CycleStartedAt,
		TotalUp: ledger.state.TotalUp, TotalDown: ledger.state.TotalDown,
	}
}

func (ledger *Ledger) saveLocked(now time.Time) error {
	if ledger.path == "" {
		ledger.lastSaved = now
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(ledger.path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(ledger.state)
	if err != nil {
		return err
	}
	tmp := ledger.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, ledger.path); err != nil {
		return err
	}
	ledger.lastSaved = now
	return nil
}

func normalizeConfig(config Config) Config {
	if config.Day < 1 || config.Day > 31 {
		config.Day = 1
	}
	if config.Hour < 0 || config.Hour > 23 {
		config.Hour = 0
	}
	if config.Minute < 0 || config.Minute > 59 {
		config.Minute = 0
	}
	if config.Timezone == "" {
		config.Timezone = "Asia/Shanghai"
	}
	return config
}

func cycleStart(config Config, now time.Time) time.Time {
	if !config.Enabled {
		return now
	}
	loc, err := time.LoadLocation(config.Timezone)
	if err != nil {
		loc = time.FixedZone(config.Timezone, 8*60*60)
	}
	localNow := now.In(loc)
	this := monthlyBoundary(localNow.Year(), localNow.Month(), config, loc)
	if !localNow.Before(this) {
		return this
	}
	return monthlyBoundary(localNow.AddDate(0, -1, 0).Year(), localNow.AddDate(0, -1, 0).Month(), config, loc)
}

func monthlyBoundary(year int, month time.Month, config Config, loc *time.Location) time.Time {
	lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, loc).Day()
	day := config.Day
	if day > lastDay {
		day = lastDay
	}
	return time.Date(year, month, day, config.Hour, config.Minute, 0, 0, loc)
}

func cycleID(config Config, start time.Time) string {
	if !config.Enabled {
		return "manual:" + start.UTC().Format(time.RFC3339Nano)
	}
	return start.UTC().Format(time.RFC3339)
}

func counterDelta(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
