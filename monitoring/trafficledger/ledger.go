package trafficledger

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	saveInterval                 = 30 * time.Second
	resetOperationRetention      = 5 * time.Minute
	maxRememberedResetOperations = 128
)

type Snapshot struct {
	CycleID         string
	CycleStartedAt  time.Time
	LedgerEpoch     string
	CycleGeneration uint64
	SampleSequence  uint64
	TotalUp         uint64
	TotalDown       uint64
}

type InterfaceCounter struct {
	Up   uint64
	Down uint64
}

type persistedInterfaceCounter struct {
	Up   uint64 `json:"up"`
	Down uint64 `json:"down"`
}

type persistedResetOperation struct {
	Snapshot  Snapshot  `json:"snapshot"`
	ExpiresAt time.Time `json:"expires_at"`
}

type persistedState struct {
	Version         int                                  `json:"version"`
	LedgerEpoch     string                               `json:"ledger_epoch"`
	CycleID         string                               `json:"cycle_id"`
	CycleStartedAt  time.Time                            `json:"cycle_started_at"`
	CycleGeneration uint64                               `json:"cycle_generation"`
	SampleSequence  uint64                               `json:"sample_sequence"`
	TotalUp         uint64                               `json:"total_up"`
	TotalDown       uint64                               `json:"total_down"`
	LastRawUp       uint64                               `json:"last_raw_up"`
	LastRawDown     uint64                               `json:"last_raw_down"`
	HasRaw          bool                                 `json:"has_raw"`
	Interfaces      map[string]persistedInterfaceCounter `json:"interfaces,omitempty"`
	LastObservedAt  time.Time                            `json:"last_observed_at,omitempty"`
	ResetOperations map[string]persistedResetOperation   `json:"reset_operations,omitempty"`
}

type Ledger struct {
	mu        sync.Mutex
	path      string
	state     persistedState
	lastSaved time.Time
}

var global = newMemoryLedger()

func newMemoryLedger() *Ledger {
	now := time.Now()
	return &Ledger{state: persistedState{
		Version: 1, LedgerEpoch: newLedgerEpoch(), CycleID: manualCycleID(now), CycleStartedAt: now, CycleGeneration: 1,
	}}
}

func DefaultPath() string {
	if dir, err := os.UserConfigDir(); err == nil && dir != "" {
		return filepath.Join(dir, "monitor-agent", "traffic-ledger.json")
	}
	return "traffic-ledger.json"
}

func Initialize(path string) error {
	if path == "" {
		path = DefaultPath()
	}
	ledger, err := Open(path)
	if err != nil {
		return err
	}
	global = ledger
	return nil
}

func Open(path string) (*Ledger, error) {
	ledger := newMemoryLedger()
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
	if ledger.state.CycleGeneration == 0 {
		ledger.state.CycleGeneration = 1
	}
	if ledger.state.Version != 1 || ledger.state.CycleID == "" || ledger.state.CycleStartedAt.IsZero() {
		ledger = newMemoryLedger()
		ledger.path = path
		return ledger, ledger.saveLocked(time.Now())
	}
	// The persisted totals survive process restarts, but the report epoch must
	// change so a reset sample sequence cannot be confused with an older process.
	ledger.state.LedgerEpoch = newLedgerEpoch()
	if err := ledger.saveLocked(time.Now()); err != nil {
		return nil, err
	}
	return ledger, nil
}

func Observe(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	return global.Observe(rawUp, rawDown, now)
}

func ObserveInterfaces(counters map[string]InterfaceCounter, now time.Time) (Snapshot, error) {
	return global.ObserveInterfaces(counters, now)
}

func SnapshotNow(now time.Time) (Snapshot, error) {
	return global.Snapshot(now)
}

func Reset(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	return global.Reset(rawUp, rawDown, now)
}

func ResetInterfaces(counters map[string]InterfaceCounter, now time.Time) (Snapshot, error) {
	return global.ResetInterfaces(counters, now)
}

func ResetInterfacesForOperation(counters map[string]InterfaceCounter, operationID string, now time.Time) (Snapshot, error) {
	return global.ResetInterfacesForOperation(counters, operationID, now)
}

func Close() error {
	global.mu.Lock()
	defer global.mu.Unlock()
	return global.saveLocked(time.Now())
}

func (ledger *Ledger) Observe(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()

	now = ledger.monotonicObservationTimeLocked(now)
	ledger.advanceSampleSequenceLocked()
	if ledger.state.HasRaw {
		ledger.state.TotalUp = saturatingAdd(ledger.state.TotalUp, counterDelta(rawUp, ledger.state.LastRawUp))
		ledger.state.TotalDown = saturatingAdd(ledger.state.TotalDown, counterDelta(rawDown, ledger.state.LastRawDown))
	}
	ledger.state.LastRawUp = rawUp
	ledger.state.LastRawDown = rawDown
	ledger.state.HasRaw = true

	if ledger.lastSaved.IsZero() || now.Sub(ledger.lastSaved) >= saveInterval {
		if err := ledger.saveLocked(now); err != nil {
			return ledger.snapshotLocked(), err
		}
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) ObserveInterfaces(counters map[string]InterfaceCounter, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()

	now = ledger.monotonicObservationTimeLocked(now)
	ledger.advanceSampleSequenceLocked()
	current := make(map[string]persistedInterfaceCounter, len(counters))
	var rawUp, rawDown uint64
	for name, counter := range counters {
		if name == "" {
			continue
		}
		current[name] = persistedInterfaceCounter{Up: counter.Up, Down: counter.Down}
		rawUp = saturatingAdd(rawUp, counter.Up)
		rawDown = saturatingAdd(rawDown, counter.Down)
	}

	if ledger.state.Interfaces == nil {
		// Older ledger files only had aggregate counters. Preserve their
		// one-time delta before switching to per-interface baselines.
		if ledger.state.HasRaw {
			ledger.state.TotalUp = saturatingAdd(ledger.state.TotalUp, counterDelta(rawUp, ledger.state.LastRawUp))
			ledger.state.TotalDown = saturatingAdd(ledger.state.TotalDown, counterDelta(rawDown, ledger.state.LastRawDown))
		}
	} else {
		for name, counter := range current {
			previous, exists := ledger.state.Interfaces[name]
			if !exists {
				continue
			}
			ledger.state.TotalUp = saturatingAdd(ledger.state.TotalUp, counterDelta(counter.Up, previous.Up))
			ledger.state.TotalDown = saturatingAdd(ledger.state.TotalDown, counterDelta(counter.Down, previous.Down))
		}
	}

	if ledger.state.Interfaces != nil {
		// Keep the last baseline for temporarily missing interfaces. If the
		// interface returns with a monotonic counter, the unseen interval is
		// still real traffic; if the counter reset, counterDelta returns zero.
		for name, previous := range ledger.state.Interfaces {
			if _, exists := current[name]; !exists {
				current[name] = previous
			}
		}
	}
	ledger.state.Interfaces = current
	ledger.state.LastRawUp = rawUp
	ledger.state.LastRawDown = rawDown
	ledger.state.HasRaw = true
	if ledger.lastSaved.IsZero() || now.Sub(ledger.lastSaved) >= saveInterval {
		if err := ledger.saveLocked(now); err != nil {
			return ledger.snapshotLocked(), err
		}
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) Snapshot(now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.monotonicObservationTimeLocked(now)
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) Reset(rawUp, rawDown uint64, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	now = ledger.monotonicObservationTimeLocked(now)
	ledger.startCycleLocked(now)
	ledger.state.Interfaces = nil
	ledger.state.ResetOperations = nil
	ledger.state.LastRawUp = rawUp
	ledger.state.LastRawDown = rawDown
	ledger.state.HasRaw = true
	if err := ledger.saveLocked(now); err != nil {
		return ledger.snapshotLocked(), err
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) ResetInterfaces(counters map[string]InterfaceCounter, now time.Time) (Snapshot, error) {
	return ledger.ResetInterfacesForOperation(counters, "", now)
}

func (ledger *Ledger) ResetInterfacesForOperation(counters map[string]InterfaceCounter, operationID string, now time.Time) (Snapshot, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	now = ledger.monotonicObservationTimeLocked(now)
	if operationID != "" {
		if snapshot, ok := ledger.replayedResetOperationLocked(operationID, now); ok {
			return snapshot, nil
		}
	}
	ledger.startCycleLocked(now)
	ledger.state.Interfaces = make(map[string]persistedInterfaceCounter, len(counters))
	var rawUp, rawDown uint64
	for name, counter := range counters {
		if name == "" {
			continue
		}
		ledger.state.Interfaces[name] = persistedInterfaceCounter{Up: counter.Up, Down: counter.Down}
		rawUp = saturatingAdd(rawUp, counter.Up)
		rawDown = saturatingAdd(rawDown, counter.Down)
	}
	ledger.state.LastRawUp = rawUp
	ledger.state.LastRawDown = rawDown
	ledger.state.HasRaw = true
	if operationID != "" {
		ledger.rememberResetOperationLocked(operationID, ledger.snapshotLocked(), now)
	}
	if err := ledger.saveLocked(now); err != nil {
		if operationID != "" {
			delete(ledger.state.ResetOperations, operationID)
		}
		return ledger.snapshotLocked(), err
	}
	return ledger.snapshotLocked(), nil
}

func (ledger *Ledger) replayedResetOperationLocked(operationID string, now time.Time) (Snapshot, bool) {
	operation, ok := ledger.state.ResetOperations[operationID]
	if !ok {
		return Snapshot{}, false
	}
	if !operation.ExpiresAt.After(now) {
		delete(ledger.state.ResetOperations, operationID)
		return Snapshot{}, false
	}
	return operation.Snapshot, true
}

func (ledger *Ledger) rememberResetOperationLocked(operationID string, snapshot Snapshot, now time.Time) {
	if ledger.state.ResetOperations == nil {
		ledger.state.ResetOperations = make(map[string]persistedResetOperation)
	}
	for id, operation := range ledger.state.ResetOperations {
		if !operation.ExpiresAt.After(now) {
			delete(ledger.state.ResetOperations, id)
		}
	}
	if len(ledger.state.ResetOperations) >= maxRememberedResetOperations {
		for id := range ledger.state.ResetOperations {
			delete(ledger.state.ResetOperations, id)
			break
		}
	}
	ledger.state.ResetOperations[operationID] = persistedResetOperation{
		Snapshot:  snapshot,
		ExpiresAt: now.Add(resetOperationRetention),
	}
}

func (ledger *Ledger) startCycleLocked(now time.Time) {
	ledger.state.CycleID = manualCycleID(now)
	ledger.state.CycleStartedAt = now
	ledger.advanceCycleGenerationLocked()
	ledger.state.TotalUp = 0
	ledger.state.TotalDown = 0
	ledger.state.SampleSequence = 0
}

func (ledger *Ledger) snapshotLocked() Snapshot {
	return Snapshot{
		CycleID: ledger.state.CycleID, CycleStartedAt: ledger.state.CycleStartedAt, LedgerEpoch: ledger.state.LedgerEpoch,
		CycleGeneration: ledger.state.CycleGeneration, SampleSequence: ledger.state.SampleSequence,
		TotalUp: ledger.state.TotalUp, TotalDown: ledger.state.TotalDown,
	}
}

func newLedgerEpoch() string {
	var data [16]byte
	prefix := strconv.FormatInt(time.Now().UnixNano(), 10)
	if _, err := rand.Read(data[:]); err == nil {
		return prefix + "-" + hex.EncodeToString(data[:])
	}
	return prefix
}

func (ledger *Ledger) advanceCycleGenerationLocked() {
	if ledger.state.CycleGeneration == math.MaxUint64 {
		ledger.state.CycleGeneration = 1
		return
	}
	ledger.state.CycleGeneration++
}

func (ledger *Ledger) advanceSampleSequenceLocked() {
	if ledger.state.SampleSequence == math.MaxUint64 {
		ledger.advanceCycleGenerationLocked()
		ledger.state.SampleSequence = 0
	}
	ledger.state.SampleSequence++
}

func (ledger *Ledger) monotonicObservationTimeLocked(now time.Time) time.Time {
	if now.IsZero() {
		now = time.Now()
	}
	if !ledger.state.LastObservedAt.IsZero() && now.Before(ledger.state.LastObservedAt) {
		now = ledger.state.LastObservedAt
	}
	ledger.state.LastObservedAt = now
	return now
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

func manualCycleID(start time.Time) string {
	return "manual:" + start.UTC().Format(time.RFC3339Nano)
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
