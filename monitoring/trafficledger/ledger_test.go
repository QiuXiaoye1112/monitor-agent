package trafficledger

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLedgerPersistsAndSurvivesRawCounterReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	start := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	ledger, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Observe(100, 200, start); err != nil {
		t.Fatal(err)
	}
	got, err := ledger.Observe(160, 290, start.Add(time.Minute))
	if err != nil || got.TotalUp != 60 || got.TotalDown != 90 {
		t.Fatalf("totals = %+v, err=%v", got, err)
	}
	if err = ledger.saveLocked(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.state.LedgerEpoch == "" || reloaded.state.LedgerEpoch == ledger.state.LedgerEpoch {
		t.Fatal("process restart did not create a new report epoch")
	}
	got, err = reloaded.Observe(10, 20, start.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 60 || got.TotalDown != 90 {
		t.Fatalf("counter reset changed totals to %d/%d", got.TotalUp, got.TotalDown)
	}
	got, err = reloaded.Observe(25, 50, start.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 75 || got.TotalDown != 120 {
		t.Fatalf("post-reset totals = %d/%d", got.TotalUp, got.TotalDown)
	}
}

func TestManualResetStartsFreshCycle(t *testing.T) {
	ledger := newMemoryLedger()
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	first, err := ledger.Observe(100, 200, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ledger.Observe(150, 260, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.SampleSequence != 1 || second.SampleSequence != 2 {
		t.Fatalf("sampling order = %+v then %+v", first, second)
	}
	reset, err := ledger.Reset(150, 260, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if reset.TotalUp != 0 || reset.TotalDown != 0 || reset.CycleGeneration <= second.CycleGeneration || reset.SampleSequence != 0 {
		t.Fatalf("bad reset: %+v", reset)
	}
	after, err := ledger.Observe(160, 280, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if after.TotalUp != 10 || after.TotalDown != 20 || after.SampleSequence != 1 {
		t.Fatalf("post-reset snapshot = %+v", after)
	}
}

func TestLedgerDoesNotRotateAtCalendarBoundary(t *testing.T) {
	ledger := newMemoryLedger()
	before := time.Date(2026, time.August, 31, 23, 59, 0, 0, time.UTC)
	first, err := ledger.Observe(100, 200, before)
	if err != nil {
		t.Fatal(err)
	}
	after, err := ledger.Observe(150, 275, before.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if after.CycleID != first.CycleID || after.CycleGeneration != first.CycleGeneration {
		t.Fatalf("calendar boundary changed manual cycle: before=%+v after=%+v", first, after)
	}
	if after.TotalUp != 50 || after.TotalDown != 75 {
		t.Fatalf("calendar boundary totals = %d/%d", after.TotalUp, after.TotalDown)
	}
}

func TestOpenPreservesLegacyConfiguredLedgerWithoutFutureRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-ledger.json")
	legacy := `{"version":1,"config":{"enabled":true,"day":1,"hour":0,"minute":0,"timezone":"Asia/Shanghai"},"ledger_epoch":"old","cycle_id":"2026-08-01T00:00:00Z","cycle_started_at":"2026-08-01T00:00:00Z","cycle_generation":7,"sample_sequence":4,"total_up":600,"total_down":900,"last_raw_up":1000,"last_raw_down":2000,"has_raw":true,"last_observed_at":"2026-08-31T23:59:00Z"}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	ledger, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ledger.Observe(1050, 2075, time.Date(2026, time.September, 1, 0, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if got.CycleID != "2026-08-01T00:00:00Z" || got.CycleGeneration != 7 {
		t.Fatalf("legacy cycle changed during upgrade: %+v", got)
	}
	if got.TotalUp != 650 || got.TotalDown != 975 {
		t.Fatalf("legacy totals were not preserved: %+v", got)
	}
}

func TestObserveReturnsInMemoryTotalsWhenPersistenceFails(t *testing.T) {
	ledger := newMemoryLedger()
	ledger.path = t.TempDir()
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ledger.state.LastRawUp = 100
	ledger.state.LastRawDown = 200
	ledger.state.HasRaw = true
	snapshot, err := ledger.Observe(150, 275, now)
	if err == nil || snapshot.TotalUp != 50 || snapshot.TotalDown != 75 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

func TestLedgerUsesPerInterfaceBaselines(t *testing.T) {
	ledger := newMemoryLedger()
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 100, Down: 200}}, now); err != nil {
		t.Fatal(err)
	}
	first, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{
		"eth0": {Up: 160, Down: 260},
		"eth1": {Up: 1 << 40, Down: 1 << 40},
	}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalUp != 60 || first.TotalDown != 60 {
		t.Fatalf("new interface counted historical traffic: %+v", first)
	}
	second, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{
		"eth0": {Up: 170, Down: 280},
		"eth1": {Up: 1<<40 + 10, Down: 1<<40 + 20},
	}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if second.TotalUp != 80 || second.TotalDown != 100 {
		t.Fatalf("per-interface deltas = %d/%d", second.TotalUp, second.TotalDown)
	}
	third, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{
		"eth0": {Up: 5, Down: 8},
		"eth1": {Up: 1<<40 + 15, Down: 1<<40 + 30},
	}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if third.TotalUp != 85 || third.TotalDown != 110 {
		t.Fatalf("counter reset handling = %d/%d", third.TotalUp, third.TotalDown)
	}
}

func TestLedgerClampsOutOfOrderObservationTime(t *testing.T) {
	ledger := newMemoryLedger()
	now := time.Date(2026, time.August, 2, 0, 0, 0, 0, time.UTC)
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 100, Down: 100}}, now); err != nil {
		t.Fatal(err)
	}
	got, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 150, Down: 160}}, now.Add(-48*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 50 || got.TotalDown != 60 {
		t.Fatalf("out-of-order sample changed totals: %+v", got)
	}
}

func TestLedgerPreservesBaselineAcrossInterfaceDisappearance(t *testing.T) {
	ledger := newMemoryLedger()
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 100, Down: 200}}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 150, Down: 275}}, now.Add(2*time.Minute))
	if err != nil || got.TotalUp != 50 || got.TotalDown != 75 {
		t.Fatalf("reappearing interface = %+v, err=%v", got, err)
	}
}

func TestResetOperationIsIdempotentAcrossReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	now := time.Date(2026, time.August, 2, 0, 0, 0, 0, time.UTC)
	ledger, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := ledger.ResetInterfacesForOperation(map[string]InterfaceCounter{"eth0": {Up: 100, Down: 200}}, "reset-operation-1", now)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reloaded.ResetInterfacesForOperation(map[string]InterfaceCounter{"eth0": {Up: 900, Down: 900}}, "reset-operation-1", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if replayed.CycleID != first.CycleID || !replayed.CycleStartedAt.Equal(first.CycleStartedAt) ||
		replayed.TotalUp != first.TotalUp || replayed.TotalDown != first.TotalDown {
		t.Fatalf("replayed reset changed result: first=%+v replayed=%+v", first, replayed)
	}
	got, err := reloaded.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 150, Down: 275}}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 50 || got.TotalDown != 75 {
		t.Fatalf("replayed reset changed baseline: %d/%d", got.TotalUp, got.TotalDown)
	}
}
