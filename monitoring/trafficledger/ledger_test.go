package trafficledger

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLedgerPersistsAndSurvivesRawCounterReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	config := Config{Enabled: true, Day: 15, Hour: 8, Minute: 30, Timezone: "Asia/Shanghai"}
	start := time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC)
	ledger, err := Open(path, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.Observe(100, 200, start); err != nil {
		t.Fatal(err)
	}
	got, err := ledger.Observe(160, 290, start.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 60 || got.TotalDown != 90 {
		t.Fatalf("totals = %d/%d", got.TotalUp, got.TotalDown)
	}
	if err = ledger.saveLocked(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	reloaded, err := Open(path, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.state.LedgerEpoch == "" || reloaded.state.LedgerEpoch == ledger.state.LedgerEpoch {
		t.Fatalf("process restart did not create a new report epoch: %q vs %q", reloaded.state.LedgerEpoch, ledger.state.LedgerEpoch)
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

func TestLedgerRotatesAtConfiguredMinute(t *testing.T) {
	ledger := newMemoryLedger(Config{Enabled: true, Day: 15, Hour: 8, Minute: 30, Timezone: "Asia/Shanghai"})
	before := time.Date(2026, 8, 15, 0, 29, 59, 0, time.UTC)
	if _, err := ledger.Observe(100, 200, before); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Observe(140, 260, before.Add(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	after, err := ledger.Observe(150, 275, before.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if after.TotalUp != 0 || after.TotalDown != 0 {
		t.Fatalf("new cycle totals = %d/%d", after.TotalUp, after.TotalDown)
	}
	if want := time.Date(2026, 8, 15, 0, 30, 0, 0, time.UTC); !after.CycleStartedAt.Equal(want) {
		t.Fatalf("cycle start = %s, want %s", after.CycleStartedAt, want)
	}
}

func TestConfigureAndManualResetStartFreshLedger(t *testing.T) {
	ledger := newMemoryLedger(Config{})
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	_, _ = ledger.Observe(100, 200, now)
	_, _ = ledger.Observe(150, 260, now.Add(time.Minute))
	configured, err := ledger.Configure(Config{Enabled: true, Day: 1, Timezone: "UTC"}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if configured.TotalUp != 0 || configured.TotalDown != 0 {
		t.Fatal("configuration did not reset ledger")
	}
	reset, err := ledger.Reset(175, 300, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if reset.TotalUp != 0 || reset.TotalDown != 0 || reset.CycleID == "" {
		t.Fatalf("bad reset: %+v", reset)
	}
}

func TestLedgerGenerationAndSampleSequenceAdvanceAcrossReset(t *testing.T) {
	ledger := newMemoryLedger(Config{Enabled: true, Day: 1, Timezone: "UTC"})
	now := time.Date(2026, time.August, 2, 0, 0, 0, 0, time.UTC)
	first, err := ledger.Observe(100, 200, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ledger.Observe(110, 220, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.CycleGeneration != second.CycleGeneration || first.SampleSequence != 1 || second.SampleSequence != 2 {
		t.Fatalf("sampling order = %+v then %+v", first, second)
	}

	reset, err := ledger.Reset(110, 220, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if reset.CycleGeneration <= second.CycleGeneration || reset.SampleSequence != 0 {
		t.Fatalf("reset did not advance generation: %+v", reset)
	}
	after, err := ledger.Observe(120, 240, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if after.CycleGeneration != reset.CycleGeneration || after.SampleSequence != 1 {
		t.Fatalf("post-reset sequence = %+v", after)
	}
}

func TestRepeatedConfigLeavesBoundaryRotationToRawObservation(t *testing.T) {
	config := Config{Enabled: true, Day: 15, Hour: 8, Minute: 30, Timezone: "Asia/Shanghai"}
	ledger := newMemoryLedger(config)
	before := time.Date(2026, 8, 15, 0, 29, 59, 0, time.UTC)
	_, _ = ledger.Observe(100, 200, before)
	_, _ = ledger.Observe(140, 260, before.Add(500*time.Millisecond))

	configured, err := ledger.Configure(config, before.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if configured.TotalUp != 40 || configured.TotalDown != 60 {
		t.Fatalf("idempotent config changed totals to %d/%d", configured.TotalUp, configured.TotalDown)
	}

	after, err := ledger.Observe(150, 275, before.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if after.TotalUp != 0 || after.TotalDown != 0 {
		t.Fatalf("new cycle totals = %d/%d", after.TotalUp, after.TotalDown)
	}
}

func TestObserveReturnsInMemoryTotalsWhenPersistenceFails(t *testing.T) {
	ledger := newMemoryLedger(Config{})
	ledger.path = t.TempDir() // Renaming the temporary file over this directory must fail.
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	ledger.state.LastRawUp = 100
	ledger.state.LastRawDown = 200
	ledger.state.HasRaw = true

	snapshot, err := ledger.Observe(150, 275, now)
	if err == nil {
		t.Fatal("expected persistence failure")
	}
	if snapshot.TotalUp != 50 || snapshot.TotalDown != 75 {
		t.Fatalf("lost in-memory totals after persistence error: %+v", snapshot)
	}
}

func TestLedgerUsesPerInterfaceBaselines(t *testing.T) {
	ledger := newMemoryLedger(Config{})
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{
		"eth0": {Up: 100, Down: 200},
	}, now); err != nil {
		t.Fatal(err)
	}
	first, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{
		"eth0": {Up: 160, Down: 260},
		// A newly discovered interface may already contain a large historical
		// counter and must not create a fake monthly spike.
		"eth1": {Up: 1 << 40, Down: 1 << 40},
	}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalUp != 60 || first.TotalDown != 60 {
		t.Fatalf("new interface was counted as historical traffic: %+v", first)
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
	ledger := newMemoryLedger(Config{Enabled: true, Day: 1, Timezone: "UTC"})
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

func TestLedgerResetsSamplingBaselineAtCycleBoundary(t *testing.T) {
	config := Config{Enabled: true, Day: 1, Timezone: "UTC"}
	ledger := newMemoryLedger(config)
	before := time.Date(2026, time.August, 31, 23, 58, 0, 0, time.UTC)
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 100, Down: 200}}, before); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 160, Down: 280}}, before.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	boundary := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	rotated, err := ledger.Snapshot(boundary)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.TotalUp != 0 || rotated.TotalDown != 0 {
		t.Fatalf("boundary snapshot retained totals: %+v", rotated)
	}

	first, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 170, Down: 300}}, boundary.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalUp != 0 || first.TotalDown != 0 {
		t.Fatalf("post-boundary baseline counted pre-boundary traffic: %+v", first)
	}
	second, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 180, Down: 325}}, boundary.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if second.TotalUp != 10 || second.TotalDown != 25 {
		t.Fatalf("post-boundary delta = %d/%d", second.TotalUp, second.TotalDown)
	}
}

func TestLedgerPreservesBaselineAcrossInterfaceDisappearance(t *testing.T) {
	ledger := newMemoryLedger(Config{})
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 100, Down: 200}}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	got, err := ledger.ObserveInterfaces(map[string]InterfaceCounter{"eth0": {Up: 150, Down: 275}}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 50 || got.TotalDown != 75 {
		t.Fatalf("reappearing interface delta = %d/%d", got.TotalUp, got.TotalDown)
	}
}

func TestResetOperationIsIdempotentAcrossReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	config := Config{Enabled: true, Day: 1, Timezone: "UTC"}
	now := time.Date(2026, time.August, 2, 0, 0, 0, 0, time.UTC)
	ledger, err := Open(path, config)
	if err != nil {
		t.Fatal(err)
	}
	first, err := ledger.ResetInterfacesForOperation(map[string]InterfaceCounter{
		"eth0": {Up: 100, Down: 200},
	}, "reset-operation-1", now)
	if err != nil {
		t.Fatal(err)
	}

	reloaded, err := Open(path, config)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reloaded.ResetInterfacesForOperation(map[string]InterfaceCounter{
		"eth0": {Up: 900, Down: 900},
	}, "reset-operation-1", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if replayed.CycleID != first.CycleID || !replayed.CycleStartedAt.Equal(first.CycleStartedAt) ||
		replayed.TotalUp != first.TotalUp || replayed.TotalDown != first.TotalDown {
		t.Fatalf("replayed reset changed result: first=%+v replayed=%+v", first, replayed)
	}

	got, err := reloaded.ObserveInterfaces(map[string]InterfaceCounter{
		"eth0": {Up: 150, Down: 275},
	}, now.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalUp != 50 || got.TotalDown != 75 {
		t.Fatalf("replayed reset changed baseline: %d/%d", got.TotalUp, got.TotalDown)
	}
}

func TestFixedUTCOffsets(t *testing.T) {
	tests := map[string]int{
		"UTC":       0,
		"UTC+8":     8 * 60,
		"UTC+08:00": 8 * 60,
		"UTC-05:30": -(5*60 + 30),
		"GMT+14:00": 14 * 60,
	}
	for value, expected := range tests {
		got, ok := fixedUTCOffsetMinutes(value)
		if !ok || got != expected {
			t.Fatalf("fixedUTCOffsetMinutes(%q) = %d, %v; want %d, true", value, got, ok, expected)
		}
	}
	for _, value := range []string{"UTC+14:01", "UTC+15:00", "UTC+08:99", "UTC+8:5", "Mars/Olympus"} {
		if _, ok := fixedUTCOffsetMinutes(value); ok {
			t.Fatalf("invalid offset %q was accepted", value)
		}
	}
}

func TestCycleStartUsesPerClientTimezone(t *testing.T) {
	now := time.Date(2026, time.August, 1, 0, 30, 0, 0, time.UTC)
	plusEight := cycleStart(Config{Enabled: true, Day: 1, Timezone: "UTC+08:00"}, now)
	if want := time.Date(2026, time.July, 31, 16, 0, 0, 0, time.UTC); !plusEight.Equal(want) {
		t.Fatalf("UTC+08 cycle start = %s, want %s", plusEight, want)
	}
	minusFive := cycleStart(Config{Enabled: true, Day: 1, Timezone: "UTC-05:00"}, now)
	if want := time.Date(2026, time.July, 1, 5, 0, 0, 0, time.UTC); !minusFive.Equal(want) {
		t.Fatalf("UTC-05 cycle start = %s, want %s", minusFive, want)
	}
	newYork := cycleStart(Config{Enabled: true, Day: 1, Timezone: "America/New_York"}, time.Date(2026, time.August, 2, 0, 0, 0, 0, time.UTC))
	if want := time.Date(2026, time.August, 1, 4, 0, 0, 0, time.UTC); !newYork.Equal(want) {
		t.Fatalf("New York cycle start = %s, want %s", newYork, want)
	}
}
