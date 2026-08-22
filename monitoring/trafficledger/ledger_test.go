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
