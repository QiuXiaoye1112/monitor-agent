package server

import (
	"context"
	"testing"
)

func TestServiceHealthStopsReportingAfterThreeMisses(t *testing.T) {
	state := newServiceHealthState()
	if state.canReport() {
		t.Fatal("reporting enabled before the first Pong")
	}
	if !state.recordPong() {
		t.Fatal("first Pong did not enable reporting")
	}

	for miss := 1; miss <= maxConsecutiveHeartbeatMisses; miss++ {
		gotMisses, becameUnavailable := state.recordMiss()
		if gotMisses != miss {
			t.Fatalf("miss %d: count = %d", miss, gotMisses)
		}
		if miss < maxConsecutiveHeartbeatMisses {
			if becameUnavailable {
				t.Fatalf("miss %d unexpectedly disabled reporting", miss)
			}
			if !state.canReport() {
				t.Fatalf("miss %d disabled reporting too early", miss)
			}
		} else {
			if !becameUnavailable {
				t.Fatal("third miss did not transition to unavailable")
			}
			if state.canReport() {
				t.Fatal("reporting still enabled after third miss")
			}
		}
	}

	gotMisses, becameUnavailable := state.recordMiss()
	if gotMisses != maxConsecutiveHeartbeatMisses || becameUnavailable {
		t.Fatalf("additional miss = (%d, %t), want capped count and no new transition", gotMisses, becameUnavailable)
	}
}

func TestServiceHealthPongRestoresReportingAndResetsMisses(t *testing.T) {
	state := newServiceHealthState()
	state.recordPong()
	for range maxConsecutiveHeartbeatMisses {
		state.recordMiss()
	}

	if !state.recordPong() {
		t.Fatal("pong did not transition unavailable service to available")
	}
	if !state.canReport() {
		t.Fatal("reporting not enabled after pong")
	}

	gotMisses, becameUnavailable := state.recordMiss()
	if gotMisses != 1 || becameUnavailable {
		t.Fatalf("first miss after recovery = (%d, %t), want (1, false)", gotMisses, becameUnavailable)
	}
	if state.recordPong() {
		t.Fatal("pong while already available reported a new transition")
	}
}

func TestServiceHealthCancelsInFlightReports(t *testing.T) {
	state := newServiceHealthState()
	initialContext, ok := state.context()
	if ok || initialContext.Err() == nil {
		t.Fatal("initial reporting context should be disabled and canceled")
	}

	state.recordPong()
	activeContext, ok := state.context()
	if !ok || activeContext.Err() != nil {
		t.Fatal("reporting context was not activated after Pong")
	}

	for range maxConsecutiveHeartbeatMisses {
		state.recordMiss()
	}
	if activeContext.Err() != context.Canceled {
		t.Fatalf("active context error = %v, want context.Canceled", activeContext.Err())
	}

	state.recordPong()
	recoveredContext, ok := state.context()
	if !ok || recoveredContext.Err() != nil {
		t.Fatal("reporting context was not recreated after recovery")
	}
	if recoveredContext == activeContext {
		t.Fatal("recovery reused the canceled reporting context")
	}
}
