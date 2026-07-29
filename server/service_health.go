package server

import (
	"context"
	"log"
	"sync"
)

const maxConsecutiveHeartbeatMisses = 3

type serviceHealthState struct {
	mu                sync.RWMutex
	consecutiveMisses int
	reportingEnabled  bool
	reportContext     context.Context
	cancelReports     context.CancelFunc
}

func newServiceHealthState() *serviceHealthState {
	reportContext, cancelReports := context.WithCancel(context.Background())
	cancelReports()
	return &serviceHealthState{
		reportContext: reportContext,
		cancelReports: cancelReports,
	}
}

func (state *serviceHealthState) canReport() bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.reportingEnabled
}

func (state *serviceHealthState) context() (context.Context, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.reportContext, state.reportingEnabled
}

func (state *serviceHealthState) recordMiss() (misses int, becameUnavailable bool) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.consecutiveMisses < maxConsecutiveHeartbeatMisses {
		state.consecutiveMisses++
	}
	if state.consecutiveMisses >= maxConsecutiveHeartbeatMisses && state.reportingEnabled {
		state.reportingEnabled = false
		state.cancelReports()
		becameUnavailable = true
	}
	return state.consecutiveMisses, becameUnavailable
}

func (state *serviceHealthState) recordPong() (becameAvailable bool) {
	state.mu.Lock()
	defer state.mu.Unlock()

	state.consecutiveMisses = 0
	if !state.reportingEnabled {
		state.reportContext, state.cancelReports = context.WithCancel(context.Background())
		state.reportingEnabled = true
		becameAvailable = true
	}
	return becameAvailable
}

var monitorServiceHealth = newServiceHealthState()

func recordHeartbeatMiss(reason string) {
	misses, becameUnavailable := monitorServiceHealth.recordMiss()
	if becameUnavailable {
		log.Printf("Server heartbeat failed %d consecutive times (%s); pausing all reports and keeping heartbeat probes only", misses, reason)
		return
	}
	log.Printf("Server heartbeat missed (%d/%d): %s", misses, maxConsecutiveHeartbeatMisses, reason)
}

func recordHeartbeatPong() bool {
	becameAvailable := monitorServiceHealth.recordPong()
	if becameAvailable {
		log.Println("Server heartbeat confirmed; enabling all reports")
		go UpdateBasicInfo()
	}
	return becameAvailable
}
