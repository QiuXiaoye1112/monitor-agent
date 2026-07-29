package monitoring

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"

	unit "monitor-agent/monitoring/unit"
)

type report struct {
	CPU         cpuReport         `json:"cpu"`
	Ram         usageReport       `json:"ram"`
	Swap        usageReport       `json:"swap"`
	Load        interface{}       `json:"load,omitempty"`
	Disk        usageReport       `json:"disk"`
	Network     networkReport     `json:"network"`
	Connections connectionsReport `json:"connections"`
	Uptime      uint64            `json:"uptime"`
	Process     int               `json:"process"`
	Message     string            `json:"message"`
}

type cpuReport struct {
	Usage float64 `json:"usage"`
}

type usageReport struct {
	Total uint64 `json:"total"`
	Used  uint64 `json:"used"`
}

type networkReport struct {
	Up        uint64 `json:"up"`
	Down      uint64 `json:"down"`
	TotalUp   uint64 `json:"totalUp"`
	TotalDown uint64 `json:"totalDown"`
}

type connectionsReport struct {
	TCP int `json:"tcp"`
	UDP int `json:"udp"`
}

type TrafficSnapshot struct {
	CapturedAt time.Time
	TotalUp    int64
	TotalDown  int64
}

func GenerateTrafficSnapshot() (TrafficSnapshot, error) {
	totalUp, totalDown, err := unit.NetworkTotalsSnapshot()
	if err != nil {
		return TrafficSnapshot{}, err
	}
	if totalUp > math.MaxInt64 || totalDown > math.MaxInt64 {
		return TrafficSnapshot{}, fmt.Errorf("network counter exceeds int64")
	}
	return TrafficSnapshot{
		CapturedAt: time.Now(),
		TotalUp:    int64(totalUp),
		TotalDown:  int64(totalDown),
	}, nil
}

func GenerateReport() []byte {
	message := ""
	data := report{}

	cpu := unit.Cpu()
	cpuUsage := cpu.CPUUsage
	if cpuUsage <= 0.001 {
		cpuUsage = 0.001
	}
	data.CPU = cpuReport{Usage: cpuUsage}

	ram := unit.Ram()
	data.Ram = usageReport{Total: ram.Total, Used: ram.Used}

	swap := unit.Swap()
	data.Swap = usageReport{Total: swap.Total, Used: swap.Used}
	disk := unit.Disk()
	data.Disk = usageReport{Total: disk.Total, Used: disk.Used}

	totalUp, totalDown, networkUp, networkDown, err := unit.NetworkSpeed()
	if err != nil {
		message += fmt.Sprintf("failed to get network speed: %v\n", err)
	}
	data.Network = networkReport{Up: networkUp, Down: networkDown, TotalUp: totalUp, TotalDown: totalDown}

	tcpCount, udpCount, err := unit.ConnectionsCount()
	if err != nil {
		message += fmt.Sprintf("failed to get connections: %v\n", err)
	}
	data.Connections = connectionsReport{TCP: tcpCount, UDP: udpCount}

	uptime, err := unit.Uptime()
	if err != nil {
		message += fmt.Sprintf("failed to get uptime: %v\n", err)
	}
	data.Uptime = uptime

	data.Process = unit.ProcessCount()

	data.Message = message

	s, err := json.Marshal(data)
	if err != nil {
		log.Println("Failed to marshal data:", err)
	}
	return s
}
