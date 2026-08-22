package monitoring

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/net"
	"monitor-agent/monitoring/trafficledger"
)

func ConnectionsCount() (tcpCount, udpCount int, err error) {
	if runtime.GOOS == "linux" {
		return connectionsCountWithProcFallback(procRoot(), gopsutilConnectionsCount)
	}

	return gopsutilConnectionsCount()
}

func connectionsCountWithProcFallback(root string, fallback func() (int, int, error)) (tcpCount, udpCount int, err error) {
	var procErr error
	tcpCount, udpCount, procErr = procNetConnectionsCount(root)
	if procErr == nil {
		return tcpCount, udpCount, nil
	}

	tcpCount, udpCount, err = fallback()
	if err != nil && procErr != nil {
		return 0, 0, fmt.Errorf("proc net fast path failed: %w; gopsutil fallback failed: %w", procErr, err)
	}
	return tcpCount, udpCount, err
}

func gopsutilConnectionsCount() (tcpCount, udpCount int, err error) {
	tcps, err := net.Connections("tcp")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get TCP connections: %w", err)
	}
	udps, err := net.Connections("udp")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get UDP connections: %w", err)
	}

	return len(tcps), len(udps), nil
}

func procRoot() string {
	if flags.HostProc != "" {
		return flags.HostProc
	}
	return "/proc"
}

func procNetConnectionsCount(root string) (tcpCount, udpCount int, err error) {
	tcpCount, err = countProcNetFiles(root, "tcp", "tcp6")
	if err != nil {
		return 0, 0, err
	}
	udpCount, err = countProcNetFiles(root, "udp", "udp6")
	if err != nil {
		return 0, 0, err
	}
	return tcpCount, udpCount, nil
}

func countProcNetFiles(root string, names ...string) (int, error) {
	total := 0
	readAny := false
	for _, name := range names {
		count, err := countProcNetFile(filepath.Join(root, "net", name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		total += count
		readAny = true
	}
	if !readAny {
		return 0, fmt.Errorf("no proc net files found under %s", filepath.Join(root, "net"))
	}
	return total, nil
}

func countProcNetFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	header := true
	for scanner.Scan() {
		if header {
			header = false
			continue
		}
		if strings.TrimSpace(scanner.Text()) != "" {
			count++
		}
	}
	return count, scanner.Err()
}

var (
	// 预定义常见的回环和虚拟接口名称
	loopbackNames = map[string]struct{}{
		"br":      {},
		"cni":     {},
		"docker":  {},
		"podman":  {},
		"flannel": {},
		"lo":      {},
		"veth":    {}, // Docker
		"virbr":   {}, // KVM
		"vmbr":    {}, // Proxmox
		"tap":     {},
		"fwbr":    {},
		"fwpr":    {},
	}
)

func NetworkSpeed() (totalUp, totalDown, upSpeed, downSpeed uint64, err error) {
	includeNics := parseNics(flags.IncludeNics)
	excludeNics := parseNics(flags.ExcludeNics)
	rawUp, rawDown, err := collectNetworkTotals(includeNics, excludeNics)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	now := time.Now()
	upSpeed, downSpeed = updateNetworkSpeedSample(rawUp, rawDown, now)
	snapshot, err := trafficledger.Observe(rawUp, rawDown, now)
	if err != nil {
		return snapshot.TotalUp, snapshot.TotalDown, upSpeed, downSpeed, fmt.Errorf("persist traffic ledger: %w", err)
	}
	return snapshot.TotalUp, snapshot.TotalDown, upSpeed, downSpeed, nil
}

// NetworkTotalsSnapshot samples the persistent traffic ledger immediately.
func NetworkTotalsSnapshot() (totalUp, totalDown uint64, err error) {
	rawUp, rawDown, err := collectNetworkTotals(parseNics(flags.IncludeNics), parseNics(flags.ExcludeNics))
	if err != nil {
		return 0, 0, err
	}
	snapshot, err := trafficledger.Observe(rawUp, rawDown, time.Now())
	if err != nil {
		return 0, 0, err
	}
	return snapshot.TotalUp, snapshot.TotalDown, nil
}

func NetworkTrafficSnapshot() (trafficledger.Snapshot, error) {
	return trafficledger.SnapshotNow(time.Now())
}

func ConfigureNetworkTraffic(config trafficledger.Config) (trafficledger.Snapshot, error) {
	return trafficledger.Configure(config, time.Now())
}

func ResetNetworkTraffic() (trafficledger.Snapshot, error) {
	rawUp, rawDown, err := collectNetworkTotals(parseNics(flags.IncludeNics), parseNics(flags.ExcludeNics))
	if err != nil {
		return trafficledger.Snapshot{}, err
	}
	return trafficledger.Reset(rawUp, rawDown, time.Now())
}

func getNetworkSpeedFallback(includeNics, excludeNics map[string]struct{}) (totalUp, totalDown, upSpeed, downSpeed uint64, err error) {
	totalUp, totalDown, err = collectNetworkTotals(includeNics, excludeNics)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	upSpeed, downSpeed = updateNetworkSpeedSample(totalUp, totalDown, time.Now())
	return totalUp, totalDown, upSpeed, downSpeed, nil
}

func collectNetworkTotals(includeNics, excludeNics map[string]struct{}) (totalUp, totalDown uint64, err error) {
	ioCounters, err := net.IOCounters(true)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get network IO counters: %w", err)
	}

	if len(ioCounters) == 0 {
		return 0, 0, fmt.Errorf("no network interfaces found")
	}

	for _, interfaceStats := range ioCounters {
		if shouldInclude(interfaceStats.Name, includeNics, excludeNics) {
			totalUp += interfaceStats.BytesSent
			totalDown += interfaceStats.BytesRecv
		}
	}

	return totalUp, totalDown, nil
}

type networkSpeedState struct {
	sync.Mutex
	totalUp   uint64
	totalDown uint64
	sampledAt time.Time
}

var networkSpeedSample networkSpeedState

func updateNetworkSpeedSample(totalUp, totalDown uint64, now time.Time) (upSpeed, downSpeed uint64) {
	networkSpeedSample.Lock()
	defer networkSpeedSample.Unlock()

	if networkSpeedSample.sampledAt.IsZero() {
		networkSpeedSample.totalUp = totalUp
		networkSpeedSample.totalDown = totalDown
		networkSpeedSample.sampledAt = now
		return 0, 0
	}

	elapsed := now.Sub(networkSpeedSample.sampledAt).Seconds()
	if elapsed <= 0 {
		return 0, 0
	}

	upDelta := safeCounterDelta(totalUp, networkSpeedSample.totalUp)
	downDelta := safeCounterDelta(totalDown, networkSpeedSample.totalDown)

	networkSpeedSample.totalUp = totalUp
	networkSpeedSample.totalDown = totalDown
	networkSpeedSample.sampledAt = now

	return uint64(float64(upDelta) / elapsed), uint64(float64(downDelta) / elapsed)
}

func safeCounterDelta(current, previous uint64) uint64 {
	if current >= previous {
		return current - previous
	}
	return 0
}

func parseNics(nics string) map[string]struct{} {
	if nics == "" {
		return nil
	}
	nicSet := make(map[string]struct{})
	for _, nic := range strings.Split(nics, ",") {
		nicSet[strings.TrimSpace(nic)] = struct{}{}
	}
	return nicSet
}

func shouldInclude(nicName string, includeNics, excludeNics map[string]struct{}) bool {
	// 默认排除回环接口
	for loopbackName := range loopbackNames {
		if strings.HasPrefix(nicName, loopbackName) {
			return false
		}
	}

	// 如果定义了白名单，则只包括白名单中的接口
	for pattern := range includeNics {
		if matched, _ := filepath.Match(pattern, nicName); matched {
			return true
		}
	}

	// 如果定义了黑名单，则排除黑名单中的接口
	for pattern := range excludeNics {
		if matched, _ := filepath.Match(pattern, nicName); matched {
			return false
		}
	}

	return len(includeNics) == 0 // 如果没有定义白名单，则默认包含所有非回环接口
}

func InterfaceList() ([]string, error) {
	includeNics := parseNics(flags.IncludeNics)
	excludeNics := parseNics(flags.ExcludeNics)
	interfaces := []string{}

	ioCounters, err := net.IOCounters(true)
	if err != nil {
		return nil, err
	}
	for _, interfaceStats := range ioCounters {
		if shouldInclude(interfaceStats.Name, includeNics, excludeNics) {
			interfaces = append(interfaces, interfaceStats.Name)
		}
	}
	return interfaces, nil
}
