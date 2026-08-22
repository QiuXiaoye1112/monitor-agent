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
	counters, _, _, err := collectNetworkCounters(includeNics, excludeNics)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	now := time.Now()
	upSpeed, downSpeed = updateNetworkSpeedSample(counters, now)
	snapshot, err := trafficledger.ObserveInterfaces(counters, now)
	if err != nil {
		return snapshot.TotalUp, snapshot.TotalDown, upSpeed, downSpeed, fmt.Errorf("persist traffic ledger: %w", err)
	}
	return snapshot.TotalUp, snapshot.TotalDown, upSpeed, downSpeed, nil
}

// NetworkTotalsSnapshot samples the persistent traffic ledger immediately.
func NetworkTotalsSnapshot() (totalUp, totalDown uint64, err error) {
	counters, _, _, err := collectNetworkCounters(parseNics(flags.IncludeNics), parseNics(flags.ExcludeNics))
	if err != nil {
		return 0, 0, err
	}
	snapshot, err := trafficledger.ObserveInterfaces(counters, time.Now())
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

func ResetNetworkTraffic(operationID string) (trafficledger.Snapshot, error) {
	counters, _, _, err := collectNetworkCounters(parseNics(flags.IncludeNics), parseNics(flags.ExcludeNics))
	if err != nil {
		return trafficledger.Snapshot{}, err
	}
	return trafficledger.ResetInterfacesForOperation(counters, operationID, time.Now())
}

func getNetworkSpeedFallback(includeNics, excludeNics map[string]struct{}) (totalUp, totalDown, upSpeed, downSpeed uint64, err error) {
	counters, totalUp, totalDown, err := collectNetworkCounters(includeNics, excludeNics)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	upSpeed, downSpeed = updateNetworkSpeedSample(counters, time.Now())
	return totalUp, totalDown, upSpeed, downSpeed, nil
}

func collectNetworkTotals(includeNics, excludeNics map[string]struct{}) (totalUp, totalDown uint64, err error) {
	_, totalUp, totalDown, err = collectNetworkCounters(includeNics, excludeNics)
	return totalUp, totalDown, err
}

func collectNetworkCounters(includeNics, excludeNics map[string]struct{}) (map[string]trafficledger.InterfaceCounter, uint64, uint64, error) {
	ioCounters, err := net.IOCounters(true)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("failed to get network IO counters: %w", err)
	}

	if len(ioCounters) == 0 {
		return nil, 0, 0, fmt.Errorf("no network interfaces found")
	}

	identities := networkInterfaceIdentities()
	counters := make(map[string]trafficledger.InterfaceCounter)
	var totalUp, totalDown uint64
	for _, interfaceStats := range ioCounters {
		if shouldInclude(interfaceStats.Name, includeNics, excludeNics) {
			counter := trafficledger.InterfaceCounter{Up: interfaceStats.BytesSent, Down: interfaceStats.BytesRecv}
			identity := interfaceStats.Name
			if resolved, ok := identities[interfaceStats.Name]; ok {
				identity = resolved
			}
			counters[identity] = counter
			totalUp = saturatingAdd(totalUp, counter.Up)
			totalDown = saturatingAdd(totalDown, counter.Down)
		}
	}

	return counters, totalUp, totalDown, nil
}

func networkInterfaceIdentities() map[string]string {
	identities := make(map[string]string)
	interfaces, err := net.Interfaces()
	if err != nil {
		return identities
	}
	for _, iface := range interfaces {
		identity := networkInterfaceIdentity(iface.Name, iface.Index, iface.HardwareAddr)
		identities[iface.Name] = identity
	}
	return identities
}

func networkInterfaceIdentity(name string, index int, hardwareAddr string) string {
	return fmt.Sprintf("%s#%d#%s", name, index, strings.ToLower(hardwareAddr))
}

func saturatingAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

type networkSpeedState struct {
	sync.Mutex
	counters  map[string]trafficledger.InterfaceCounter
	sampledAt time.Time
}

var networkSpeedSample networkSpeedState

func updateNetworkSpeedSample(counters map[string]trafficledger.InterfaceCounter, now time.Time) (upSpeed, downSpeed uint64) {
	networkSpeedSample.Lock()
	defer networkSpeedSample.Unlock()

	if networkSpeedSample.sampledAt.IsZero() {
		networkSpeedSample.counters = copyInterfaceCounters(counters)
		networkSpeedSample.sampledAt = now
		return 0, 0
	}

	elapsed := now.Sub(networkSpeedSample.sampledAt).Seconds()
	if elapsed <= 0 {
		return 0, 0
	}

	var upDelta, downDelta uint64
	for name, counter := range counters {
		previous, exists := networkSpeedSample.counters[name]
		if !exists {
			continue
		}
		upDelta = saturatingAdd(upDelta, safeCounterDelta(counter.Up, previous.Up))
		downDelta = saturatingAdd(downDelta, safeCounterDelta(counter.Down, previous.Down))
	}

	networkSpeedSample.counters = copyInterfaceCounters(counters)
	networkSpeedSample.sampledAt = now

	return uint64(float64(upDelta) / elapsed), uint64(float64(downDelta) / elapsed)
}

func copyInterfaceCounters(counters map[string]trafficledger.InterfaceCounter) map[string]trafficledger.InterfaceCounter {
	copyOf := make(map[string]trafficledger.InterfaceCounter, len(counters))
	for name, counter := range counters {
		copyOf[name] = counter
	}
	return copyOf
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
