package monitoring

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"monitor-agent/monitoring/trafficledger"
)

func TestConnectionsCount(t *testing.T) {
	tcpCount, udpCount, err := ConnectionsCount()
	if err != nil {
		t.Fatalf("ConnectionsCount failed: %v", err)
	}

	if tcpCount < 0 {
		t.Errorf("Expected non-negative TCP count, got %d", tcpCount)
	}

	if udpCount < 0 {
		t.Errorf("Expected non-negative UDP count, got %d", udpCount)
	}

	t.Logf("TCP connections: %d, UDP connections: %d", tcpCount, udpCount)
}

func TestConnectionsCountCombinesProcAndFallbackErrors(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("proc net fast path only runs on linux")
	}

	fallbackErr := errors.New("fallback unavailable")
	fallback := func() (int, int, error) {
		return 0, 0, fallbackErr
	}

	tcpCount, udpCount, err := connectionsCountWithProcFallback(t.TempDir(), fallback)
	if err == nil {
		t.Fatal("expected combined error, got nil")
	}
	if tcpCount != 0 || udpCount != 0 {
		t.Fatalf("expected zero counts on failure, got tcp=%d udp=%d", tcpCount, udpCount)
	}
	if !errors.Is(err, fallbackErr) {
		t.Fatalf("expected combined error to wrap fallback error, got %v", err)
	}

	errText := err.Error()
	for _, want := range []string{"proc net fast path failed", "no proc net files found", "gopsutil fallback failed"} {
		if !strings.Contains(errText, want) {
			t.Fatalf("expected error %q to contain %q", errText, want)
		}
	}
}

func TestParseNics(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]struct{}
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:  "single nic",
			input: "eth0",
			expected: map[string]struct{}{
				"eth0": {},
			},
		},
		{
			name:  "multiple nics",
			input: "eth0,wlan0,enp0s3",
			expected: map[string]struct{}{
				"eth0":   {},
				"wlan0":  {},
				"enp0s3": {},
			},
		},
		{
			name:  "nics with spaces",
			input: " eth0 , wlan0 , enp0s3 ",
			expected: map[string]struct{}{
				"eth0":   {},
				"wlan0":  {},
				"enp0s3": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseNics(tt.input)

			if tt.expected == nil && result != nil {
				t.Errorf("Expected nil, got %v", result)
				return
			}

			if tt.expected != nil && result == nil {
				t.Errorf("Expected %v, got nil", tt.expected)
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("Expected %d items, got %d", len(tt.expected), len(result))
				return
			}

			for key := range tt.expected {
				if _, exists := result[key]; !exists {
					t.Errorf("Expected key %s not found in result", key)
				}
			}
		})
	}
}

func TestNetworkInterfaceIdentityDistinguishesRecreatedDevice(t *testing.T) {
	first := networkInterfaceIdentity("eth0", 2, "AA:BB:CC:DD:EE:FF")
	second := networkInterfaceIdentity("eth0", 7, "11:22:33:44:55:66")
	if first == second {
		t.Fatalf("recreated interfaces shared identity %q", first)
	}
	if first != "eth0#2#aa:bb:cc:dd:ee:ff" {
		t.Fatalf("unexpected normalized identity %q", first)
	}
}

func TestShouldInclude(t *testing.T) {
	tests := []struct {
		name        string
		nicName     string
		includeNics map[string]struct{}
		excludeNics map[string]struct{}
		expected    bool
	}{
		{
			name:        "loopback interface should be excluded",
			nicName:     "lo",
			includeNics: nil,
			excludeNics: nil,
			expected:    false,
		},
		{
			name:        "docker interface should be excluded",
			nicName:     "docker0",
			includeNics: nil,
			excludeNics: nil,
			expected:    false,
		},
		{
			name:        "normal interface with no filters",
			nicName:     "eth0",
			includeNics: nil,
			excludeNics: nil,
			expected:    true,
		},
		{
			name:    "interface in include list",
			nicName: "eth0",
			includeNics: map[string]struct{}{
				"eth0": {},
			},
			excludeNics: nil,
			expected:    true,
		},
		{
			name:    "interface not in include list",
			nicName: "wlan0",
			includeNics: map[string]struct{}{
				"eth0": {},
			},
			excludeNics: nil,
			expected:    false,
		},
		{
			name:        "interface in exclude list",
			nicName:     "eth0",
			includeNics: nil,
			excludeNics: map[string]struct{}{
				"eth0": {},
			},
			expected: false,
		},
		{
			name:        "interface not in exclude list",
			nicName:     "wlan0",
			includeNics: nil,
			excludeNics: map[string]struct{}{
				"eth0": {},
			},
			expected: true,
		},
		{
			name:    "loopback in include list should still be excluded",
			nicName: "lo",
			includeNics: map[string]struct{}{
				"lo": {},
			},
			excludeNics: nil,
			expected:    false,
		},
		{
			name:        "interface with wildcard pattern in exclude list",
			nicName:     "tun0",
			includeNics: nil,
			excludeNics: map[string]struct{}{
				"tun*": {},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldInclude(tt.nicName, tt.includeNics, tt.excludeNics)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestNetworkSpeedFallback(t *testing.T) {
	// 测试回退方法
	includeNics := map[string]struct{}{}
	excludeNics := map[string]struct{}{}

	totalUp, totalDown, upSpeed, downSpeed, err := getNetworkSpeedFallback(includeNics, excludeNics)
	if err != nil {
		t.Fatalf("getNetworkSpeedFallback failed: %v", err)
	}

	t.Logf("TotalUp: %d, TotalDown: %d, UpSpeed: %d/s, DownSpeed: %d/s",
		totalUp, totalDown, upSpeed, downSpeed)
}

func TestNetworkSpeedUsesPerInterfaceBaselines(t *testing.T) {
	networkSpeedSample.Lock()
	networkSpeedSample.counters = nil
	networkSpeedSample.sampledAt = time.Time{}
	networkSpeedSample.Unlock()

	start := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	first, down := updateNetworkSpeedSample(map[string]trafficledger.InterfaceCounter{
		"eth0": {Up: 100, Down: 200},
	}, start)
	if first != 0 || down != 0 {
		t.Fatalf("initial speed = %d/%d", first, down)
	}
	up, down := updateNetworkSpeedSample(map[string]trafficledger.InterfaceCounter{
		"eth0": {Up: 200, Down: 300},
		"eth1": {Up: 1 << 40, Down: 1 << 40},
	}, start.Add(time.Second))
	if up != 100 || down != 100 {
		t.Fatalf("new interface caused speed spike = %d/%d", up, down)
	}
}

func TestNetworkSpeedWithNicFilters(t *testing.T) {
	// 保存原始值
	originalIncludeNics := flags.IncludeNics
	originalExcludeNics := flags.ExcludeNics

	// 恢复原始值
	defer func() {
		flags.IncludeNics = originalIncludeNics
		flags.ExcludeNics = originalExcludeNics
	}()

	// 测试排除回环接口
	flags.IncludeNics = ""
	flags.ExcludeNics = "lo,docker0"

	totalUp, totalDown, upSpeed, downSpeed, err := NetworkSpeed()
	if err != nil {
		t.Fatalf("NetworkSpeed with excludeNics failed: %v", err)
	}

	t.Logf("With excludeNics - TotalUp: %d, TotalDown: %d, UpSpeed: %d/s, DownSpeed: %d/s",
		totalUp, totalDown, upSpeed, downSpeed)
}
