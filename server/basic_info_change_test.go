package server

import (
	"context"
	"errors"
	"net"
	"testing"
)

func TestBasicInfoCheckUsesLocalChangeDetection(t *testing.T) {
	original := basicInfoFingerprint{
		OS:          "linux",
		Network:     "eth0|192.0.2.1",
		MemoryTotal: 1024,
		SwapTotal:   512,
		DiskTotal:   2048,
	}
	if !shouldUploadBasicInfo(false, false, basicInfoFingerprint{}, original) {
		t.Fatal("first basic info snapshot was not selected for upload")
	}
	if shouldUploadBasicInfo(false, true, original, original) {
		t.Fatal("unchanged basic info was selected for upload")
	}

	changed := original
	changed.DiskTotal++
	if !shouldUploadBasicInfo(false, true, original, changed) {
		t.Fatal("changed basic info was not selected for upload")
	}
	if !shouldUploadBasicInfo(true, true, original, original) {
		t.Fatal("forced heartbeat upload was skipped")
	}
}

func TestBasicInfoRequestHonorsCanceledReportingContext(t *testing.T) {
	originalEndpoint := flags.Endpoint
	originalToken := flags.Token
	flags.Endpoint = "http://127.0.0.1:1"
	flags.Token = "test"
	t.Cleanup(func() {
		flags.Endpoint = originalEndpoint
		flags.Token = originalToken
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := tryUploadData(ctx, map[string]interface{}{"os": "linux"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("upload error = %v, want context.Canceled", err)
	}
}

func TestBasicInfoNetworkFilter(t *testing.T) {
	tests := []struct {
		name             string
		networkInterface net.Interface
		want             bool
	}{
		{
			name:             "physical interface",
			networkInterface: net.Interface{Name: "eth0", Flags: net.FlagUp},
			want:             true,
		},
		{
			name:             "down interface",
			networkInterface: net.Interface{Name: "eth0"},
		},
		{
			name:             "loopback may carry routed public address",
			networkInterface: net.Interface{Name: "lo", Flags: net.FlagUp | net.FlagLoopback},
			want:             true,
		},
		{
			name:             "container veth",
			networkInterface: net.Interface{Name: "veth1234", Flags: net.FlagUp},
		},
		{
			name:             "docker bridge",
			networkInterface: net.Interface{Name: "docker0", Flags: net.FlagUp},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isReportRelevantInterface(test.networkInterface); got != test.want {
				t.Fatalf("isReportRelevantInterface() = %t, want %t", got, test.want)
			}
		})
	}

	if !isReportRelevantIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("public IPv4 address was filtered")
	}
	if isReportRelevantIP(net.ParseIP("10.0.0.1")) {
		t.Fatal("private IPv4 address was included")
	}
	if isReportRelevantIP(net.ParseIP("100.64.0.1")) {
		t.Fatal("carrier-grade NAT address was included")
	}
	if !isReportRelevantIP(net.ParseIP("2001:4860:4860::8888")) {
		t.Fatal("public IPv6 address was filtered")
	}
	if isReportRelevantIP(net.ParseIP("fe80::1")) {
		t.Fatal("link-local IPv6 address was included")
	}
}
