package monitoring

import (
	"net"
	"testing"
)

func TestIsPublicIP(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{address: "8.8.8.8", want: true},
		{address: "10.0.0.1"},
		{address: "100.64.0.1"},
		{address: "127.0.0.1"},
		{address: "2001:4860:4860::8888", want: true},
		{address: "fd00::1"},
		{address: "fe80::1"},
	}
	for _, test := range tests {
		t.Run(test.address, func(t *testing.T) {
			if got := isPublicIP(net.ParseIP(test.address)); got != test.want {
				t.Fatalf("isPublicIP(%s) = %t, want %t", test.address, got, test.want)
			}
		})
	}
}

func TestIsVirtualInterfaceName(t *testing.T) {
	if !isVirtualInterfaceName("veth1234") || !isVirtualInterfaceName("docker0") {
		t.Fatal("container interface was not filtered")
	}
	if isVirtualInterfaceName("eth0") || isVirtualInterfaceName("ens3") {
		t.Fatal("physical interface was filtered")
	}
}
