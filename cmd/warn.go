//go:build !windows

package cmd

func WarnMonitorRunning() {
	// No-op on non-Windows platforms
	return
}

func ShowToast() {
	// No-op on non-Windows platforms
	return
}
