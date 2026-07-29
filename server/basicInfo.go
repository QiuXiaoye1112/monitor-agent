package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"monitor-agent/dnsresolver"
	monitoring "monitor-agent/monitoring/unit"
	"monitor-agent/protocol/transport"
	v2 "monitor-agent/protocol/v2"

	pkg_flags "monitor-agent/cmd/flags"
)

var flags = pkg_flags.GlobalConfig

const basicInfoCheckInterval = time.Minute

type basicInfoFingerprint struct {
	OS          string
	Network     string
	MemoryTotal uint64
	SwapTotal   uint64
	DiskTotal   uint64
}

var basicInfoUploadState struct {
	sync.Mutex
	lastFingerprint basicInfoFingerprint
	hasUploaded     bool
}

func MonitorBasicInfoChanges() {
	ticker := time.NewTicker(basicInfoCheckInterval)
	defer ticker.Stop()
	for range ticker.C {
		if !monitorServiceHealth.canReport() {
			continue
		}
		checkAndUploadBasicInfo(false)
	}
}

func UpdateBasicInfo() {
	if !monitorServiceHealth.canReport() {
		return
	}
	checkAndUploadBasicInfo(true)
}

func checkAndUploadBasicInfo(force bool) {
	fingerprint := collectBasicInfoFingerprint()

	basicInfoUploadState.Lock()
	defer basicInfoUploadState.Unlock()

	if !monitorServiceHealth.canReport() {
		return
	}
	if !shouldUploadBasicInfo(
		force,
		basicInfoUploadState.hasUploaded,
		basicInfoUploadState.lastFingerprint,
		fingerprint,
	) {
		return
	}

	reportContext, ok := monitorServiceHealth.context()
	if !ok {
		return
	}
	err := uploadBasicInfo(reportContext)
	if err != nil {
		log.Println("Error uploading basic info:", err)
		return
	}

	basicInfoUploadState.lastFingerprint = fingerprint
	basicInfoUploadState.hasUploaded = true
	if force {
		log.Println("Basic info uploaded after heartbeat confirmation")
		return
	}
	log.Println("Basic info changed and was uploaded")
}

func shouldUploadBasicInfo(
	force bool,
	hasUploaded bool,
	lastFingerprint basicInfoFingerprint,
	currentFingerprint basicInfoFingerprint,
) bool {
	return force || !hasUploaded || lastFingerprint != currentFingerprint
}

func collectBasicInfoFingerprint() basicInfoFingerprint {
	ram := monitoring.Ram()
	swap := monitoring.Swap()
	disk := monitoring.Disk()
	return basicInfoFingerprint{
		OS:          monitoring.OSName(),
		Network:     collectLocalNetworkState(),
		MemoryTotal: ram.Total,
		SwapTotal:   swap.Total,
		DiskTotal:   disk.Total,
	}
}

func collectLocalNetworkState() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	entries := make([]string, 0, len(interfaces)*2)
	for _, networkInterface := range interfaces {
		if !isReportRelevantInterface(networkInterface) {
			continue
		}
		addresses, addressErr := networkInterface.Addrs()
		if addressErr != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, parseErr := net.ParseCIDR(address.String())
			if parseErr != nil {
				ip = net.ParseIP(address.String())
			}
			if !isReportRelevantIP(ip) {
				continue
			}
			entries = append(entries, fmt.Sprintf(
				"address=%d|%s|%s",
				networkInterface.Index,
				networkInterface.Name,
				ip.String(),
			))
		}
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n")
}

func isReportRelevantInterface(networkInterface net.Interface) bool {
	if networkInterface.Flags&net.FlagUp == 0 {
		return false
	}
	name := strings.ToLower(networkInterface.Name)
	ignoredPrefixes := []string{
		"veth",
		"docker",
		"br-",
		"virbr",
		"cni",
		"flannel",
		"podman",
	}
	for _, prefix := range ignoredPrefixes {
		if strings.HasPrefix(name, prefix) {
			return false
		}
	}
	return true
}

func isReportRelevantIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	if ipv4 := ip.To4(); ipv4 != nil && ipv4[0] == 100 && ipv4[1]&0xc0 == 64 {
		return false
	}
	return true
}

func uploadBasicInfo(ctx context.Context) error {
	osname := monitoring.OSName()
	ipv4, ipv6, err := monitoring.GetIPAddressContext(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	data := map[string]interface{}{
		"os":         osname,
		"ipv4":       ipv4,
		"ipv6":       ipv6,
		"mem_total":  monitoring.Ram().Total,
		"swap_total": monitoring.Swap().Total,
		"disk_total": monitoring.Disk().Total,
	}

	err = tryUploadData(ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func tryUploadData(ctx context.Context, data map[string]interface{}) error {
	protocolVersion := uploadProtocolVersion()
	if protocolVersion >= 2 {
		err := tryUploadDataWithProtocol(ctx, data, 2)
		if shouldFallbackToV1(2, err) {
			log.Printf("v2 basic info failed %d consecutive protocol attempts, falling back to v1", v2ProtocolFallbackThreshold)
			setConnectionProtocolVersion(1)
			return tryUploadDataWithProtocol(ctx, data, 1)
		}
		return err
	}
	return tryUploadDataWithProtocol(ctx, data, 1)
}

func tryUploadDataWithProtocol(ctx context.Context, data map[string]interface{}, protocolVersion int) error {
	endpoint := strings.TrimSuffix(flags.Endpoint, "/") + "/api/clients/uploadBasicInfo?token=" + flags.Token
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if protocolVersion >= 2 {
		endpoint = strings.TrimSuffix(flags.Endpoint, "/") + "/api/clients/v2/rpc?token=" + flags.Token
		payload = v2.BuildBasicInfoPayload(data)
	}
	body := payload
	compressed := false
	if protocolVersion >= 2 && !flags.DisableCompression {
		if gz, err := transport.GzipBytes(payload); err == nil {
			body = gz
			compressed = true
		}
	}

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if compressed {
		req.Header.Set("Content-Encoding", "gzip")
	}

	// 添加Cloudflare Access头部
	if flags.CFAccessClientID != "" && flags.CFAccessClientSecret != "" {
		req.Header.Set("CF-Access-Client-Id", flags.CFAccessClientID)
		req.Header.Set("CF-Access-Client-Secret", flags.CFAccessClientSecret)
	}

	client := dnsresolver.GetHTTPClientWithPreference(30*time.Second, flags.PreferIPVersion)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	message := string(respBody)

	if resp.StatusCode != http.StatusOK {
		return &httpStatusError{StatusCode: resp.StatusCode, Status: resp.Status, Body: message}
	}
	if protocolVersion >= 2 {
		if len(bytes.TrimSpace(respBody)) > 0 {
			if _, err := parseV2Response(respBody); err != nil {
				return err
			}
		}
		resetV2ProtocolFailures(protocolVersion)
	}

	return nil
}
