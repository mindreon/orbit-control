package app

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

var localMCPHosts = map[string]struct{}{
	"localhost":               {},
	"host.docker.internal":    {},
	"gateway.docker.internal": {},
}

var secretQueryKeys = map[string]struct{}{
	"token":        {},
	"key":          {},
	"access_token": {},
	"api_key":      {},
	"secret":       {},
	"password":     {},
	"apikey":       {},
}

// validateMCPHTTPURL allows HTTP for a local or LAN host. A public host must
// use HTTPS. The URL is not echoed when it is rejected, so a pasted secret
// does not land in the error or the log line that prints the error.
func validateMCPHTTPURL(raw string) (string, error) {
	text := strings.TrimSpace(raw)
	parsed, err := url.Parse(text)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("url must be http or https and include a host")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("url must not include a username or password")
	}
	for key := range parsed.Query() {
		if _, secret := secretQueryKeys[strings.ToLower(key)]; secret {
			return "", fmt.Errorf("url must not include a secret query parameter")
		}
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if isPrivateOrLocalHost(host) {
		return text, nil
	}
	if parsed.Scheme != "https" {
		return "", fmt.Errorf("public MCP server URLs must use https")
	}
	if publicIPRejected(host) {
		return "", fmt.Errorf("public MCP server URLs must not use a private address")
	}
	return text, nil
}

func isPrivateOrLocalHost(host string) bool {
	if _, ok := localMCPHosts[host]; ok {
		return true
	}
	if strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".internal") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || isCGNAT(ip)
}

func publicIPRejected(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || isCGNAT(ip)
}

func isCGNAT(ip net.IP) bool {
	ip4 := ip.To4()
	if ip4 == nil {
		return false
	}
	return ip4[0] == 100 && ip4[1]&0xc0 == 64
}
