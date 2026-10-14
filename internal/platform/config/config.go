package config

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL, AdminDatabaseURL, Addr, AuthMode, Issuer, ClientID, Audience, KeyFile, LogFormat string
	CORS                                                                                          []string
	AllowDevNetwork                                                                               bool
	PersonalTenants                                                                               bool
	PublicOrigin                                                                                  string
	CapabilityKeyFile, CapabilityKeyID                                                            string
	CapabilityPublicKeys                                                                          map[string][]byte
	SecretsKeyVersion                                                                             int
	SecretsKEKs                                                                                   map[int][]byte
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("ORBIT_DATABASE_URL"), AdminDatabaseURL: os.Getenv("ORBIT_ADMIN_DATABASE_URL"), Addr: env("ORBIT_HTTP_ADDR", "127.0.0.1:8080"), AuthMode: env("ORBIT_AUTH_MODE", "oidc"), Issuer: os.Getenv("ORBIT_OIDC_ISSUER"), ClientID: os.Getenv("ORBIT_OIDC_CLIENT_ID"), Audience: os.Getenv("ORBIT_OIDC_AUDIENCE"), KeyFile: env("ORBIT_SIGNING_KEY_FILE", ".orbit-data/signing.key"), LogFormat: env("ORBIT_LOG_FORMAT", "json"), AllowDevNetwork: os.Getenv("ORBIT_ALLOW_DEV_AUTH_ON_NETWORK") == "1"}
	c.PersonalTenants = os.Getenv("ORBIT_PERSONAL_TENANTS") == "true"
	if v := os.Getenv("ORBIT_PERSONAL_TENANTS"); v != "" && v != "true" && v != "false" {
		return c, fmt.Errorf("ORBIT_PERSONAL_TENANTS must be true or false")
	}
	c.PublicOrigin = os.Getenv("ORBIT_PUBLIC_ORIGIN")
	c.CapabilityKeyFile = env("ORBIT_CAPABILITY_KEY_FILE", ".orbit-data/capability.key")
	c.CapabilityKeyID = env("ORBIT_CAPABILITY_KEY_ID", "1")
	c.CapabilityPublicKeys = map[string][]byte{}
	// Rotation keys are kid:base64(raw Ed25519 public key), comma-separated.
	if v := os.Getenv("ORBIT_CAPABILITY_PUBLIC_KEYS"); v != "" {
		for pair := range strings.SplitSeq(v, ",") {
			id, encoded, ok := strings.Cut(pair, ":")
			b, err := base64.StdEncoding.DecodeString(encoded)
			if !ok || id == "" || err != nil || len(b) != 32 {
				return c, fmt.Errorf("ORBIT_CAPABILITY_PUBLIC_KEYS must contain kid:base64 Ed25519 keys")
			}
			if _, ok := c.CapabilityPublicKeys[id]; ok {
				return c, fmt.Errorf("ORBIT_CAPABILITY_PUBLIC_KEYS duplicate kid")
			}
			c.CapabilityPublicKeys[id] = b
		}
	}
	c.SecretsKeyVersion = 1
	if v := os.Getenv("ORBIT_SECRETS_KEY_VERSION"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return c, fmt.Errorf("ORBIT_SECRETS_KEY_VERSION must be positive")
		}
		c.SecretsKeyVersion = n
	}
	c.SecretsKEKs = map[int][]byte{}
	if v := os.Getenv("ORBIT_SECRETS_KEK"); v != "" {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil || len(b) != 32 {
			return c, fmt.Errorf("ORBIT_SECRETS_KEK must be base64 of 32 bytes")
		}
		c.SecretsKEKs[c.SecretsKeyVersion] = b
	}
	if v := os.Getenv("ORBIT_SECRETS_KEK_PREVIOUS"); v != "" {
		for pair := range strings.SplitSeq(v, ",") {
			version, encoded, ok := strings.Cut(pair, ":")
			n, err := strconv.Atoi(version)
			b, decodeErr := base64.StdEncoding.DecodeString(encoded)
			if !ok || err != nil || n < 1 || decodeErr != nil || len(b) != 32 || c.SecretsKEKs[n] != nil {
				return c, fmt.Errorf("ORBIT_SECRETS_KEK_PREVIOUS must contain distinct version:base64 32-byte keys")
			}
			c.SecretsKEKs[n] = b
		}
	}
	if s := os.Getenv("ORBIT_CORS_ORIGINS"); s != "" {
		c.CORS = strings.Split(s, ",")
	}
	if c.AuthMode != "dev" && c.AuthMode != "oidc" {
		return c, fmt.Errorf("ORBIT_AUTH_MODE must be oidc or dev")
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		return c, fmt.Errorf("ORBIT_LOG_FORMAT must be json or text")
	}
	origins := append([]string{}, c.CORS...)
	if c.PublicOrigin != "" {
		origins = append(origins, c.PublicOrigin)
	}
	for _, s := range origins {
		u, e := url.Parse(s)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return c, fmt.Errorf("ORBIT_PUBLIC_ORIGIN / ORBIT_CORS_ORIGINS must contain exact HTTP origins")
		}
	}
	return c, nil
}
func (c Config) ValidateAPI() error {
	if err := validateURL("ORBIT_DATABASE_URL", c.DatabaseURL); err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(c.Addr)
	if err != nil {
		return fmt.Errorf("ORBIT_HTTP_ADDR: invalid host:port")
	}
	ip := net.ParseIP(host)
	if c.AuthMode == "dev" && !(host == "localhost" || ip != nil && ip.IsLoopback()) && !c.AllowDevNetwork {
		return fmt.Errorf("ORBIT_ALLOW_DEV_AUTH_ON_NETWORK=1 is required for ORBIT_AUTH_MODE=dev on non-loopback ORBIT_HTTP_ADDR")
	}
	if c.AuthMode == "oidc" {
		u, e := url.Parse(c.Issuer)
		if e != nil || u.Host == "" || u.Scheme != "https" && u.Scheme != "http" {
			return fmt.Errorf("ORBIT_OIDC_ISSUER must be an HTTP issuer URL")
		}
		if c.ClientID == "" {
			return fmt.Errorf("ORBIT_OIDC_CLIENT_ID is required")
		}
		if c.Audience == "" {
			return fmt.Errorf("ORBIT_OIDC_AUDIENCE is required")
		}
	}
	if len(c.SecretsKEKs[c.SecretsKeyVersion]) != 32 {
		return fmt.Errorf("ORBIT_SECRETS_KEK is required (base64 32 bytes)")
	}
	if c.CapabilityKeyFile == "" || c.CapabilityKeyID == "" {
		return fmt.Errorf("ORBIT_CAPABILITY_KEY_FILE and ORBIT_CAPABILITY_KEY_ID are required")
	}
	if c.KeyFile == c.CapabilityKeyFile {
		return fmt.Errorf("ORBIT_CAPABILITY_KEY_FILE must differ from ORBIT_SIGNING_KEY_FILE")
	}
	if c.KeyFile == "" {
		return fmt.Errorf("ORBIT_SIGNING_KEY_FILE is required")
	}
	return nil
}
func (c Config) ValidateAdmin() error {
	return validateURL("ORBIT_ADMIN_DATABASE_URL", c.AdminDatabaseURL)
}
func validateURL(name, value string) error {
	u, e := url.Parse(value)
	if e != nil || u.Host == "" || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return fmt.Errorf("%s must be a PostgreSQL URL", name)
	}
	return nil
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
