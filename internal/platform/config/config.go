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
	InternalAddr, WorkerServiceTokenSHA256, NATSURL, Environment                                              string
	DefaultRunBudget                                                                                          int64
	AssetUploadConcurrency                                                                                    int
	TemporalAddress, TemporalNamespace, TemporalCAFile, TemporalCertFile, TemporalKeyFile, TemporalServerName string
	TemporalTLS                                                                                               bool
	ObjectEndpoint, ObjectBucket, ObjectAccessKey, ObjectSecretKey                                            string
	ObjectTLS                                                                                                 bool
	AssetKeyFile, AssetKeyID                                                                                  string
	AssetPublicKeys                                                                                           map[string][]byte
	DatabaseURL, AdminDatabaseURL, Addr, AuthMode, Issuer, ClientID, Audience, KeyFile, LogFormat             string
	CORS                                                                                                      []string
	AllowDevNetwork                                                                                           bool
	PersonalTenants                                                                                           bool
	PublicOrigin                                                                                              string
	CapabilityKeyFile, CapabilityKeyID                                                                        string
	CapabilityPublicKeys                                                                                      map[string][]byte
	SecretsKeyVersion                                                                                         int
	SecretsKEKs                                                                                               map[int][]byte
}

func Load() (Config, error) {
	c := Config{DatabaseURL: os.Getenv("ORBIT_DATABASE_URL"), AdminDatabaseURL: os.Getenv("ORBIT_ADMIN_DATABASE_URL"), Addr: env("ORBIT_HTTP_ADDR", "127.0.0.1:8080"), AuthMode: env("ORBIT_AUTH_MODE", "oidc"), Issuer: os.Getenv("ORBIT_OIDC_ISSUER"), ClientID: os.Getenv("ORBIT_OIDC_CLIENT_ID"), Audience: os.Getenv("ORBIT_OIDC_AUDIENCE"), KeyFile: env("ORBIT_SIGNING_KEY_FILE", ".orbit-data/signing.key"), LogFormat: env("ORBIT_LOG_FORMAT", "json"), AllowDevNetwork: os.Getenv("ORBIT_ALLOW_DEV_AUTH_ON_NETWORK") == "1"}
	c.InternalAddr = env("ORBIT_INTERNAL_ADDR", "127.0.0.1:8081")
	c.NATSURL = env("ORBIT_NATS_URL", "nats://127.0.0.1:4222")
	c.WorkerServiceTokenSHA256 = os.Getenv("ORBIT_WORKER_SERVICE_TOKEN_SHA256")
	c.Environment = env("ORBIT_ENV", "dev")
	if c.Environment != "dev" && c.Environment != "prod" {
		return c, fmt.Errorf("ORBIT_ENV must be dev or prod")
	}
	c.DefaultRunBudget = 200000
	if value := os.Getenv("ORBIT_DEFAULT_RUN_BUDGET_MILLICREDITS"); value != "" {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n < 1 || n > 9007199254740991 {
			return c, fmt.Errorf("ORBIT_DEFAULT_RUN_BUDGET_MILLICREDITS must be a positive safe integer")
		}
		c.DefaultRunBudget = n
	}
	c.AssetUploadConcurrency = 2
	if value := os.Getenv("ORBIT_ASSET_UPLOAD_CONCURRENCY"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 32 {
			return c, fmt.Errorf("ORBIT_ASSET_UPLOAD_CONCURRENCY must be between 1 and 32")
		}
		c.AssetUploadConcurrency = n
	}
	c.TemporalAddress = env("ORBIT_TEMPORAL_ADDRESS", "127.0.0.1:7233")
	c.TemporalNamespace = env("ORBIT_TEMPORAL_NAMESPACE", "default")
	for _, name := range []string{"ORBIT_TEMPORAL_TLS", "ORBIT_OBJECT_TLS"} {
		if v := os.Getenv(name); v != "" && v != "true" && v != "false" {
			return c, fmt.Errorf("%s must be true or false", name)
		}
	}
	c.TemporalTLS = os.Getenv("ORBIT_TEMPORAL_TLS") == "true"
	c.TemporalCAFile = os.Getenv("ORBIT_TEMPORAL_TLS_CA_FILE")
	c.TemporalCertFile = os.Getenv("ORBIT_TEMPORAL_TLS_CERT_FILE")
	c.TemporalKeyFile = os.Getenv("ORBIT_TEMPORAL_TLS_KEY_FILE")
	c.TemporalServerName = os.Getenv("ORBIT_TEMPORAL_TLS_SERVER_NAME")
	c.ObjectEndpoint = env("ORBIT_S3_ENDPOINT", os.Getenv("ORBIT_OBJECT_ENDPOINT"))
	c.ObjectBucket = env("ORBIT_S3_BUCKET", env("ORBIT_OBJECT_BUCKET", "orbit"))
	c.ObjectAccessKey = env("ORBIT_S3_ACCESS_KEY", os.Getenv("ORBIT_OBJECT_ACCESS_KEY"))
	c.ObjectSecretKey = env("ORBIT_S3_SECRET_KEY", os.Getenv("ORBIT_OBJECT_SECRET_KEY"))
	c.ObjectTLS = env("ORBIT_S3_USE_TLS", os.Getenv("ORBIT_OBJECT_TLS")) == "true"
	c.AssetKeyFile = env("ORBIT_ASSET_SIGNING_KEY_FILE", ".orbit-data/assets.key")
	c.AssetKeyID = env("ORBIT_ASSET_SIGNING_KEY_ID", "1")
	c.AssetPublicKeys = map[string][]byte{}
	for pair := range strings.SplitSeq(os.Getenv("ORBIT_ASSET_PUBLIC_KEYS"), ",") {
		if pair == "" {
			continue
		}
		id, encoded, ok := strings.Cut(pair, ":")
		b, e := base64.StdEncoding.DecodeString(encoded)
		if !ok || id == "" || e != nil || len(b) != 32 || c.AssetPublicKeys[id] != nil {
			return c, fmt.Errorf("ORBIT_ASSET_PUBLIC_KEYS must contain distinct kid:base64 Ed25519 keys")
		}
		c.AssetPublicKeys[id] = b
	}
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
	if file := os.Getenv("ORBIT_KEK_FILE"); file != "" {
		info, err := os.Stat(file)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return c, fmt.Errorf("ORBIT_KEK_FILE must be a regular file with mode 0600")
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			return c, fmt.Errorf("ORBIT_KEK_FILE cannot be read")
		}
		key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil || len(key) != 32 {
			return c, fmt.Errorf("ORBIT_KEK_FILE must contain base64 of 32 bytes")
		}
		c.SecretsKEKs[c.SecretsKeyVersion] = key
	}

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

func (c Config) ValidateAssets() error {
	for _, v := range []struct{ name, value string }{{"ORBIT_OBJECT_ENDPOINT", c.ObjectEndpoint}, {"ORBIT_OBJECT_BUCKET", c.ObjectBucket}, {"ORBIT_OBJECT_ACCESS_KEY", c.ObjectAccessKey}, {"ORBIT_OBJECT_SECRET_KEY", c.ObjectSecretKey}, {"ORBIT_ASSET_SIGNING_KEY_FILE", c.AssetKeyFile}, {"ORBIT_ASSET_SIGNING_KEY_ID", c.AssetKeyID}} {
		if v.value == "" {
			return fmt.Errorf("%s is required", v.name)
		}
	}
	if c.AssetKeyFile == c.KeyFile || c.AssetKeyFile == c.CapabilityKeyFile {
		return fmt.Errorf("ORBIT_ASSET_SIGNING_KEY_FILE must differ from authentication signing keys")
	}
	if strings.Contains(c.AssetKeyID, ":") {
		return fmt.Errorf("ORBIT_ASSET_SIGNING_KEY_ID cannot contain a colon")
	}
	return nil
}
func (c Config) ValidateWorker() error {
	if err := validateURL("ORBIT_DATABASE_URL", c.DatabaseURL); err != nil {
		return err
	}
	if _, _, err := net.SplitHostPort(c.TemporalAddress); err != nil {
		return fmt.Errorf("ORBIT_TEMPORAL_ADDRESS must be host:port")
	}
	if c.TemporalNamespace == "" {
		return fmt.Errorf("ORBIT_TEMPORAL_NAMESPACE is required")
	}
	if (c.TemporalCertFile == "") != (c.TemporalKeyFile == "") {
		return fmt.Errorf("ORBIT_TEMPORAL_TLS_CERT_FILE and ORBIT_TEMPORAL_TLS_KEY_FILE must be provided together")
	}
	if !c.TemporalTLS && (c.TemporalCertFile != "" || c.TemporalCAFile != "" || c.TemporalServerName != "") {
		return fmt.Errorf("ORBIT_TEMPORAL_TLS=true is required for TLS options")
	}
	return nil
}

func (c Config) ValidateRuns(api bool) error {
	if err := c.ValidateWorker(); err != nil {
		return err
	}
	u, e := url.Parse(c.NATSURL)
	if e != nil || u.Host == "" || (u.Scheme != "nats" && u.Scheme != "tls") {
		return fmt.Errorf("ORBIT_NATS_URL must be a NATS URL")
	}
	if api {
		if _, _, err := net.SplitHostPort(c.InternalAddr); err != nil {
			return fmt.Errorf("ORBIT_INTERNAL_ADDR must be host:port")
		}
		if len(c.WorkerServiceTokenSHA256) != 64 || strings.Trim(c.WorkerServiceTokenSHA256, "0123456789abcdef") != "" {
			return fmt.Errorf("ORBIT_WORKER_SERVICE_TOKEN_SHA256 must be sha256 hex")
		}
	}
	return nil
}
