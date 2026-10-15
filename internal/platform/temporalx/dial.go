package temporalx

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/platform/config"
)

func Dial(c config.Config) (client.Client, error) {
	options := client.Options{HostPort: c.TemporalAddress, Namespace: c.TemporalNamespace}
	if c.TemporalTLS {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TemporalServerName}
		if c.TemporalCAFile != "" {
			b, e := os.ReadFile(c.TemporalCAFile)
			if e != nil {
				return nil, fmt.Errorf("cannot read ORBIT_TEMPORAL_TLS_CA_FILE")
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			if !tlsConfig.RootCAs.AppendCertsFromPEM(b) {
				return nil, fmt.Errorf("invalid Temporal CA")
			}
		}
		if c.TemporalCertFile != "" || c.TemporalKeyFile != "" {
			cert, e := tls.LoadX509KeyPair(c.TemporalCertFile, c.TemporalKeyFile)
			if e != nil {
				return nil, fmt.Errorf("invalid Temporal client certificate/key")
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		options.ConnectionOptions.TLS = tlsConfig
	}
	return client.Dial(options)
}
