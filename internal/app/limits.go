package app

import (
	"github.com/mindreon/orbit-control/internal/config"
)

// ingestMaxBytes is ORBIT_INGEST_MAX_BYTES, the cap of one POST /internal/events body. An invalid value keeps the
// default.
func ingestMaxBytes() int64 {
	return config.Load().IngestMaxBytes
}
