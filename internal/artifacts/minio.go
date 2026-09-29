package artifacts

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

var sha256Ref = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type MinIO struct {
	client *minio.Client
	bucket string
	TTL    time.Duration
}

func New(endpoint, accessKey, secretKey, bucket string, secure bool) (*MinIO, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		return nil, errors.New("artifact object store configuration is incomplete")
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.Scheme == "" {
		return nil, errors.New("artifact object store endpoint is invalid")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("artifact object store endpoint must use http or https")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure || u.Scheme == "https",
		// Presigning is local; a fixed region keeps it from asking the store where the bucket lives.
		Region: "us-east-1",
	})
	if err != nil {
		return nil, errors.New("artifact object store client setup failed")
	}
	return &MinIO{client: client, bucket: bucket, TTL: 10 * time.Minute}, nil
}

func (m *MinIO) PresignArtifact(ctx context.Context, p taskruntime.Principal, manifest taskruntime.ArtifactManifest, entry map[string]any) (string, error) {
	if p.TenantID == "" || manifest.TaskID == "" {
		return "", taskruntime.ErrNotFound
	}
	ref, _ := entry["blob_ref"].(string)
	if !sha256Ref.MatchString(ref) {
		return "", errors.New("artifact entry has an invalid blob reference")
	}
	name := strings.TrimPrefix(ref, "sha256:")
	object := "artifacts/" + p.TenantID + "/" + name
	ttl := m.TTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	result, err := m.client.PresignedGetObject(ctx, m.bucket, object, ttl, nil)
	if err != nil {
		return "", errors.New("artifact URL could not be created")
	}
	return result.String(), nil
}
