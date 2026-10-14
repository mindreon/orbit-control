package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"

	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Store interface {
	Put(context.Context, string, string, []byte) (string, error)
	Get(context.Context, string, string, int64) ([]byte, error)
}
type S3 struct {
	client *minio.Client
	bucket string
}

func New(endpoint, bucket, access, secret string, secure bool) (*S3, error) {
	if endpoint == "" || bucket == "" || access == "" || secret == "" {
		return nil, fmt.Errorf("object storage configuration required")
	}
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: secure})
	if err != nil {
		return nil, fmt.Errorf("invalid object storage configuration")
	}
	return &S3{c, bucket}, nil
}
func Key(tenant, digest string) (string, error) {
	hex := strings.TrimPrefix(digest, "sha256:")
	if !ids.Valid(tenant) || len(hex) != 64 || strings.Trim(hex, "0123456789abcdef") != "" || digest != "sha256:"+hex {
		return "", fmt.Errorf("invalid blob identity")
	}
	return "t/" + tenant + "/blobs/" + hex, nil
}
func (s *S3) Put(ctx context.Context, tenant, digest string, data []byte) (string, error) {
	if fmt.Sprintf("sha256:%x", sha256.Sum256(data)) != digest {
		return "", fmt.Errorf("blob digest mismatch")
	}
	key, err := Key(tenant, digest)
	if err != nil {
		return "", err
	}
	_, err = s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return key, err
}
func (s *S3) Get(ctx context.Context, tenant, digest string, limit int64) ([]byte, error) {
	key, err := Key(tenant, digest)
	if err != nil {
		return nil, err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	data, err := io.ReadAll(io.LimitReader(obj, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit || fmt.Sprintf("sha256:%x", sha256.Sum256(data)) != digest {
		return nil, fmt.Errorf("blob size or digest mismatch")
	}
	return data, nil
}
