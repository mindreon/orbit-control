package objstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/mindreon/orbit-control/internal/platform/ids"
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
	return &S3{client: c, bucket: bucket}, nil
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
	_, err = s.client.PutObject(
		ctx,
		s.bucket,
		key,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
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

// ObjectStore serves run inputs and artifacts in addition to content-addressed blobs.
type ObjectStore interface {
	Store
	PutObject(context.Context, string, string, []byte, string) error
	GetObject(context.Context, string, string, int64) ([]byte, string, error)
	PresignGet(context.Context, string, string, time.Duration) (string, error)
	Head(context.Context, string, string) (int64, string, error)
}

func tenantKey(tenant, key string) error {
	if !ids.Valid(tenant) || !strings.HasPrefix(key, "t/"+tenant+"/") || strings.Contains(key, "..") ||
		strings.ContainsAny(key, "\\\x00") {
		return fmt.Errorf("invalid tenant object key")
	}
	return nil
}
func (s *S3) PutObject(ctx context.Context, tenant, key string, data []byte, mime string) error {
	if e := tenantKey(tenant, key); e != nil {
		return e
	}
	_, e := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: mime})
	return e
}
func (s *S3) GetObject(ctx context.Context, tenant, key string, limit int64) ([]byte, string, error) {
	if e := tenantKey(tenant, key); e != nil {
		return nil, "", e
	}
	o, e := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if e != nil {
		return nil, "", e
	}
	defer o.Close()
	st, e := o.Stat()
	if e != nil {
		return nil, "", e
	}
	b, e := io.ReadAll(io.LimitReader(o, limit+1))
	if int64(len(b)) > limit {
		return nil, "", fmt.Errorf("object exceeds limit")
	}
	return b, st.ContentType, e
}
func (s *S3) PresignGet(ctx context.Context, tenant, key string, ttl time.Duration) (string, error) {
	if e := tenantKey(tenant, key); e != nil {
		return "", e
	}
	u, e := s.client.PresignedGetObject(ctx, s.bucket, key, ttl, nil)
	if e != nil {
		return "", e
	}
	return u.String(), nil
}
func (s *S3) Head(ctx context.Context, tenant, key string) (int64, string, error) {
	if e := tenantKey(tenant, key); e != nil {
		return 0, "", e
	}
	st, e := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	return st.Size, st.ContentType, e
}
