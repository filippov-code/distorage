package repositories

import (
	"context"
	"io"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Storager interface {
	Save(id uuid.UUID, r io.Reader, size int64) error
	Get(id uuid.UUID) (io.ReadCloser, error)
	Delete(id uuid.UUID) error
}

const bucketName string = "uploaded"

type MinioStorage struct {
	client     *minio.Client
	bucketName string
}

func NewMinioStorage(endpoint, accessKey, secretKey string) (*MinioStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	if !exists {
		err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, err
		}
	}

	return &MinioStorage{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// Save сохраняет данные из io.Reader в объект MinIO
func (s *MinioStorage) Save(id uuid.UUID, r io.Reader, size int64) error {
	_, err := s.client.PutObject(
		context.Background(),
		s.bucketName,
		id.String(),
		r,
		size,
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

// Get возвращает io.ReadCloser для чтения объекта
func (s *MinioStorage) Get(id uuid.UUID) (io.ReadCloser, error) {
	return s.client.GetObject(
		context.Background(),
		s.bucketName,
		id.String(),
		minio.GetObjectOptions{},
	)
}

func (s *MinioStorage) Delete(id uuid.UUID) error {
	return s.client.RemoveObject(
		context.Background(),
		s.bucketName,
		id.String(),
		minio.RemoveObjectOptions{},
	)
}
