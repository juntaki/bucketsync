package bucketsync

import (
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type S3Session struct {
	svc    *s3.S3
	cache  *Cache
	logger *Logger
	bucket string
}

func NewS3Session(config *Config, logger *Logger) (*S3Session, error) {
	sess := session.Must(session.NewSession())

	svc := s3.New(sess, &aws.Config{
		Region: aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(
			config.AccessKey,
			config.SecretKey,
			"",
		),
		Logger: aws.Logger(logger),
		//LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	})

	return &S3Session{svc: svc,
		cache:  NewCache(),
		logger: logger,
		bucket: config.Bucket,
	}, nil
}

func (s *S3Session) DownloadWithCache(key ObjectKey) ([]byte, error) {
	cached := s.cache.get(key)
	if cached != nil {
		return cached, nil
	}
	new, err := s.Download(key)
	if err != nil {
		return nil, err
	}
	s.cache.set(key, new)
	return new, nil
}

func (s *S3Session) Download(key ObjectKey) ([]byte, error) {
	s.logger.Info("Download", zap.String("key", key))

	if key == "" {
		return nil, errors.New("Key shouldn't be empty")
	}

	paramsGet := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	obj, cause := s.svc.GetObject(paramsGet)
	if cause != nil {
		return nil, errors.Wrapf(cause, "GetObject failed. key = %s", key)
	}
	defer obj.Body.Close()

	body, cause := ioutil.ReadAll(obj.Body)
	if cause != nil {
		return nil, errors.Wrapf(cause, "GetObject failed. key = %s", key)
	}

	s.logger.Debug("Download", zap.Int("size", len(body)))
	return body, nil
}

func (s *S3Session) UploadWithCache(key ObjectKey, value io.ReadSeeker) error {
	s.cache.invalidate(key)
	return s.Upload(key, value)
}

func (s *S3Session) Upload(key ObjectKey, value io.ReadSeeker) error {
	s.logger.Info("Upload", zap.String("key", key))

	paramsPut := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   value,
	}
	_, cause := s.svc.PutObject(paramsPut)
	if cause != nil {
		return errors.Wrapf(cause, "PutObject failed. key = %s", key)
	}
	return nil
}

func (s *S3Session) IsExist(key ObjectKey) bool {
	paramsHead := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	_, err := s.svc.HeadObject(paramsHead)
	return err == nil
}
