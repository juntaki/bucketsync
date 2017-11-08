package bucketsync

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"syscall"
	"time"

	"encoding/json"

	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Config struct {
	Bucket        string `yaml:"bucket"`
	Region        string `yaml:"region"`
	AccessKey     string `yaml:"access_key"`
	SecretKey     string `yaml:"secret_key"`
	Password      string `yaml:"password"`
	Logging       string `yaml:"logging"`
	LogOutputPath string `yaml:"log_output_path"`
}

func (c *Config) validate() bool {
	// TODO
	return true
}

type Session struct {
	svc    *s3.S3
	config *Config
	logger *Logger
}

func (s *Session) RootKey() ObjectKey {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(s.config.Password)))
}

func NewSession(config *Config) (*Session, error) {
	if !config.validate() {
		return nil, errors.New("Invalid config")
	}

	logger, err := NewLogger(config.LogOutputPath, config.Logging == "development")
	if err != nil {
		return nil, err
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess, &aws.Config{
		Region: aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(
			config.AccessKey,
			config.SecretKey,
			"",
		),
		Logger: aws.Logger(logger),
		//LogLevel:    aws.LogLevel(aws.LogDebug),
	})

	bsess := &Session{
		svc:    svc,
		config: config,
		logger: logger,
	}

	if !bsess.IsExist(bsess.RootKey()) {
		logger.Error("root key is not found", zap.Error(err))

		root := &Directory{
			Key:    bsess.RootKey(),
			Parent: bsess.RootKey(),
			Meta: Meta{
				Mode:  fuse.S_IFDIR | 0755,
				Size:  0,
				UID:   0,
				GID:   0,
				Atime: time.Now(),
				Ctime: time.Now(),
				Mtime: time.Now(),
			},
			FileMeta: make(map[string]ObjectKey, 0),
			sess:     bsess,
		}

		err := root.Save()
		if err != nil {
			return nil, err
		}
	}

	logger.Info("New session created", zap.String("Root UUID", bsess.RootKey()))
	return bsess, nil
}

func (s *Session) CreateDirectory(key, parent ObjectKey, mode uint32, context *fuse.Context) *Directory {
	return &Directory{
		Key:      key,
		Parent:   parent,
		Meta:     NewMeta(fuse.S_IFDIR|mode, context),
		FileMeta: make(map[string]ObjectKey, 0),
		sess:     s,
	}
}

func (s *Session) NewDirectory(key ObjectKey) (*Directory, error) {
	obj, err := s.Download(key)
	if err != nil {
		return nil, err
	}
	node := &Directory{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (s *Session) CreateFile(key, parent ObjectKey, mode uint32, context *fuse.Context) *File {
	return &File{
		Key:    key,
		Parent: parent,
		Meta:   NewMeta(fuse.S_IFREG|mode, context),
		Extent: make(map[string]Extent, 0),
		sess:   s,
	}
}

func (s *Session) NewFile(key ObjectKey) (*File, error) {
	obj, err := s.Download(key)
	if err != nil {
		return nil, err
	}
	node := &File{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (s *Session) CreateSymLink(key, parent ObjectKey, linkTo string, context *fuse.Context) *SymLink {
	return &SymLink{
		Key:    key,
		Parent: parent,
		Meta:   NewMeta(fuse.S_IFLNK, context),
		LinkTo: linkTo,
		sess:   s,
	}
}

func (s *Session) NewSymLink(key ObjectKey) (*SymLink, error) {
	obj, err := s.Download(key)
	if err != nil {
		return nil, err
	}
	node := &SymLink{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (s *Session) NewNode(key ObjectKey) (*Node, error) {
	obj, err := s.Download(key)
	if err != nil {
		return nil, err
	}
	node := &Node{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// NewNode returns Directory, File or Symlink
func (s *Session) NewTypedNode(key ObjectKey) (interface{}, error) {
	obj, err := s.Download(key)
	if err != nil {
		return nil, err
	}

	tmpNode := &Node{}
	err = json.Unmarshal(obj, tmpNode)
	if err != nil {
		return nil, err
	}

	var node interface{}

	switch tmpNode.Meta.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		node = &Directory{}
	case syscall.S_IFREG:
		node = &File{}
	case syscall.S_IFLNK:
		node = &SymLink{}
	default:
		panic("Not implemented")
	}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (s *Session) PathWalk(relPath string) (key ObjectKey, err error) {
	s.logger.Info("PathWalk", zap.String("relPath", relPath))
	key = s.RootKey()

	// root
	if relPath == "." || relPath == "" {
		return
	}

	node, err := s.NewDirectory(key)
	if err != nil {
		return "", err
	}

	// "a/b/c" => [0:a, 1:b, 2:c] , len = 3
	pathList := strings.Split(relPath, string(filepath.Separator))
	for i, p := range pathList {
		var ok bool
		if key, ok = node.FileMeta[p]; !ok {
			return "", err
		}

		if i == len(pathList)-1 { // key points 2:c in example.
			break
		}

		node, err = s.NewDirectory(key)
		if err != nil {
			return "", err
		}
	}

	s.logger.Info("PathWalk finished", zap.String("key", key))
	return
}

// func aesStreamReader(in io.Reader, AESKey string) (cipher.StreamReader, error) {
// 	key := []byte(AESKey)

// 	block, err := aes.NewCipher(key)
// 	if err != nil {
// 		return cipher.StreamReader{}, err
// 	}
// 	var iv [aes.BlockSize]byte
// 	stream := cipher.NewOFB(block, iv[:])

// 	return cipher.StreamReader{S: stream, R: in}, nil
// }

// func aesStreamWriter(out io.Writer, AESKey string) (cipher.StreamWriter, error) {
// 	key := []byte(AESKey)

// 	block, err := aes.NewCipher(key)
// 	if err != nil {
// 		return cipher.StreamWriter{}, err
// 	}
// 	var iv [aes.BlockSize]byte
// 	stream := cipher.NewOFB(block, iv[:])

// 	return cipher.StreamWriter{S: stream, W: out}, nil
// }

func (s *Session) Download(key ObjectKey) ([]byte, error) {
	s.logger.Info("Download", zap.String("key", key))
	paramsGet := &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	}
	obj, cause := s.svc.GetObject(paramsGet)
	if aerr, ok := cause.(awserr.Error); ok {
		if aerr.Code() == "NoSuchKey" {
			return nil, newErrorKeyNotFound(cause, key)
		}
	}
	if cause != nil {
		return nil, errors.Wrapf(cause, "GetObject failed. key = %s", key)
	}
	defer obj.Body.Close()

	body, cause := ioutil.ReadAll(obj.Body)
	if cause != nil {
		return nil, errors.Wrapf(cause, "GetObject failed. key = %s", key)
	}

	return body, nil

	// r := bytes.NewReader(body)

	// ar, cause := aesStreamReader(r, s.config.Password)
	// if cause != nil {
	// 	return nil, errors.Wrapf(cause, "Decrypt failed. key = %s, Check your key", key)
	// }
	// gar, cause := gzip.NewReader(ar)
	// if cause != nil {
	// 	return nil, errors.Wrapf(cause, "Uncompress failed. key = %s", key)
	// }

	// return binaryObject(gar), nil
}

func (s *Session) Upload(key ObjectKey, value io.ReadSeeker) error {
	s.logger.Info("Upload", zap.String("key", key))
	// // TODO: error and close
	// buf := &bytes.Buffer{}
	// aw, err := aesStreamWriter(buf, s.config.Password)
	// if err != nil {
	// 	return err
	// }

	// gaw := gzip.NewWriter(aw)

	// _, err = gaw.Write(obj.Body())
	// if err != nil {
	// 	return err
	// }
	// gaw.Flush()
	// gaw.Close()
	// aw.Close()

	paramsPut := &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
		Body:   value,
	}
	//s.logger.Debug("buffer status", zap.Int("length", len(buf.Bytes())))
	_, cause := s.svc.PutObject(paramsPut)
	if cause != nil {
		return errors.Wrapf(cause, "PutObject failed. key = %s", key)
	}
	return nil
}

func (s *Session) IsExist(key ObjectKey) bool {
	paramsHead := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(key),
	}
	_, err := s.svc.HeadObject(paramsHead)
	return err == nil
}
