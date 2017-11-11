package bucketsync

import (
	"fmt"
	"strings"
	"syscall"
	"time"

	"encoding/json"

	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/pkg/errors"
	"github.com/spaolacci/murmur3"
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
	cache  *Cache
}

func (s *Session) KeyGen(object []byte) ObjectKey {
	return fmt.Sprintf("%x", murmur3.Sum64(object))
}

func (s *Session) RootKey() ObjectKey {
	return s.KeyGen([]byte(s.config.Password))
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
		//LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	})

	bsess := &Session{
		svc:    svc,
		config: config,
		logger: logger,
		cache:  NewCache(),
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
	obj, err := s.DownloadWithCache(key)
	if err != nil {
		return nil, err
	}
	node := &Directory{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}
	node.sess = s
	return node, nil
}

func (s *Session) CreateFile(key, parent ObjectKey, mode uint32, context *fuse.Context) *File {
	return &File{
		Key:        key,
		Parent:     parent,
		Meta:       NewMeta(fuse.S_IFREG|mode, context),
		ExtentSize: ExtentSize,
		Extent:     make(map[int64]*Extent, 0),
		sess:       s,
	}
}

func (s *Session) NewFile(key ObjectKey) (*File, error) {
	obj, err := s.DownloadWithCache(key)
	if err != nil {
		return nil, err
	}
	node := &File{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}
	node.sess = s
	for _, e := range node.Extent {
		e.sess = s
	}

	s.logger.Debug("NewFile", zap.String("key", key),
		zap.Int("extent count", len(node.Extent)))
	return node, nil
}
func (s *Session) CreateExtent(size int64) *Extent {
	return &Extent{
		body: make([]byte, size),
		sess: s,
	}
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
	obj, err := s.DownloadWithCache(key)
	if err != nil {
		return nil, err
	}
	node := &SymLink{}
	err = json.Unmarshal(obj, node)
	if err != nil {
		return nil, err
	}
	node.sess = s
	return node, nil
}

func (s *Session) NewNode(key ObjectKey) (*Node, error) {
	obj, err := s.DownloadWithCache(key)
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
	obj, err := s.DownloadWithCache(key)
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
		node = &Directory{sess: s}
	case syscall.S_IFREG:
		node = &File{sess: s}
	case syscall.S_IFLNK:
		node = &SymLink{sess: s}
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
