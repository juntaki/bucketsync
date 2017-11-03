package bucketsync

import (
	"bytes"
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type Session struct {
	svc    *s3.S3
	root   *RootMeta
	config *Config
}

type Config struct {
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Password  string `yaml:"password"`
}

func (c *Config) validate() bool {
	return true
}

func NewSession(config *Config) (*Session, error) {
	if !config.validate() {
		return nil, errors.New("Invalid config")
	}
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess, &aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKey, config.SecretKey, ""),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	})

	bsess := &Session{
		svc:    svc,
		config: config,
	}

	r, err := bsess.Download(RootKey)
	// Create root key, if it's not exist.
	// TODO: only not exist error
	if err != nil {
		if IsKeyNotFound(err) {
			fmt.Println(err)
			rm := &RootMeta{
				Root:    ObjectKey(uuid.NewV4().String()),
				Version: Version,
			}
			err = bsess.Upload(rm)
			if err != nil {
				return nil, err
			}
			bsess.root = rm

			rootdir := &Meta{
				Me:       rm.Root,
				new:      true,
				Parent:   RootKey,
				Children: make(map[string]ObjectKey), // initialize
				Mode:     fuse.S_IFDIR | 0755,
				Size:     0,
				UID:      0,
				GID:      0,
				Atime:    time.Now(),
				Ctime:    time.Now(),
				Mtime:    time.Now(),
				Version:  Version,
				queue:    NewQueue(),
			}
			err = bsess.Upload(rootdir)
			if err != nil {
				return nil, err
			}
			return bsess, nil
		} else {
			panic(err)
		}
	}
	bsess.root = NewRootMeta(r)
	fmt.Println("RootUUID: ", bsess.root.Root)
	return bsess, nil
}

func (s *Session) PathWalk(relPath string) (ObjectKey, error) {
	// parent of root
	if relPath == ".." {
		return ObjectKey(RootKey), nil
	}
	// root
	if relPath == "." || relPath == "" {
		return s.root.Root, nil
	}

	fmt.Println("relPath: ", relPath)

	r, err := s.Download(s.root.Root)
	if err != nil {
		return "", err
	}
	currentMeta, err := newMetaFromObject(r)
	if err != nil {
		return "", err
	}

	var uuid ObjectKey

	pathList := strings.Split(relPath, "/")
	fmt.Println("walk: ", pathList)
	for i, p := range pathList {
		var ok bool
		uuid, ok = currentMeta.Children[p]
		if !ok {
			return "", newErrorKeyNotFound(nil, ObjectKey(p))
		}
		if i == len(pathList)-1 {
			break
		}

		r, err := s.Download(uuid)
		if err != nil {
			return "", err
		}
		currentMeta, err = newMetaFromObject(r)
		if err != nil {
			return "", err
		}
	}

	fmt.Println("uuid = ", uuid)
	return uuid, nil
}

func aesStreamReader(in io.Reader, AESKey string) (cipher.StreamReader, error) {
	key := []byte(AESKey)

	block, err := aes.NewCipher(key)
	if err != nil {
		return cipher.StreamReader{}, err
	}
	var iv [aes.BlockSize]byte
	stream := cipher.NewOFB(block, iv[:])

	return cipher.StreamReader{S: stream, R: in}, nil
}

func aesStreamWriter(out io.Writer, AESKey string) (cipher.StreamWriter, error) {
	key := []byte(AESKey)

	block, err := aes.NewCipher(key)
	if err != nil {
		return cipher.StreamWriter{}, err
	}
	var iv [aes.BlockSize]byte
	stream := cipher.NewOFB(block, iv[:])

	return cipher.StreamWriter{S: stream, W: out}, nil
}

func (s *Session) Download(key ObjectKey) (binaryObject, error) {
	fmt.Println("Download: ", key)
	paramsGet := &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(string(key)),
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

	// S3 bug?
	body, cause := ioutil.ReadAll(obj.Body)
	if cause != nil {
		return nil, errors.Wrapf(cause, "GetObject failed. key = %s", key)
	}

	r := bytes.NewReader(body)

	ar, cause := aesStreamReader(r, s.config.Password)
	if cause != nil {
		return nil, errors.Wrapf(cause, "Decrypt failed. key = %s, Check your key", key)
	}
	gar, cause := gzip.NewReader(ar)
	if cause != nil {
		return nil, errors.Wrapf(cause, "Uncompress failed. key = %s", key)
	}

	return binaryObject(gar), nil
}

func (s *Session) RecursiveUpload(targetObject bucketObject) error {
	currentObject := targetObject
	for {
		status := currentObject.Status()
		if status&Ready != 0 {
			if status&Update == 0 && s.IsExist(ObjectKey(currentObject.Key())) {
				fmt.Println("exist: ", currentObject.Key())
			} else {
				err := s.Upload(currentObject)
				if err != nil {
					return err
				}
			}
		}
		currentObject = currentObject.Next()
		if currentObject == nil {
			break
		}
	}
	return nil
}

func (s *Session) Upload(obj bucketObject) error {
	fmt.Println("Uploading: ", obj.Key())

	// TODO: error and close
	buf := &bytes.Buffer{}
	aw, err := aesStreamWriter(buf, s.config.Password)
	if err != nil {
		return err
	}

	gaw := gzip.NewWriter(aw)

	_, err = gaw.Write(obj.Body())
	if err != nil {
		return err
	}
	gaw.Flush()
	gaw.Close()
	aw.Close()

	paramsPut := &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(obj.Key()),
		Body:   bytes.NewReader(buf.Bytes()),
	}
	fmt.Println("DEBUG:", "len:", len(buf.Bytes()))
	_, cause := s.svc.PutObject(paramsPut)
	if cause != nil {
		return errors.Wrapf(cause, "PutObject failed. key = %s", obj.Key())
	}
	return nil
}

func (s *Session) IsExist(key ObjectKey) bool {
	paramsHead := &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(string(key)),
	}
	_, err := s.svc.HeadObject(paramsHead)
	return err == nil
}
