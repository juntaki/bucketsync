package bucketsync

import (
	"encoding/json"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Meta is common struct for directory, file and symlink
type Meta struct {
	bucketObject `json:"-"`
	Me           ObjectKey                    `json:"me"`
	Parent       ObjectKey                    `json:"parent"`
	Children     map[string]ObjectKey         `json:"children"`
	Size         int64                        `json:"size"`
	Mode         uint32                       `json:"mode"`
	UID          uint32                       `json:"uid"`
	GID          uint32                       `json:"gid"`
	Atime        time.Time                    `json:"atime"`
	Ctime        time.Time                    `json:"ctime"`
	Mtime        time.Time                    `json:"mtime"`
	Version      int                          `json:"version"`
	new          bool                         `json:"-"` // New object for S3?
	queue        *Queue                       `json:"-"` // Next() children
	cb           func(ObjectKey) bucketObject `json:"-"` // Next() callback
}

func (m *Meta) Key() string {
	return string(m.Me)
}

func (m *Meta) Body() []byte {
	binary, _ := json.Marshal(m)
	return binary
}

func (m *Meta) Next() bucketObject {
	if m.queue.Size() == 0 {
		if m.cb != nil {
			return m.cb(m.Me)
		} else {
			return nil
		}
	}
	return m.queue.Dequeue()
}

func (m *Meta) Status() UploadStatus {
	if m.queue.Size() == 0 {
		return Ready | Update
	}
	return Defer
}

func (m *Meta) extentFileCallback(o ObjectKey) bucketObject {
	m.Children[strconv.Itoa(len(m.Children)*ChunkSize)] = o
	return m
}

func (m *Meta) extentLinkCallback(o ObjectKey) bucketObject {
	m.Children["linkto"] = o
	return m
}

type MetaType int

const (
	Directory MetaType = iota
	RegularFile
	SymLink
)

func (m *Meta) Path(sess *Session) string {
	path := []string{}
	target := m
	for {
		bin, err := sess.Download(target.Parent)
		if err != nil {
			panic(err)
		}
		nextTarget, err := newMetaFromObject(bin)
		if err != nil {
			panic(err)
		}
		for k, v := range nextTarget.Children {
			if v == ObjectKey(target.Key()) {
				path = append(path, k)
				break
			}
		}
		if target.Parent == ObjectKey(RootKey) {
			break
		}
		target = nextTarget
	}

	rpath := make([]string, len(path))
	for i := 0; i < len(path); i++ {
		rpath[i] = path[len(path)-i-1]
	}

	return "/" + strings.Join(rpath, "/")
}

func (m *Meta) Type() MetaType {
	switch m.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		return Directory
	case syscall.S_IFREG:
		return RegularFile
	case syscall.S_IFLNK:
		return SymLink
	default:
		panic("Not implemented")
	}
}

func NewMetaFromObjectKey(key ObjectKey, sess *Session) (*Meta, error) {
	bin, err := sess.Download(key)
	if err != nil {
		return nil, err
	}
	return newMetaFromObject(bin)
}

func newMetaFromObject(r binaryObject) (*Meta, error) {
	m := &Meta{}
	err := json.NewDecoder(r).Decode(m)
	if err != nil {
		return nil, err
	}
	m.queue = NewQueue()
	return m, nil
}

func NewMetaFromPath(path string, sess *Session) (*Meta, error) {
	key, err := sess.PathWalk(path)
	if err != nil {
		return nil, err
	}

	bin, err := sess.Download(key)
	if err != nil {
		return nil, err
	}

	return newMetaFromObject(bin)
}
