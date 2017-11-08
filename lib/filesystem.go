package bucketsync

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"time"

	"encoding/json"

	"github.com/hanwen/go-fuse/fuse"
)

// Meta is common struct for directory, file and symlink
type Meta struct {
	Size  int64     `json:"size"`
	Mode  uint32    `json:"mode"`
	UID   uint32    `json:"uid"`
	GID   uint32    `json:"gid"`
	Atime time.Time `json:"atime"`
	Ctime time.Time `json:"ctime"`
	Mtime time.Time `json:"mtime"`
}

// Node is common part of Directory, File, and SymLink
type Node struct {
	Key    ObjectKey `json:"key"`
	Parent ObjectKey `json:"parent"`
	Meta   Meta      `json:"meta"`
}

type Directory struct {
	Key      ObjectKey            `json:"key"`
	Parent   ObjectKey            `json:"parent"`
	Meta     Meta                 `json:"meta"`
	FileMeta map[string]ObjectKey `json:"children"`
	sess     *Session
}

func (o *Directory) Save() error {
	result, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return o.sess.Upload(o.Key, bytes.NewReader(result))
}

type File struct {
	Key    ObjectKey         `json:"key"`
	Parent ObjectKey         `json:"parent"`
	Meta   Meta              `json:"meta"`
	Extent map[string]Extent `json:"children"`
	sess   *Session
}

func (o *File) Save() error {
	for _, e := range o.Extent {
		if e.update {
			key := fmt.Sprintf("%x", sha256.Sum256(e.body))
			if o.sess.IsExist(key) {
				continue
			}
			err := o.sess.Upload(key, bytes.NewReader(e.body))
			if err != nil {
				return err
			}
		}
	}

	result, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return o.sess.Upload(o.Key, bytes.NewReader(result))
}

type Extent struct {
	Key    ObjectKey `json:"key"`
	update bool
	body   []byte // call Fill() to use this
	sess   *Session
}

type SymLink struct {
	Key    ObjectKey `json:"key"`
	Parent ObjectKey `json:"parent"`
	Meta   Meta      `json:"meta"`
	LinkTo string    `json:"linkto"`
	sess   *Session
}

func (o *SymLink) Save() error {
	result, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return o.sess.Upload(o.Key, bytes.NewReader(result))
}

func NewMeta(mode uint32, context *fuse.Context) Meta {
	meta := Meta{
		Mode:  mode,
		Size:  0,
		UID:   context.Uid,
		GID:   context.Gid,
		Atime: time.Now(),
		Ctime: time.Now(),
		Mtime: time.Now(),
	}
	return meta
}
