package bucketsync

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

func (m *Meta) WriteToFilesystem(sess *Session, fp *os.File) error {
	sess.logger.Debug("WriteToFilesystem", zap.Any("children", m.Children))
	switch m.Type() {
	case Directory:
	case RegularFile:
		sess.logger.Info("Write to tmpfile")
		for offset, key := range m.Children {
			bin, err := sess.Download(key)
			if err != nil {
				panic(err)
			}
			b, err := ioutil.ReadAll(bin)
			if err != nil {
				panic(err)
			}
			off, _ := strconv.Atoi(offset)
			size, err := fp.WriteAt(b, int64(off))
			sess.logger.Debug("Current file status", zap.Int("size", size))
			if err != nil {
				panic(err)
			}
		}
	case SymLink:
	}
	return nil
}

func (m *Meta) UpdateChildrenByFileSystem(relPath string) error {
	absPath, _ := filepath.Abs(relPath)
	stat, err := os.Lstat(absPath)
	if err != nil {
		return err
	}

	// TODO: other member
	m.Size = stat.Size()
	// initialize
	m.Children = make(map[string]ObjectKey)

	switch m.Type() {
	case Directory:
	case RegularFile:
		file, err := os.Open(absPath)
		if err != nil {
			return err
		}
		// TODO: close
		for i := int64(0); i < m.Size; i += ChunkSize {
			// Create Extent
			sr := io.NewSectionReader(file, i, ChunkSize)
			e := NewExtent(sr, m.extentFileCallback)
			m.queue.Enqueue(e)
		}
	case SymLink:
	}
	return nil
}

func NewMeta(me ObjectKey, parent ObjectKey, mode uint32, context *fuse.Context) *Meta {
	meta := &Meta{
		Me:       me,
		new:      true,
		Parent:   parent,
		Children: make(map[string]ObjectKey), // initialize
		Mode:     mode,
		Size:     0,
		UID:      context.Uid,
		GID:      context.Gid,
		Atime:    time.Now(),
		Ctime:    time.Now(),
		Mtime:    time.Now(),
		Version:  Version,
		queue:    NewQueue(),
	}
	return meta
}

func CreateMetaFromFileSystem(relPath string, sess *Session) (*Meta, error) {
	sess.logger.Info("CreateMetaFromFileSystem", zap.String("relPath", relPath))
	absPath, _ := filepath.Abs(relPath)
	stat, err := os.Lstat(absPath)
	if err != nil {
		return nil, err
	}
	sstat := stat.Sys().(*syscall.Stat_t)
	if sstat == nil {
		return nil, errors.New("syscall")
	}
	// Parent must exist

	parentPath := filepath.Dir(absPath)
	wd, _ := os.Getwd()
	parentPath, _ = filepath.Rel(wd, parentPath)

	parent, err := sess.PathWalk(parentPath)
	if err != nil {
		return nil, err
	}
	// Am I exist?
	new := false
	me, err := sess.PathWalk(relPath)
	if err != nil {
		// TODO only if Not found
		// parent will be fixed too
		sess.logger.Info("uuid not assigned")
		new = true
		me = ObjectKey(uuid.NewV4().String())
	}

	sess.logger.Info("uuid assigned", zap.String("uuid", me))

	meta := &Meta{
		Me:       me,
		new:      new,
		Parent:   parent,
		Children: make(map[string]ObjectKey), // initialize
		Mode:     sstat.Mode,
		Size:     stat.Size(),
		UID:      sstat.Uid,
		GID:      sstat.Gid,
		Atime:    time.Unix(sstat.Ctim.Sec, sstat.Ctim.Nsec),
		Ctime:    time.Unix(sstat.Atim.Sec, sstat.Atim.Nsec),
		Mtime:    stat.ModTime(),
		Version:  Version,
		queue:    NewQueue(),
	}

	// Children
	switch meta.Type() {
	case Directory:
		files, err := ioutil.ReadDir(absPath)
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			UUID, err := sess.PathWalk(filepath.Join(relPath, f.Name()))
			if err != nil {
				// TODO only if Not found
				UUID = ObjectKey(uuid.NewV4().String())
			}
			meta.Children[(f.Name())] = UUID
		}
	case RegularFile:
		file, err := os.Open(absPath)
		// TODO: close
		if err != nil {
			return nil, err
		}
		for i := int64(0); i < meta.Size; i += ChunkSize {
			// Create Extent
			sr := io.NewSectionReader(file, i, ChunkSize)
			e := NewExtent(sr, meta.extentFileCallback)
			meta.queue.Enqueue(e)
		}
	case SymLink:
		linkTo, err := os.Readlink(absPath)
		if err != nil {
			return nil, err
		}
		linkContent := bytes.NewReader([]byte(linkTo))
		e := NewExtent(linkContent, meta.extentLinkCallback)
		meta.queue.Enqueue(e)
	default:
		panic("not implemented")
	}
	return meta, nil
}
