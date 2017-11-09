package bucketsync

import (
	"hash/fnv"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
)

type FileSystem struct {
	pathfs.FileSystem
	Sess *Session
}

func NewFileSystem(config *Config) *pathfs.PathNodeFs {
	sess, err := NewSession(config)
	if err != nil {
		panic(err)
	}

	fs := &FileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		Sess:       sess,
	}
	return pathfs.NewPathNodeFs(fs, nil)
}

func InodeHash(o ObjectKey) uint64 {
	h := fnv.New64a()
	h.Write([]byte(o))
	return h.Sum64()
}

func NewObjectKey() ObjectKey {
	return uuid.NewV4().String()
}

func (f *FileSystem) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	f.Sess.logger.Info("GetAttr", zap.String("name", name))

	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	node, err := f.Sess.NewNode(key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	attr := &fuse.Attr{
		Ino:   InodeHash(key),
		Size:  uint64(node.Meta.Size),
		Mode:  node.Meta.Mode,
		Nlink: 1,
		Owner: fuse.Owner{
			Uid: node.Meta.UID,
			Gid: node.Meta.GID,
		},
	}
	attr.SetTimes(&node.Meta.Atime, &node.Meta.Mtime, &node.Meta.Ctime)
	return attr, fuse.OK
}

func (f *FileSystem) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	f.Sess.logger.Info("Open", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	node, err := f.Sess.NewFile(key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	return NewOpenedFile(node), fuse.OK
}

func (f *FileSystem) getParent(name string) (*Directory, fuse.Status) {
	parent := filepath.Dir(name)
	key, err := f.Sess.PathWalk(parent)
	if err != nil {
		return nil, fuse.ENOENT
	}
	dir, err := f.Sess.NewDirectory(key)
	if err != nil {
		return nil, fuse.EACCES
	}
	return dir, fuse.OK
}

func (f *FileSystem) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Rename", zap.String("oldName", oldName), zap.String("newName", newName))

	// Get old dir
	dirOld, status := f.getParent(oldName)
	if status != fuse.OK {
		return status
	}

	// Get new dir
	dirNew, status := f.getParent(newName)
	if status != fuse.OK {
		return status
	}

	// Rename
	dirNew.FileMeta[filepath.Base(newName)] = dirOld.FileMeta[filepath.Base(newName)]
	delete(dirOld.FileMeta, filepath.Base(oldName))

	// Save
	err := dirNew.Save()
	if err != nil {
		return fuse.EIO
	}
	err = dirOld.Save()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	f.Sess.logger.Info("Mkdir", zap.String("name", name))

	dir, status := f.getParent(name)
	if status != fuse.OK {
		return status
	}

	// Set
	newKey := NewObjectKey()
	dir.FileMeta[filepath.Base(name)] = newKey

	newDir := f.Sess.CreateDirectory(newKey, dir.Key, mode, context)

	// Save
	err := newDir.Save()
	if err != nil {
		return fuse.EIO
	}
	err = dir.Save()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Symlink",
		zap.String("value", value),
		zap.String("linkName", linkName))

	dir, status := f.getParent(linkName)
	if status != fuse.OK {
		return status
	}

	// Set
	newKey := NewObjectKey()
	dir.FileMeta[filepath.Base(linkName)] = newKey
	symlink := f.Sess.CreateSymLink(newKey, dir.Key, value, context)

	// Save
	err := symlink.Save()
	if err != nil {
		return fuse.EIO
	}
	err = dir.Save()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	// TODO: flags??
	f.Sess.logger.Info("Create",
		zap.String("name", name),
		zap.Uint32("flags", flags),
		zap.Uint32("mode", mode),
	)

	dir, status := f.getParent(name)
	if status != fuse.OK {
		return nil, status
	}

	// Set
	newKey := NewObjectKey()
	dir.FileMeta[filepath.Base(name)] = newKey

	file := f.Sess.CreateFile(newKey, dir.Key, mode, context)

	err := file.Save()
	if err != nil {
		return nil, fuse.EIO
	}
	err = dir.Save()
	if err != nil {
		return nil, fuse.EIO
	}
	return NewOpenedFile(file), fuse.OK
}

func (f *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	f.Sess.logger.Info("OpenDir", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return nil, fuse.ENOENT
	}

	dir, err := f.Sess.NewDirectory(key)
	if err != nil {
		return nil, fuse.ENOENT
	}

	stream = make([]fuse.DirEntry, 0)
	for name, objkey := range dir.FileMeta {
		dentry := fuse.DirEntry{
			Name: name,
			Ino:  InodeHash(objkey),
		}
		stream = append(stream, dentry)
	}
	return stream, fuse.OK
}

func (f *FileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {
}

func (f *FileSystem) OnUnmount() {
	// TODO: close connection
}

func (f *FileSystem) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Chmod", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	node, err := f.Sess.NewTypedNode(key)
	if err != nil {
		return fuse.ENOENT
	}

	switch typed := node.(type) {
	case *Directory:
		typed.Meta.Mode = (typed.Meta.Mode & syscall.S_IFMT) | mode
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *File:
		typed.Meta.Mode = (typed.Meta.Mode & syscall.S_IFMT) | mode
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *SymLink:
		typed.Meta.Mode = (typed.Meta.Mode & syscall.S_IFMT) | mode
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	}
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Chown", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	node, err := f.Sess.NewTypedNode(key)
	if err != nil {
		return fuse.ENOENT
	}

	switch typed := node.(type) {
	case *Directory:
		typed.Meta.UID = uid
		typed.Meta.GID = gid
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *File:
		typed.Meta.UID = uid
		typed.Meta.GID = gid
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *SymLink:
		typed.Meta.UID = uid
		typed.Meta.GID = gid
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	}
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Utimens", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	node, err := f.Sess.NewTypedNode(key)
	if err != nil {
		return fuse.ENOENT
	}

	switch typed := node.(type) {
	case *Directory:
		typed.Meta.Atime = *Atime
		typed.Meta.Mtime = *Mtime
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *File:
		typed.Meta.Atime = *Atime
		typed.Meta.Mtime = *Mtime
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	case *SymLink:
		typed.Meta.Atime = *Atime
		typed.Meta.Mtime = *Mtime
		typed.Meta.Ctime = time.Now()
		err = typed.Save()
	}
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK

}

func (f *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Access",
		zap.String("name", name),
		zap.Uint32("mode", mode),
	)

	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	if f.Sess.IsExist(key) {
		return fuse.OK
	}
	return fuse.ENOENT
}

func (f *FileSystem) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Truncate", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return fuse.ENOENT
	}

	node, err := f.Sess.NewFile(key)
	if err != nil {
		return fuse.ENOENT
	}

	node.Meta.Size = int64(size)
	err = node.Save()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *FileSystem) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	f.Sess.logger.Info("Readlink", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		return "", fuse.ENOENT
	}

	node, err := f.Sess.NewSymLink(key)
	if err != nil {
		return "", fuse.ENOENT
	}

	return node.LinkTo, fuse.OK
}

func (f *FileSystem) Rmdir(name string, context *fuse.Context) (code fuse.Status) {

	return f.Unlink(name, context)
}

func (f *FileSystem) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Unlink", zap.String("name", name))
	dir, status := f.getParent(name)
	if status != fuse.OK {
		return status
	}

	delete(dir.FileMeta, filepath.Base(name))

	err := dir.Save()
	if err != nil {
		return fuse.EIO
	}

	return fuse.OK
}

func (f *FileSystem) String() string {
	return "bucketsync"
}

// // TODO
// func (f *FileSystem) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
// 	return nil, fuse.OK
// }
// func (f *FileSystem) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status) {
// 	return nil, fuse.OK
// }
// func (f *FileSystem) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
// 	return fuse.OK
// }
// func (f *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
// 	return fuse.OK
// }
// func (f *FileSystem) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
// 	return fuse.OK
// }
// func (f *FileSystem) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
// 	return fuse.OK
// }
// func (f *FileSystem) StatFs(name string) *fuse.StatfsOut {
// 	return nil
// }
// func (f *FileSystem) SetDebug(debug bool) {
// }
