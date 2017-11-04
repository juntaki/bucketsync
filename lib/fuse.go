package bucketsync

import (
	"bytes"
	"hash/fnv"
	"io/ioutil"
	"os"
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

func (f *FileSystem) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {

	f.Sess.logger.Info("GetAttr", zap.String("name", name))
	meta, err := NewMetaFromPath(name, f.Sess)
	if err != nil {
		if IsKeyNotFound(err) {
			return nil, fuse.ENOENT
		}
		panic(err)
	}
	attr := &fuse.Attr{
		Ino:   InodeHash(ObjectKey(meta.Key())),
		Size:  uint64(meta.Size),
		Mode:  meta.Mode,
		Nlink: 1,
		Owner: fuse.Owner{
			Uid: meta.UID,
			Gid: meta.GID,
		},
	}
	attr.SetTimes(&meta.Atime, &meta.Mtime, &meta.Ctime)
	return attr, fuse.OK
}

func (f *FileSystem) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {

	f.Sess.logger.Info("Open", zap.String("name", name))
	key, err := f.Sess.PathWalk(name)
	if err != nil {
		if IsKeyNotFound(err) {
			return nil, fuse.ENOENT
		}
		panic(err)
	}
	meta, err := NewMetaFromObjectKey(key, f.Sess)
	if err != nil {
		panic(err)
	}
	// Create tmp file
	fp, err := os.Create(string(key))
	if err != nil {
		panic(err)
	}
	meta.WriteToFilesystem(f.Sess, fp)
	file = &File{
		File:      nodefs.NewLoopbackFile(fp),
		sess:      f.Sess,
		objectkey: key,
		tmpfile:   fp,
		meta:      meta,
	}
	return file, fuse.OK
}

func (f *FileSystem) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	f.Sess.logger.Info("Rename", zap.String("oldName", oldName), zap.String("newName", newName))

	key, err := f.Sess.PathWalk(oldName)
	if err != nil {
		if IsKeyNotFound(err) {
			return fuse.ENOENT
		}
		panic(err)
	}

	// New name
	parentNew := filepath.Dir(newName)
	parentNewMeta, err := NewMetaFromPath(parentNew, f.Sess)
	if err != nil {
		panic(err)
	}
	parentNewMeta.Children[filepath.Base(newName)] = key
	f.Sess.Upload(parentNewMeta)

	// Old name
	parentOld := filepath.Dir(oldName)
	parentOldMeta, err := NewMetaFromPath(parentOld, f.Sess)
	if err != nil {
		panic(err)
	}
	delete(parentOldMeta.Children, filepath.Base(oldName))
	f.Sess.Upload(parentOldMeta)

	return fuse.OK
}

func (f *FileSystem) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {

	f.Sess.logger.Info("Mkdir", zap.String("name", name))
	// Add new uuid to parent object
	parent := filepath.Dir(name)
	parentMeta, err := NewMetaFromPath(parent, f.Sess)
	if err != nil {
		panic(err)
	}
	f.Sess.logger.Info("uuid not assigned")

	key := ObjectKey(uuid.NewV4().String())

	parentMeta.Children[filepath.Base(name)] = key
	f.Sess.Upload(parentMeta)

	// New File
	meta := NewMeta(key, ObjectKey(parentMeta.Key()), fuse.S_IFDIR|mode, context)
	f.Sess.Upload(meta)
	return fuse.OK
}

func (f *FileSystem) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {

	f.Sess.logger.Info("Symlink",
		zap.String("value", value),
		zap.String("linkName", linkName))

	// Add new uuid to parent object
	parent := filepath.Dir(linkName)
	parentMeta, err := NewMetaFromPath(parent, f.Sess)
	if err != nil {
		panic(err)
	}
	f.Sess.logger.Info("uuid not assigned")

	key := ObjectKey(uuid.NewV4().String())

	parentMeta.Children[filepath.Base(linkName)] = key
	f.Sess.Upload(parentMeta)

	// Upload symlink
	meta := NewMeta(key, ObjectKey(parentMeta.Key()), fuse.S_IFLNK, context)
	linkContent := bytes.NewReader([]byte(value))
	e := NewExtent(linkContent, meta.extentLinkCallback)
	meta.queue.Enqueue(e)
	f.Sess.RecursiveUpload(meta)

	return fuse.OK
}

func (f *FileSystem) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	// TODO: flags??

	f.Sess.logger.Info("Create",
		zap.String("name", name),
		zap.Uint32("flags", flags),
		zap.Uint32("mode", mode),
	)

	// Add new uuid to parent object
	parent := filepath.Dir(name)
	parentMeta, err := NewMetaFromPath(parent, f.Sess)
	if err != nil {
		panic(err)
	}
	f.Sess.logger.Info("uuid not assigned")

	key := ObjectKey(uuid.NewV4().String())

	parentMeta.Children[filepath.Base(name)] = key
	f.Sess.Upload(parentMeta)

	// New File
	meta := NewMeta(key, ObjectKey(parentMeta.Key()), fuse.S_IFREG|mode, context)
	f.Sess.Upload(meta)
	fp, err := os.Create(string(key))
	if err != nil {
		panic(err)
	}
	meta.WriteToFilesystem(f.Sess, fp)
	file = &File{
		File:      nodefs.NewLoopbackFile(fp),
		sess:      f.Sess,
		objectkey: key,
		tmpfile:   fp,
		meta:      meta,
	}

	return file, fuse.OK
}

func (f *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {

	f.Sess.logger.Info("OpenDir", zap.String("name", name))
	meta, err := NewMetaFromPath(name, f.Sess)
	if err != nil {
		if IsKeyNotFound(err) {
			return nil, fuse.ENOENT
		}
		panic(err)
	}

	if meta.Type() != Directory {
		return nil, fuse.ENOTDIR
	}

	stream = make([]fuse.DirEntry, 0)
	for name, objkey := range meta.Children {
		// TODO: Is it OK?
		// meta, err := NewMetaFromObjectKey(objkey, f.Sess)
		// if err != nil {
		// 	panic(err)
		// }
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
	meta, err := NewMetaFromPath(name, f.Sess)
	if err != nil {
		panic(err)
	}
	f.Sess.logger.Info("Current Mode", zap.Uint32("mode", meta.Mode))
	meta.Mode = (meta.Mode & syscall.S_IFMT) | mode
	f.Sess.logger.Info("New mode", zap.Uint32("mode", meta.Mode))
	meta.Ctime = time.Now()
	f.Sess.Upload(meta)
	return fuse.OK
}

func (f *FileSystem) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {

	f.Sess.logger.Info("Chown", zap.String("name", name))
	meta, err := NewMetaFromPath(name, f.Sess)
	if err != nil {
		panic(err)
	}
	meta.UID = uid
	meta.GID = gid
	meta.Ctime = time.Now()
	f.Sess.Upload(meta)
	return fuse.OK
}

func (f *FileSystem) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {

	f.Sess.logger.Info("Utimens", zap.String("name", name))
	meta, err := NewMetaFromPath(name, f.Sess)
	if err != nil {
		panic(err)
	}
	meta.Atime = *Atime
	meta.Mtime = *Mtime
	meta.Ctime = time.Now()
	f.Sess.Upload(meta)
	return fuse.OK
}

func (f *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {

	f.Sess.logger.Info("Access",
		zap.String("name", name),
		zap.Uint32("mode", mode),
	)
	// TODO:
	return fuse.OK
}

func (f *FileSystem) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {

	f.Sess.logger.Info("Truncate", zap.String("name", name))
	meta, err := NewMetaFromPath(name, f.Sess)
	if err != nil {
		panic(err)
	}
	meta.Size = int64(size)
	//TODO: Update Children
	f.Sess.Upload(meta)
	return fuse.OK
}

func (f *FileSystem) Readlink(name string, context *fuse.Context) (string, fuse.Status) {

	f.Sess.logger.Info("Readlink", zap.String("name", name))
	meta, err := NewMetaFromPath(name, f.Sess)

	bin, err := f.Sess.Download(meta.Children["linkto"])
	if err != nil {
		panic(err)
	}

	link, err := ioutil.ReadAll(bin)
	if err != nil {
		panic(err)
	}
	return string(link), fuse.OK
}

func (f *FileSystem) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	return f.Unlink(name, context)
}

func (f *FileSystem) Unlink(name string, context *fuse.Context) (code fuse.Status) {

	f.Sess.logger.Info("Unlink", zap.String("name", name))
	parent := filepath.Dir(name)
	meta, err := NewMetaFromPath(parent, f.Sess)
	if err != nil {
		panic(err)
	}
	delete(meta.Children, filepath.Base(name))
	// TODO: Delete meta on S3
	f.Sess.Upload(meta)
	return fuse.OK
}

// TODO
// func (f *FileSystem) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status)
// func (f *FileSystem) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status
// func (f *FileSystem) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status)
// func (f *FileSystem) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status)
// func (f *FileSystem) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status
// func (f *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status
// func (f *FileSystem) StatFs(name string) *fuse.StatfsOut
