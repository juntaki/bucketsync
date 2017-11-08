package bucketsync

import (
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

// nodefs.File interface
type OpenedFile struct {
	nodefs.File
	file *File
}

func (f *OpenedFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *OpenedFile) String() string {
	return f.file.Key
}

func (f *OpenedFile) InnerFile() nodefs.File {
	return f
}

func (f *OpenedFile) SetInode(*nodefs.Inode) {

}

func (f *OpenedFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	return &ReadResult{}, fuse.OK
}

func (f *OpenedFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	return uint32(len(data)), fuse.OK
}

func (f *OpenedFile) Flock(flags int) fuse.Status {
	return fuse.OK
}

func (f *OpenedFile) Release() {

}
func (f *OpenedFile) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}
func (f *OpenedFile) Truncate(size uint64) fuse.Status {
	return fuse.OK
}
func (f *OpenedFile) GetAttr(out *fuse.Attr) fuse.Status {
	return fuse.OK

}
func (f *OpenedFile) Chown(uid uint32, gid uint32) fuse.Status {
	return fuse.OK

}
func (f *OpenedFile) Chmod(perms uint32) fuse.Status {
	return fuse.OK

}
func (f *OpenedFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	return fuse.OK

}
func (f *OpenedFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.OK
}

type ReadResult struct {
	bytes []byte
}

func (r *ReadResult) Bytes(buf []byte) ([]byte, fuse.Status) {
	return r.bytes, fuse.OK
}
func (r *ReadResult) Size() int {
	return len(r.bytes)
}
func (r *ReadResult) Done() {

}
