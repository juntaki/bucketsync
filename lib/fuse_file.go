package bucketsync

import (
	"bytes"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"go.uber.org/zap"
)

// nodefs.File interface
type OpenedFile struct {
	nodefs.File
	file  *File
	dirty bool
	open  bool
}

func NewOpenedFile(file *File) *OpenedFile {
	return &OpenedFile{
		File:  nodefs.NewDefaultFile(),
		file:  file,
		dirty: false,
		open:  true,
	}
}

func (f *OpenedFile) Flush() fuse.Status {
	f.file.sess.logger.Debug("Flush")
	if f.dirty {
		f.file.Save()
		f.dirty = false
	}
	return fuse.OK
}

func (f *OpenedFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	f.file.sess.logger.Debug("Read")

	if off > f.file.Meta.Size {
		return nil, fuse.ENODATA
	}

	// Calculate Extent index, offset
	// example: ExtentSize = 3, off = 8, len(dest) = 8
	//        ---|---|--=|===|===|=--|---
	// offset:012 345 678 901 234 567 890
	// index:  0   1   2   3   4   5   6
	//        012 012 012 012 012 012 012
	// firstIndex = 2, lastIndex = 5
	// startOffset = 2 endOffset = 0
	first := off / f.file.ExtentSize
	last := (int64(len(dest)) + off) / f.file.ExtentSize

	startOffset := off - (first)*f.file.ExtentSize
	endOffset := (int64(len(dest)) + off) - last*f.file.ExtentSize - 1

	f.file.sess.logger.Debug("Read params", zap.Int64("first", first),
		zap.Int64("last", last),
		zap.Int64("startOffset", startOffset),
		zap.Int64("endOffset", endOffset))

	// Get extents concurrently
	extentBytes := make([][]byte, last-first+1)

	var wg sync.WaitGroup
	errc := make(chan error)
	done := make(chan struct{})
	for i := first; i <= last; i++ {
		f.file.sess.logger.Debug("Download thread started", zap.Int64("num", i))
		wg.Add(1)
		go func(i int64) {
			bytesIndex := i - first

			extent, ok := f.file.Extent[i]
			if !ok {
				// No extent means sparce area, fill zero.
				extentBytes[bytesIndex] = make([]byte, f.file.ExtentSize)
				wg.Done()
				return
			}
			err := extent.Fill()
			if err != nil {
				f.file.sess.logger.Error("Fill failed")
				errc <- err
			}
			extentBytes[bytesIndex] = extent.body
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		f.file.sess.logger.Debug("All download threads done")
		close(done)
	}()

	select {
	case <-errc:
		return nil, fuse.EIO
	case <-done:
		// Trim
		extentBytes[0] = extentBytes[0][startOffset:len(extentBytes[0])]
		content := bytes.Join(extentBytes, []byte{})
		copy(dest, content)

		f.file.sess.logger.Debug("wait done", zap.Int("content len", len(content)),
			zap.Int("dest len", len(dest)))
		return &ReadResult{content: dest, size: len(dest)}, fuse.OK
	}
}

func (f *OpenedFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	f.file.sess.logger.Debug("Write", zap.Int("datalen", len(data)),
		zap.Int64("offset", off))
	f.dirty = true

	first := off / f.file.ExtentSize
	startOffset := off - (first)*f.file.ExtentSize
	pos := 0

	f.file.sess.logger.Debug("Write/offset", zap.Int64("first", first),
		zap.Int64("startOffset", startOffset))

	for i := first; pos < len(data); i++ {
		if _, ok := f.file.Extent[i]; !ok {
			f.file.Extent[i] = f.file.sess.CreateExtent(f.file.ExtentSize)
		} else {
			f.file.Extent[i].Fill()
		}
		f.file.Extent[i].dirty = true

		if int64(len(f.file.Extent[i].body)) != f.file.ExtentSize {
			f.file.sess.logger.Error("Filled extent size is invalid",
				zap.Int("actual", len(f.file.Extent[i].body)),
				zap.Int64("expected", f.file.ExtentSize))
			return 0, fuse.EIO
		}
		if i == first {
			pos += copy(f.file.Extent[i].body[startOffset:len(f.file.Extent[i].body)], data[pos:len(data)])
			f.file.sess.logger.Debug("Write/position", zap.Int("pos", pos), zap.Int64("index", i))
		} else {
			pos += copy(f.file.Extent[i].body, data[pos:len(data)])
			f.file.sess.logger.Debug("Write/position", zap.Int("pos", pos), zap.Int64("index", i))
		}
		f.file.Extent[i].Key = f.file.Extent[i].CurrentKey()
	}

	if f.file.Meta.Size < off+int64(len(data)) {
		f.file.Meta.Size = off + int64(len(data))
	}

	return uint32(len(data)), fuse.OK
}

func (f *OpenedFile) Release() {
	f.file.sess.logger.Debug("Release")
	if f.dirty {
		f.file.Save()
		f.dirty = false
	}
	f.open = false
}

func (f *OpenedFile) Fsync(flags int) (code fuse.Status) {
	f.file.sess.logger.Debug("Fsync")
	if f.dirty {
		f.file.Save()
		f.dirty = false
	}
	return fuse.OK
}

func (f *OpenedFile) String() string {
	return f.file.Key
}

func (f *OpenedFile) Truncate(size uint64) fuse.Status {
	f.file.sess.logger.Debug("Truncate", zap.Uint64("size", size))
	if !f.open {
		return fuse.EBADF
	}
	f.file.Meta.Size = int64(size)
	return fuse.OK
}

func (f *OpenedFile) GetAttr(out *fuse.Attr) fuse.Status {
	f.file.sess.logger.Debug("GetAttr")
	if !f.open {
		return fuse.EBADF
	}

	out.Ino = InodeHash(f.file.Key)
	out.Size = uint64(f.file.Meta.Size)
	out.Mode = f.file.Meta.Mode
	out.Nlink = 1
	out.Owner = fuse.Owner{
		Uid: f.file.Meta.UID,
		Gid: f.file.Meta.GID,
	}
	out.SetTimes(&f.file.Meta.Atime, &f.file.Meta.Mtime, &f.file.Meta.Ctime)
	return fuse.OK
}

func (f *OpenedFile) Chown(uid uint32, gid uint32) fuse.Status {
	f.file.sess.logger.Debug("Chown")
	if !f.open {
		return fuse.EBADF
	}
	f.file.Meta.UID = uid
	f.file.Meta.GID = gid
	f.file.Meta.Ctime = time.Now()
	return fuse.OK
}

func (f *OpenedFile) Chmod(perms uint32) fuse.Status {
	f.file.sess.logger.Debug("Chmod")
	if !f.open {
		return fuse.EBADF
	}
	f.file.Meta.Mode = (f.file.Meta.Mode & syscall.S_IFMT) | perms
	f.file.Meta.Ctime = time.Now()
	return fuse.OK
}

func (f *OpenedFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	f.file.sess.logger.Debug("Utimens")
	if !f.open {
		return fuse.EBADF
	}
	f.file.Meta.Atime = *atime
	f.file.Meta.Mtime = *mtime
	f.file.Meta.Ctime = time.Now()
	return fuse.OK
}

func (f *OpenedFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	f.file.sess.logger.Debug("Allocate")
	if !f.open {
		return fuse.EBADF
	}
	return fuse.OK
}

type ReadResult struct {
	content []byte
	size    int
}

func (r *ReadResult) Bytes(buf []byte) ([]byte, fuse.Status) {
	return r.content, fuse.OK
}

func (r *ReadResult) Size() int {
	return r.size
}
func (r *ReadResult) Done() {

}
