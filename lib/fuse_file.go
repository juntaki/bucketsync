package bucketsync

import (
	"bytes"
	"sync"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/k0kubun/pp"
	"go.uber.org/zap"
)

// nodefs.File interface
type OpenedFile struct {
	nodefs.File
	file  *File
	dirty bool
}

func NewOpenedFile(file *File) *OpenedFile {
	return &OpenedFile{
		File:  nodefs.NewDefaultFile(),
		file:  file,
		dirty: false,
	}
}

func (f *OpenedFile) Flush() fuse.Status {
	f.file.sess.logger.Info("Flush")
	if f.dirty {
		f.file.Save()
		f.dirty = false
	}
	return fuse.OK
}

func (f *OpenedFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	f.file.sess.logger.Info("Read")

	// Calculate Extent index, offset
	// example: ExtentSize = 3, off = 8, len(dest) = 8
	//        ---|---|--=|===|===|=--|---
	// offset:012 345 678 901 234 567 890
	// index:  0   1   2   3   4   5   6
	//        012 012 012 012 012 012 012
	// firstIndex = 2, lastIndex = 5
	// startOffset = 2 endOffset = 0
	first := off / f.file.ExtentSize
	last := (int64(len(dest))+off)/f.file.ExtentSize + 1

	f.file.sess.logger.Debug("Read params", zap.Int64("last", last))

	startOffset := off - (first)*f.file.ExtentSize
	endOffset := (int64(len(dest)) + off) - last*f.file.ExtentSize - 1

	// Get extents concurrently
	extentBytes := make([][]byte, last-first+1)

	wg := sync.WaitGroup{}
	errc := make(chan error)
	done := make(chan struct{})
	for i := first; i <= last; i++ {
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
				errc <- err
			}
			extentBytes[bytesIndex] = extent.body
			wg.Done()
		}(i)
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-errc:
		return nil, fuse.EIO
	case <-done:
		// Trim
		extentBytes[0] = extentBytes[0][startOffset:len(extentBytes[0])]
		pp.Println(endOffset, extentBytes)
		extentBytes[len(extentBytes)-1] = extentBytes[len(extentBytes)-1][0 : endOffset-1]

		content := bytes.Join(extentBytes, []byte{})

		return &ReadResult{content: content, size: len(content)}, fuse.OK
	}
}

func (f *OpenedFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	f.file.sess.logger.Info("Write")
	f.dirty = true

	first := off / f.file.ExtentSize
	startOffset := off - (first)*f.file.ExtentSize

	pos := 0

	for i := first; pos < len(data); i++ {
		_, ok := f.file.Extent[i]
		if !ok {
			f.file.Extent[i] = &Extent{body: make([]byte, f.file.ExtentSize)}
		}
		f.file.Extent[i].dirty = true
		if i == first {
			pos += copy(f.file.Extent[i].body[startOffset:len(f.file.Extent[i].body)],
				data[pos:len(data)])
		} else {
			pos += copy(f.file.Extent[i].body,
				data[pos:len(data)])
		}
		f.file.Extent[i].Key = f.file.Extent[i].CurrentKey()
	}

	return uint32(len(data)), fuse.OK
}

func (f *OpenedFile) Release() {
	f.file.sess.logger.Info("Release")
	if f.dirty {
		f.file.Save()
		f.dirty = false
	}
}

func (f *OpenedFile) Fsync(flags int) (code fuse.Status) {
	f.file.sess.logger.Info("Fsync")
	if f.dirty {
		f.file.Save()
		f.dirty = false
	}
	return fuse.OK
}

func (f *OpenedFile) String() string {
	return f.file.Key
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
