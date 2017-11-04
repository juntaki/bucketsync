package bucketsync

import (
	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"go.uber.org/zap"
)

type File struct {
	nodefs.File
	sess      *Session
	path      string
	objectkey ObjectKey
	tmpfile   *os.File
	meta      *Meta
}

func (f *File) Flush() fuse.Status {
	f.sess.logger.Info("Flush", zap.String("key", f.objectkey))

	err := f.meta.UpdateChildrenByFileSystem(string(f.objectkey))
	if err != nil {
		panic(err)
	}
	err = f.sess.RecursiveUpload(f.meta)
	if err != nil {
		panic(err)
	}
	//TODO: Close
	return fuse.OK
}

func (f *File) String() string {
	return string(f.objectkey)
}

type ReadResult struct {
}
