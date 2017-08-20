package bucketsync

import (
	"fmt"
	"os"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
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
	fmt.Println("Flush: ", string(f.objectkey), f.sess)

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
