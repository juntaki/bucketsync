package bucketsync

import (
	"fmt"
	"os"
	"testing"
)

func TestNewMeta(t *testing.T) {
	sess, err := NewSession()
	if err != nil {
		t.Error(err)
	}

	testList := []string{
		"",
		"dir",
		"dir/dir",
		"file",
		"link",
	}

	os.Chdir("/home/juntaki/.go/src/github.com/juntaki/bucketsync/lib/test-root")

	for _, path := range testList {
		fmt.Println("Start: ", path)
		m, err := CreateMetaFromFileSystem(path, sess)
		if err != nil {
			t.Error(err)
		}
		err = sess.RecursiveUpload(m)
		if err != nil {
			t.Error(err)
		}

		fmt.Println("path: ", m.Path(sess))
	}
}
