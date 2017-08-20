package bucketsync

import (
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
)

// Extent is 10MB chunk of file
// >10MB chunk is filled by zero.
type Extent struct {
	bucketObject
	reader io.ReadSeeker

	key    string // cache
	body   []byte // cache
	status UploadStatus
	cb     func(ObjectKey) bucketObject // Next() callback
}

func NewExtent(r io.ReadSeeker, cb func(ObjectKey) bucketObject) *Extent {
	extent := &Extent{
		reader: r,
		status: Defer,
		cb:     cb,
	}

	return extent
}

func (e *Extent) calculate() {
	e.reader.Seek(0, 0)
	e.body, _ = ioutil.ReadAll(e.reader)
	e.key = fmt.Sprintf("%x", sha256.Sum256(e.body))
}

func (e *Extent) Key() string {
	if len(e.key) == 0 {
		e.calculate()
	}
	return e.key
}

func (e *Extent) Body() []byte {
	if len(e.body) == 0 {
		e.calculate()
	}
	return e.body
}

func (e *Extent) Next() bucketObject {
	return e.cb(ObjectKey(e.Key()))
}

func (e *Extent) Status() UploadStatus {
	return Ready
}
