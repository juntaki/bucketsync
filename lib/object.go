package bucketsync

import "io"

// Version of object data structure
const Version = 1
const RootKey = "root"
const ChunkSize = 1024 * 1024

type UploadStatus int

const (
	Ready UploadStatus = 1 << iota
	Defer
	Update
)

type bucketObject interface {
	Key() string
	Body() []byte
	Next() bucketObject
	Status() UploadStatus
}

// binaryObject is downloaded object reader
type binaryObject io.Reader

// ObjectKey is v4 UUID assgned to newly created object
type ObjectKey string
