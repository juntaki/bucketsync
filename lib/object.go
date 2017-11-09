package bucketsync

import "io"

// Version of object data structure
const ExtentSize = 1024 * 1024

type bucketObject interface {
	Key() string
	Body() []byte
	Next() bucketObject
}

// binaryObject is downloaded object reader
type binaryObject io.Reader

// ObjectKey is v4 UUID assgned to newly created object
type ObjectKey = string
