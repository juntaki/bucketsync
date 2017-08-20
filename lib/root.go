package bucketsync

import (
	"encoding/json"
	"fmt"
)

// RootMeta is for root of object structure
type RootMeta struct {
	bucketObject `json:"-"`
	Root         ObjectKey `json:"root"`
	Version      int       `json:"version"`
}

func NewRootMeta(r binaryObject) *RootMeta {
	rm := &RootMeta{}
	err := json.NewDecoder(r).Decode(rm)
	if err != nil {
		panic(err)
	}
	return rm
}

func (r *RootMeta) Key() string {
	return RootKey
}

func (r *RootMeta) Body() []byte {
	binary, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(binary))
	return binary
}
