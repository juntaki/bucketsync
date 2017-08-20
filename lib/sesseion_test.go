package bucketsync

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestSession(t *testing.T) {
	sess, err := NewSession()
	if err != nil {
		t.Error(err)
	}
	if sess.root == nil {
		t.Error(sess)
	}

	_, err = sess.PathWalk("")
	if err != nil {
		t.Error(err)
	}
}

func TestDecrypt(t *testing.T) {
	buf := &bytes.Buffer{}
	aw, err := aesStreamWriter(buf)
	if err != nil {
		t.Fatal(err)
	}

	gaw := gzip.NewWriter(aw)

	_, err = gaw.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	gaw.Flush()
	gaw.Close()
	aw.Close()

	b := buf.Bytes()

	br := bytes.NewReader(b)

	if err != nil {
		t.Fatal(err)
	}
	ar, err := aesStreamReader(br)
	if err != nil {
		t.Fatal(err)
	}
	gar, err := gzip.NewReader(ar)
	if err != nil {
		t.Fatal(err)
	}
	bin, err := ioutil.ReadAll(gar)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(bin)
}

func TestUpDownload(t *testing.T) {
	sess, err := NewSession()
	if err != nil {
		t.Error(err)
	}

	r := bytes.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	ext := NewExtent(r, func(o ObjectKey) bucketObject { return nil })

	err = sess.Upload(ext)
	if err != nil {
		t.Fatal(err)
	}
	b, err := sess.Download(ObjectKey(ext.Key()))
	if err != nil {
		t.Fatal(err)
	}

	read, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(read)
}
