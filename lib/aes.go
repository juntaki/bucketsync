package bucketsync

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"io"

	"github.com/pkg/errors"
)

type Cipher struct {
	block cipher.Block
}

func NewCipher(password string) (*Cipher, error) {
	key := sha256.Sum256([]byte(password))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return &Cipher{
		block: block,
	}, nil
}

func (c *Cipher) StreamReader(in io.Reader, key ObjectKey) (cipher.StreamReader, error) {
	if len(key) < aes.BlockSize {
		return cipher.StreamReader{}, errors.New("key length is too short")
	}
	iv := []byte(key)[:aes.BlockSize]
	stream := cipher.NewCTR(c.block, iv)
	return cipher.StreamReader{S: stream, R: in}, nil
}

func (c *Cipher) StreamWriter(out io.Writer, key ObjectKey) (cipher.StreamWriter, error) {
	if len(key) < aes.BlockSize {
		return cipher.StreamWriter{}, errors.New("key length is too short")
	}
	iv := []byte(key)[:aes.BlockSize]
	stream := cipher.NewCTR(c.block, iv)
	return cipher.StreamWriter{S: stream, W: out}, nil
}
