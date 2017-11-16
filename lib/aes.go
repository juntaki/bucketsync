package bucketsync

import (
	"crypto/aes"
	"crypto/cipher"
	"io"
)

type Cipher struct {
	block cipher.Block
}

func NewCiper(password string) (*Cipher, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return &Cipher{
		block: block,
	}, nil
}

func (c *Cipher) StreamReader(in io.Reader, iv []byte) (cipher.StreamReader, error) {
	iv = iv[:aes.BlockSize]
	stream := cipher.NewCTR(c.block, iv)
	return cipher.StreamReader{S: stream, R: in}, nil
}

func (c *Cipher) StreamWriter(out io.Writer, iv []byte) (cipher.StreamWriter, error) 
	iv = iv[:aes.BlockSize]
	stream := cipher.NewCTR(c.block, iv)
	return cipher.StreamWriter{S: stream, W: out}, nil
}
