package qmfsshard

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
)

type Key []byte

func (k Key) Shard(entityID string) []string {
	return Shard([]byte(k), entityID)
}

func GenerateKey() ([]byte, error) {
	rv := make([]byte, 32)
	_, err := rand.Read(rv)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func Shard(key []byte, entityID string) []string {
	data := append(key, []byte(entityID)...)
	hexdigest := fmt.Sprintf("%x", sha256.Sum256(data))
	return []string{hexdigest[:2], hexdigest[2:4]}
}
