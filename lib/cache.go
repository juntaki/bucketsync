package bucketsync

type Cache struct {
	cache map[ObjectKey][]byte
}

func NewCache() *Cache {
	return &Cache{
		cache: make(map[ObjectKey][]byte),
	}
}

func (c *Cache) set(k ObjectKey, d []byte) {
	c.cache[k] = d
}

func (c *Cache) get(k ObjectKey) []byte {
	return c.cache[k]
}

func (c *Cache) invalidate(k ObjectKey) {
	delete(c.cache, k)
}
