package bucketsync

import (
	"sync"

	"github.com/pkg/errors"
)

type cache struct {
	hash           map[ObjectKey]*keyValue
	lock           sync.RWMutex
	listHead       *keyValue
	currentEntries int
	maxEntries     int
}

type keyValue struct {
	key   ObjectKey
	value []byte
	prev  *keyValue
	next  *keyValue
}

// NewCache returns LRU Cache
func NewCache(maxEntries int) *cache {
	c := &cache{
		hash:           make(map[ObjectKey]*keyValue),
		currentEntries: 0,
		maxEntries:     maxEntries,
		listHead:       &keyValue{},
		lock:           sync.RWMutex{},
	}

	c.listHead.next = c.listHead
	c.listHead.prev = c.listHead
	return c
}

// Get value from cache if exist
func (c *cache) Get(key ObjectKey) (data []byte, err error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if kv, ok := c.hash[key]; ok {
		if kv != c.listHead.next {
			listRemove(kv)
			listAdd(c.listHead, kv)

		}
		return kv.value, nil

	}
	return nil, errors.New("not found")
}

// Add value to cache
func (c *cache) Add(key ObjectKey, data []byte) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if kv, ok := c.hash[key]; ok {
		if kv != c.listHead.next {
			listRemove(kv)
			listAdd(c.listHead, kv)
		}
		kv.value = data

	} else {
		if c.maxEntries != c.currentEntries {
			c.currentEntries++

		} else {
			lastItem := c.listHead.prev
			delete(c.hash, lastItem.key)
			listRemove(lastItem)
		}

		kv := &keyValue{
			key:   key,
			value: data,
		}
		listAdd(c.listHead, kv)
		c.hash[key] = kv
	}
	return nil
}

// Remove value from cache
func (c *cache) Remove(key ObjectKey) (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	if kv, ok := c.hash[key]; ok {
		delete(c.hash, key)
		listRemove(kv)

	}
	return nil
}

func listRemove(kv *keyValue) {
	kv.prev.next = kv.next
	kv.next.prev = kv.prev
}

func listAdd(prev, kv *keyValue) {
	next := prev.next
	kv.next = next
	kv.prev = prev
	next.prev = kv
	prev.next = kv
}
