// Copyright 2016 RapidLoop. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package skv provides a simple persistent key-value store for Go values. It
// can store a mapping of string to any gob-encodable Go value. It is
// lightweight and performant, and ideal for use in low-traffic websites,
// utilities and the like.
//
// The API is very simple - you can Put(), Get() or Delete() entries. These
// methods are goroutine-safe.
//
// skv uses BoltDB for storage and the encoding/gob package for encoding and
// decoding values. There are no other dependencies.
package skv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"time"

	"git.mills.io/prologic/bitcask"
)

const (
	CacheTagPattern = "db_tag_%s"
)

// KVStore represents the key value store. Use the Open() method to create
// one, and Close() it when done.
type KVStore[T any] struct {
	db *bitcask.Bitcask
	mu sync.RWMutex
}

var (
	// ErrNotFound is returned when the key supplied to a Get or Delete
	// method does not exist in the database.
	ErrNotFound = errors.New("skv: key not found")

	// ErrBadValue is returned when the value supplied to the Put method
	// is nil.
	ErrBadValue = errors.New("skv: bad value")
)

// Open a key-value store. "path" is the full path to the database file, any
// leading directories must have been created already. File is created with
// mode 0640 if needed.
//
// Because of BoltDB restrictions, only one process may open the file at a
// time. Attempts to open the file from another process will fail with a
// timeout error.
func Open[T any](path string) (*KVStore[T], error) {
	db, err := bitcask.Open(path)
	if err != nil {
		return nil, err
	}
	kv := KVStore[T]{
		db: db,
		mu: sync.RWMutex{},
	}
	return &kv, nil
}

// Put an entry into the store. The passed value is gob-encoded and stored.
// The key can be an empty string, but the value cannot be nil - if it is,
// Put() returns ErrBadValue.
//
//	err := store.Put("key42", 156)
//	err := store.Put("key42", "this is a string")
//	m := map[string]int{
//	    "harry": 100,
//	    "emma":  101,
//	}
//	err := store.Put("key43", m)
func (kvs *KVStore[T]) Put(key string, value T) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return kvs.db.Put([]byte(key), buf.Bytes())
}

// Put an entry into the store with a TTL to expire the entry
func (kvs *KVStore[T]) PutWithTTL(key string, value T, ttl time.Duration) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return kvs.db.PutWithTTL([]byte(key), buf.Bytes(), ttl)
}

func (kvs *KVStore[T]) PutWithTags(key string, value T, tags []string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	err := kvs.db.Put([]byte(key), buf.Bytes())
	if err != nil {
		return err
	}
	return kvs.saveTags(key, tags)
}

// Put an entry into the store with a TTL to expire the entry
func (kvs *KVStore[T]) PutWithTagsAndTTL(key string, value T, ttl time.Duration, tags []string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	err := kvs.db.PutWithTTL([]byte(key), buf.Bytes(), ttl)
	if err != nil {
		return err
	}

	return kvs.saveTags(key, tags)
}

func (kvs *KVStore[T]) saveTags(key string, tags []string) error {
	// get the tags
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	for _, tag := range tags {

		var cacheKeys map[string]struct{}

		tagKey := []byte(fmt.Sprintf(CacheTagPattern, tag))
		if result, err := kvs.db.Get(tagKey); err == nil {
			e := new(map[string]struct{})
			d := gob.NewDecoder(bytes.NewReader(result))
			d.Decode(e)
			cacheKeys = *e
		}

		if cacheKeys == nil {
			cacheKeys = make(map[string]struct{})
		}

		if _, exists := cacheKeys[key]; exists {
			continue
		}

		cacheKeys[key] = struct{}{}

		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(cacheKeys); err != nil {
			return err
		}

		kvs.db.Put(tagKey, buf.Bytes())

	}
	return nil
}

// Get an entry from the store. "value" must be a pointer-typed. If the key
// is not present in the store, Get returns ErrNotFound.
//
//	type MyStruct struct {
//	    Numbers []int
//	}
//	var val MyStruct
//	if err := store.Get("key42", &val); err == skv.ErrNotFound {
//	    // "key42" not found
//	} else if err != nil {
//	    // an error occurred
//	} else {
//	    // ok
//	}
//
// The value passed to Get() can be nil, in which case any value read from
// the store is silently discarded.
//
//	if err := store.Get("key42", nil); err == nil {
//	    fmt.Println("entry is present")
//	}
func (kvs *KVStore[T]) Get(key string) (T, error) {
	output := new(T)

	v, e := kvs.db.Get([]byte(key))

	if e == bitcask.ErrKeyNotFound {
		return *output, ErrNotFound
	}

	if e != nil {
		return *output, e
	}

	d := gob.NewDecoder(bytes.NewReader(v))
	d.Decode(output)
	return *output, e
}

func (kvs *KVStore[T]) GetWithPrefix(p string) ([]T, error) {
	output := make([]T, 0)
	prefix := []byte(p)

	err := kvs.db.Scan(prefix, func(key []byte) error {
		entry, err := kvs.Get(string(key))
		if err != nil {
			return err
		}
		output = append(output, entry)
		return err
	})
	return output, err
}

func (kvs *KVStore[T]) GetWithTag(tag string) ([]T, error) {
	output := make([]T, 0)
	tagKey := []byte(fmt.Sprintf(CacheTagPattern, tag))
	updateTags := false

	// lock the tags mutex
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	var cacheKeys map[string]struct{}
	if result, err := kvs.db.Get(tagKey); err == nil {
		e := new(map[string]struct{})
		d := gob.NewDecoder(bytes.NewReader(result))
		d.Decode(e)
		cacheKeys = *e
	}

	if cacheKeys == nil {
		return output, nil
	}

	for key := range cacheKeys {
		item, err := kvs.Get(key)

		// key might have expired or deleted
		if err == ErrNotFound {
			delete(cacheKeys, key)
			updateTags = true
			continue
		}

		if err != nil {
			return make([]T, 0), nil
		}
		output = append(output, item)
	}

	if updateTags {
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(cacheKeys); err != nil {
			return make([]T, 0), err
		}

		kvs.db.Put(tagKey, buf.Bytes())
	}

	return output, nil
}

// Delete the entry with the given key. If no such key is present in the store,
// it returns ErrNotFound.
//
//	store.Delete("key42")
func (kvs *KVStore[T]) Delete(key string) error {
	return kvs.db.Delete([]byte(key))
}

// Iterate over all existing keys and return a slice of the keys.
// If no keys are found, return an empty slice.
//
//	store.GetKeys()
func (kvs *KVStore[T]) GetKeys() ([]string, error) {
	var kl []string

	err := kvs.db.Sift(func(key []byte) (bool, error) {
		kl = append(kl, string(key))
		return false, nil
	})

	return kl, err
}

// Returns all items.
// If no items are found, return an empty slice.
//
//	store.GetAll()
func (kvs *KVStore[T]) GetAll() ([]T, error) {
	var kl []T

	err := kvs.db.Sift(func(key []byte) (bool, error) {
		entry, err := kvs.Get(string(key))
		if err != nil {
			return false, err
		}
		kl = append(kl, entry)
		return false, nil
	})

	return kl, err
}

// Close closes the key-value store file.
func (kvs *KVStore[T]) Close() error {
	return kvs.db.Close()
}
