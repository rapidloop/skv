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
	"reflect"
	"time"

	"github.com/boltdb/bolt"
)

// KVStore represents the key value store. Use the Open() method to create
// one, and Close() it when done.
type KVStore struct {
	db *bolt.DB
}

var (
	// ErrNotFound is returned when the key supplied to a Get or Delete
	// method does not exist in the database.
	ErrNotFound = errors.New("skv: key not found")

	// ErrBadValue is returned when the value supplied to the Put method
	// is nil.
	ErrBadValue = errors.New("skv: bad value")

	bucketName = []byte("kv")
)

// Open a key-value store. "path" is the full path to the database file, any
// leading directories must have been created already. File is created with
// mode 0640 if needed.
//
// Because of BoltDB restrictions, only one process may open the file at a
// time. Attempts to open the file from another process will fail with a
// timeout error.
func Open(path string) (*KVStore, error) {
	opts := &bolt.Options{
		Timeout: 50 * time.Millisecond,
	}
	if db, err := bolt.Open(path, 0640, opts); err != nil {
		return nil, err
	} else {
		err := db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(bucketName)
			return err
		})
		if err != nil {
			return nil, err
		} else {
			return &KVStore{db: db}, nil
		}
	}
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
func (kvs *KVStore) Put(key string, value interface{}) error {
	if value == nil {
		return ErrBadValue
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}
	return kvs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(key), buf.Bytes())
	})
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
//  if err := store.Get("key42", nil); err == nil {
//      fmt.Println("entry is present")
//  }
func (kvs *KVStore) Get(key string, value interface{}) error {
	return kvs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		if k, v := c.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else if value == nil {
			return nil
		} else {
			d := gob.NewDecoder(bytes.NewReader(v))
			return d.Decode(value)
		}
	})
}

// Delete the entry with the given key. If no such key is present in the store,
// it returns ErrNotFound.
//
//	store.Delete("key42")
func (kvs *KVStore) Delete(key string) error {
	return kvs.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		if k, _ := c.Seek([]byte(key)); k == nil || string(k) != key {
			return ErrNotFound
		} else {
			return c.Delete()
		}
	})
}

// List all entry values from the store. In case of there are not entries, it will return nil.
func (kvs *KVStore) List(ty reflect.Type) (map[string]interface{}, error) {
	output := make(map[string]interface{})
	err := kvs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketName).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			value := reflect.New(ty)
			vl := value.Interface()
			d := gob.NewDecoder(bytes.NewReader(v))
			d.Decode(vl)
			output[string(k)] = vl
		}
		return nil
	})
	return output, err
}

// Close closes the key-value store file.
func (kvs *KVStore) Close() error {
	return kvs.db.Close()
}
