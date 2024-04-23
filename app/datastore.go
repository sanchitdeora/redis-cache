package main

import (
	"encoding/base64"
	"fmt"
	"time"
)

type ValueStore struct {
	Value      string
	Expiration int64
}

type KVDataStore map[string]*ValueStore

type Store struct {
	kvDataStore KVDataStore
}

func NewStore() Store {
	return Store{kvDataStore: make(KVDataStore)}
}

const (
	EMPTY_RDB_FILE_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
)

func (s *Store) Set(key string, val string, expDur int64) error {
	var expiration int64 = -1 
	
	if expDur > 0 {
		expiration = time.Now().UnixMilli() + expDur
	}
	
	fmt.Printf("Expiration time: %v  Value expiration duration: %v\n", expiration, expDur)

	value := &ValueStore{
		Value:      val,
		Expiration: expiration,
	}

	s.kvDataStore[key] = value

	return nil
}

func (s *Store) Get(key string) (string, error) {
	val, exists := s.kvDataStore[key]; if !exists {
		return "", nil
	}

	if val.Expiration > 0 && time.Now().UnixMilli() > val.Expiration {
		fmt.Printf("Request receive time: %v  Value expiration time: %v\n", time.Now().UnixMilli(), val.Expiration)
		// if value is expired, delete from store
		delete(s.kvDataStore, key)
		return "", nil
	}

	return val.Value, nil
}

func (s *Store) ToRDBStore() ([]byte, error) {
	return base64.StdEncoding.DecodeString(EMPTY_RDB_FILE_BASE64)
}