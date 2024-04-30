package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var ErrInvalidEntryID = errors.New("entry ID is invalid")

func (s *StreamDataStoreImpl) Set(streamKey string, entryID string, entry StreamEntry) error {
	val, exists := s.DataStore[streamKey]; if exists {
		prevEntry := val[len(val)-1]
		err := ValidateEntryID(prevEntry.ID, entryID)
		if err != nil {
			return err
		}
	}

	streamVal := StreamValues{
		ID: entryID,
		Entry: entry,
	}

	s.DataStore[streamKey] = append(s.DataStore[streamKey], streamVal)
	return nil
}

func (s *StreamDataStoreImpl) SetEntry(streamKey string, entryID string, entry StreamEntry) (string, error) {
	
	var prevEntryID string
	val, exists := s.DataStore[streamKey]; if exists {
		prevEntryID = val[len(val)-1].ID
	}

	updatedEntryID, err := GetEntryID(prevEntryID, entryID)
	if err != nil {
		return "", err
	}

	streamVal := StreamValues{
		ID: updatedEntryID,
		Entry: entry,
	}

	s.DataStore[streamKey] = append(s.DataStore[streamKey], streamVal)
	return updatedEntryID, nil
}

// func (s *StreamDataStoreImpl) Get(key string) (interface{}, error) {
// 	val, exists := s.DataStore[key]; if !exists {
// 		return "", nil
// 	}

// 	if val.Expiration > 0 && time.Now().UnixMilli() > val.Expiration {
// 		// if value is expired, delete from store
// 		delete(s.DataStore, key)
// 		return "", nil
// 	}

// 	return val.Value, nil
// }

func (s *StreamDataStoreImpl) GetStream(streamKey string) ([]StreamValues, error) {
	val, exists := s.DataStore[streamKey]; if !exists {
		return nil, nil
	}

	return val, nil
}

func (s *StreamDataStoreImpl) GetEntry(streamKey string, entryID string) (StreamEntry, error) {
	val, exists := s.DataStore[streamKey]; if !exists {
		return StreamEntry{}, nil
	}

	if len(val) > 0 {
		for _, entry := range val {
			if entry.ID == entryID {
				return entry.Entry, nil
			}
		}
	} else {
		return StreamEntry{}, nil
	}

	return StreamEntry{}, nil
}

// func (s *StreamDataStoreImpl) GetKeys() []string {
// 	keys := make([]string, 0, len(s.DataStore))

// 	for k := range s.DataStore {
// 		keys = append(keys, k)
// 	}

// 	return keys
// }

func GetEntryID(prevEntryID, entryID string) (string, error) {
	err := ValidateEntryID(prevEntryID, entryID)
	if err != nil {
		return "", err
	}

	currTs, currSeq := ParseEntryID(entryID)
	if currTs == 0 && currSeq == math.MaxInt {
		return fmt.Sprintf("%v-%v", currTs, 1), nil
	}

	if prevEntryID == "" {
		if currSeq == math.MaxInt{
			return fmt.Sprintf("%v-%v", currTs, 0), nil
		}
	} else {
		prevTs, prevSeq := ParseEntryID(prevEntryID)
		
		if currTs == prevTs && currSeq == math.MaxInt {
			return fmt.Sprintf("%v-%v", currTs, prevSeq + 1), nil
		}

		if currTs > prevTs && currSeq == math.MaxInt {
			return fmt.Sprintf("%v-%v", currTs, 0), nil
		}

	}

	return entryID, nil
}

func ParseEntryID(entryID string) (int, int) {
	if entryID == "*" {
		return math.MaxInt, math.MaxInt
	}

	entryIdComposition := strings.Split(entryID, "-")
	timestamp, _ := strconv.Atoi(entryIdComposition[0])
	if entryIdComposition[1] == "*" {
		return timestamp, math.MaxInt
	}
	sequenceNum, _ := strconv.Atoi(entryIdComposition[1])

	return timestamp, sequenceNum
}

func ValidateEntryID(prevEntryID, entryID string) error {
	if prevEntryID == "" {
		return nil
	}

	if prevEntryID == entryID {
		return ErrInvalidEntryID
	}

	prevTs, prevSeq := ParseEntryID(prevEntryID)
	currTs, currSeq := ParseEntryID(entryID)

	if prevTs > currTs || (prevTs == currTs && prevSeq > currSeq ) {
		return ErrInvalidEntryID
	}

	return nil
}