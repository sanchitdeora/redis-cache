package store

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidEntryID = errors.New("entry ID is invalid")

func (s *StreamDataStoreImpl) Set(streamKey string, entryID string, entry []StreamEntry) error {

	val, exists := s.DataStore[streamKey]; if exists {
		prevEntry := val[len(val)-1]
		err := validateEntryID(prevEntry.ID, entryID)
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

func (s *StreamDataStoreImpl) SetEntry(streamKey string, entryID string, entry []StreamEntry) (string, error) {

	var prevEntryID string
	val, exists := s.DataStore[streamKey]; if exists {
		prevEntryID = val[len(val)-1].ID
	}

	updatedEntryID, err := getUpdatedEntryID(prevEntryID, entryID)
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

func (s *StreamDataStoreImpl) GetEntry(streamKey string, entryID string) []StreamEntry {
	val, exists := s.DataStore[streamKey]; if !exists {
		return nil
	}

	if len(val) > 0 {
		for _, entry := range val {
			if entry.ID == entryID {
				return entry.Entry
			}
		}
	} else {
		return nil
	}

	return nil
}

func (s *StreamDataStoreImpl) GetEntryRange(streamKey string, startEntryID string, endEntryID string) ([]StreamValues) {
	if startEntryID == "-" {
		startEntryID = "0-1"
	}

	if endEntryID == "+" {
		endEntryID = fmt.Sprintf("%v-%v", time.Now().UnixMilli(), time.Now().UnixMilli())
	}

	err := validateEntryID(startEntryID, endEntryID)
	if err != nil {
		return nil
	}

	streamValues, exists := s.DataStore[streamKey]; if !exists {
		return nil
	}

	resp := make([]StreamValues, 0)

	startTs, startSeq := parseEntryID(startEntryID)
	endTs, endSeq := parseEntryID(endEntryID)

	for _, val := range streamValues {
		currTs, currSeq := parseEntryID(val.ID)

		if (startTs <= currTs && currTs <= endTs) {
			if startTs == endTs && startSeq <= currSeq && currSeq <= endSeq {
				resp = append(resp, val)
			} else if startTs != endTs && startTs == currTs && startSeq <= currSeq {
				resp = append(resp, val)
			} else if startTs != endTs && currTs == endTs && currSeq <= endSeq {
				resp = append(resp, val)
			}
		}
	}
	
	return resp
}

// func (s *StreamDataStoreImpl) GetKeys() []string {
// 	keys := make([]string, 0, len(s.DataStore))

// 	for k := range s.DataStore {
// 		keys = append(keys, k)
// 	}

// 	return keys
// }

func getUpdatedEntryID(prevEntryID string, entryID string) (string, error) {
	err := validateEntryID(prevEntryID, entryID)
	if err != nil {
		return "", err
	}

	currTs, currSeq := parseEntryID(entryID)
	if currTs == math.MaxInt {
		return fmt.Sprintf("%v-0", time.Now().UnixMilli()), nil
	}
	
	if currTs == 0 && currSeq == math.MaxInt {
		return fmt.Sprintf("%v-1", currTs), nil
	}

	if prevEntryID == "" {
		if currSeq == math.MaxInt{
			return fmt.Sprintf("%v-0", currTs), nil
		}
	} else {
		prevTs, prevSeq := parseEntryID(prevEntryID)
		
		if currTs == prevTs && currSeq == math.MaxInt {
			return fmt.Sprintf("%v-%v", currTs, prevSeq + 1), nil
		}

		if currTs > prevTs && currSeq == math.MaxInt {
			return fmt.Sprintf("%v-0", currTs), nil
		}

	}

	return entryID, nil
}

func parseEntryID(entryID string) (int, int) {
	if entryID == "*" {
		return math.MaxInt, math.MaxInt
	}

	entryIdComposition := strings.Split(entryID, "-")
	if len(entryIdComposition) == 1 {
		if timestamp, _ := strconv.Atoi(entryIdComposition[0]); timestamp == 0 {
			return 0, 1
		} else {
			return timestamp, 0
		}
	}

	timestamp, _ := strconv.Atoi(entryIdComposition[0])
	if entryIdComposition[1] == "*" {
		return timestamp, math.MaxInt
	}
	sequenceNum, _ := strconv.Atoi(entryIdComposition[1])

	return timestamp, sequenceNum
}

func validateEntryID(prevEntryID, entryID string) error {
	if prevEntryID == "" {
		return nil
	}

	if prevEntryID == entryID {
		return ErrInvalidEntryID
	}

	prevTs, prevSeq := parseEntryID(prevEntryID)
	currTs, currSeq := parseEntryID(entryID)

	if prevTs > currTs || (prevTs == currTs && prevSeq > currSeq ) {
		return ErrInvalidEntryID
	}

	return nil
}