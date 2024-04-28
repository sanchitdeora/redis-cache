package main

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

type ValueStore struct {
	Value      string
	Expiration int64
}

type KVDataStore map[string]*ValueStore

type RDBConfig struct {
	Dir string
	DbFileName string
}

type StoreOpts struct {
	Config RDBConfig
}

type Store struct {
	StoreOpts
	kvDataStore KVDataStore
}

func NewStore(opts StoreOpts) Store {
	return Store{StoreOpts: opts, kvDataStore: make(KVDataStore)}
}

type OpCode byte

const (
	RdbEmptyFileBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	// OpCode
	OpAUX 			OpCode = 0xFA
	OpResizeDB 		OpCode = 0xFB
	OpExpireTimeMs 	OpCode = 0xFC
	OpExpireTime 	OpCode = 0xFD
	OpSelectDB 		OpCode = 0xFE
	OpEOF 			OpCode = 0xFF

)

func (s *Store) Set(key string, val string, expDur int64) error {
	var expiration int64 = -1 

	if expDur > 0 {
		expiration = time.Now().UnixMilli() + expDur
	}

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
		// if value is expired, delete from store
		delete(s.kvDataStore, key)
		return "", nil
	}

	return val.Value, nil
}

func (s *Store) GetKeys() []string {
	keys := make([]string, 0, len(s.kvDataStore))

	for k := range s.kvDataStore {
		keys = append(keys, k)
	}

	return keys
}

func (s *Store) ToRDBStore() ([]byte, error) {
	return base64.StdEncoding.DecodeString(RdbEmptyFileBase64)
}

func (s *Store) InitializeDB() KVDataStore {
	fmt.Println("Initializing DB", s.Config)
	
	file, err := os.Open(fmt.Sprintf("%s/%s", s.Config.Dir, s.Config.DbFileName))
	if err != nil {
		return make(KVDataStore)
	}
	reader := bufio.NewReader(file)

	// skip to 0xFE opcode
	reader.ReadBytes(0xFE)
	reader.UnreadByte()

	return s.ParseRdbFile(reader)
}

func (s *Store) ParseRdbFile(reader *bufio.Reader) KVDataStore {
	main:
		for {
			opCode, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					fmt.Println("EOF???")
					break
				}
				panic(err)
			}

			// getStringVal(reader)

			switch OpCode(opCode) {
				case 0xFF: // EOF
					break main
				case 0xFE:
					BufJump(reader, 2)
					s.LengthParser(reader)
					s.LengthParser(reader)
				case 0xFD:
				case 0xFC:
				default:
					expiry := int64(-1)
					valueType := opCode
					if opCode == 0xFD {
						expiry = int64(binary.LittleEndian.Uint32(s.ReadBytes(reader, 4)))
						valueType, _ = reader.ReadByte()
					} else if opCode == 0xFC {
						expiry = int64(binary.LittleEndian.Uint32(s.ReadBytes(reader, 8)))
						valueType, _ = reader.ReadByte()
					}
					if valueType != 0x00 {
						fmt.Printf("RedisRDB.Load: opcode not implemented: 0x%x\n", valueType)
						continue
					}
					keyLength := s.LengthParser(reader)
					key := make([]byte, keyLength)
					reader.Read(key)
					valueLength := s.LengthParser(reader)
					value := make([]byte, valueLength)
					reader.Read(value)
					fmt.Printf("RedisRDB.Load: Key: %s, Value: %s, expiry: %d\n", string(key), string(value), expiry)
					s.kvDataStore[string(key)] = &ValueStore{
						Value:  string(value),
						Expiration: expiry,
					}
			}
		}
	return s.kvDataStore
}

func BufJump(buf *bufio.Reader, x int) error {
	a := make([]byte, x)
	_, err := buf.Read(a)
	return err
}

func (s *Store) ReadBytes(reader *bufio.Reader, x int) []byte {
	buf := make([]byte, x)
	reader.Read(buf)
	return buf
}

func (s *Store) LengthParser(reader *bufio.Reader) int {
	lenByte, _ := reader.ReadByte()
	switch lenByte >> 6 {
	case 0b00:
		return int(binary.LittleEndian.Uint16([]byte{lenByte, 00}))
	case 0b01:
		additionalByte, _ := reader.ReadByte()
		return int(binary.LittleEndian.Uint16([]byte{lenByte & 0b00111111, additionalByte}))
	case 0b10:
		fmt.Println("hello???")
	case 0b11:
		switch lenByte & 0b00111111 {
		case 0:
			buf := make([]byte, 1)
			reader.Read(buf)
			fmt.Printf("wow??? 0x%02x", buf)
			return int(binary.LittleEndian.Uint16([]byte{buf[0], 00}))
		case 1:
			buf := make([]byte, 2)
			reader.Read(buf)
			return int(binary.LittleEndian.Uint16(buf))
		case 2:
			buf := make([]byte, 4)
			reader.Read(buf)
			return int(binary.LittleEndian.Uint32(buf))
		}
	}
	
	return -1
}

func getStringVal(r *bufio.Reader) {
	fmt.Println(r.Peek(r.Size()))
}

func (s *Store) EmptyRedisFile() []byte {
	return []byte("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
}