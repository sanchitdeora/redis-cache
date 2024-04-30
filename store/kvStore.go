package store

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
)

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

func (kv *KVStoreImpl) Set(key string, val string, expDur int64) error {
	var expiration int64 = -1 

	if expDur > 0 {
		expiration = time.Now().UnixMilli() + expDur
	}

	value := &Values{
		Value:      val,
		Expiration: expiration,
	}

	kv.DataStore[key] = value

	return nil
}

func (kv *KVStoreImpl) Get(key string) (string, error) {
	val, exists := kv.DataStore[key]; if !exists {
		return "", nil
	}

	if val.Expiration > 0 && time.Now().UnixMilli() > val.Expiration {
		// if value is expired, delete from store
		delete(kv.DataStore, key)
		return "", nil
	}

	return val.Value, nil
}

func (kv *KVStoreImpl) GetKeys() []string {
	keys := make([]string, 0, len(kv.DataStore))

	for k := range kv.DataStore {
		keys = append(keys, k)
	}

	return keys
}

func (kv *KVStoreImpl) ToRDBStore() ([]byte, error) {
	return base64.StdEncoding.DecodeString(RdbEmptyFileBase64)
}

func (kv *KVStoreImpl) InitializeDB() {
	fmt.Println("Initializing DB", kv.Config)

	file, err := os.Open(fmt.Sprintf("%s/%s", kv.Config.Dir, kv.Config.DbFileName))
	if err != nil {
		kv.DataStore = make(KVDataStore)
	}
	reader := bufio.NewReader(file)

	// skip to 0xFE opcode
	reader.ReadBytes(0xFE)
	reader.UnreadByte()

	kv.DataStore = kv.ParseRdbFile(reader)
}

func (kv *KVStoreImpl) ParseRdbFile(reader *bufio.Reader) KVDataStore {
	main:
		for {
			opCode, err := reader.ReadByte()
			if err != nil {
				if err == io.EOF {
					fmt.Println("EOF???")
					break
				}
				return make(KVDataStore)
			}

			// getStringVal(reader)

			switch OpCode(opCode) {
				case 0xFF: // EOF
					break main
				case 0xFE:
					bufJump(reader, 2)
					lengthParser(reader)
					lengthParser(reader)
					continue
			}

			expiry := int64(-1)
			valueType := opCode
			if opCode == 0xFD {
				expiry = int64(binary.LittleEndian.Uint32(readBytes(reader, 4))) * 1000
				valueType, _ = reader.ReadByte()
			} else if opCode == 0xFC {
				expiry = int64(binary.LittleEndian.Uint32(readBytes(reader, 8))) * 1000
				valueType, _ = reader.ReadByte()
			}

			if valueType != 0x00 {
				fmt.Printf("RedisRDB.Load: opcode not implemented: 0x%x\n", valueType)
				continue
			}

			keyLength := lengthParser(reader)
			key := make([]byte, keyLength)
			reader.Read(key)
			
			valueLength := lengthParser(reader)
			value := make([]byte, valueLength)
			reader.Read(value)
			
			fmt.Printf("RedisRDB.Load: Key: %s, Value: %s, expiry: %d\n", string(key), string(value), expiry)
			kv.DataStore[string(key)] = &Values{
				Value:  string(value),
				Expiration: expiry,
			}
		}		

	return kv.DataStore
}

func bufJump(buf *bufio.Reader, x int) error {
	a := make([]byte, x)
	_, err := buf.Read(a)
	return err
}

func readBytes(reader *bufio.Reader, x int) []byte {
	buf := make([]byte, x)
	reader.Read(buf)
	return buf
}

func lengthParser(reader *bufio.Reader) int {
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

func (kv *KVStoreImpl) EmptyRedisFile() []byte {
	return []byte("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
}