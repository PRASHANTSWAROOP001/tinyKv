package db

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type TinyDB struct {
	dataFile *os.File
	index    map[string]int64
	mu       sync.RWMutex
}

func Open(path string) (*TinyDB, error) {
	// ensure data dir exists
	os.MkdirAll("data", 0755)

	dataFile, err := os.OpenFile("data/store.data",
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	db := &TinyDB{
		dataFile: dataFile,
		index:    make(map[string]int64),
	}

	// TODO: load existing data into index (later step)
	return db, nil
}

func (db *TinyDB) Close() {
	db.dataFile.Close()
}

func (db *TinyDB) Set(key string, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	rec := &Record{Key: key, Value: value, Deleted: false}
	data, _ := rec.Encode()

	offset, _ := db.dataFile.Seek(0, io.SeekEnd)
	_, err := db.dataFile.Write(data)

	if err != nil {
		return err
	}

	db.index[key] = offset
	return nil
}

func (db *TinyDB) Get(key string) (string, bool) {
	db.mu.RLock()         // applied read lock
	defer db.mu.RUnlock() // defer read unlock

	offset, ok := db.index[key] // get the offset from the
	if !ok {
		// if no offset means data not added
		return "missing data", false
	}
	// seek the last position
	_, err := db.dataFile.Seek(offset, io.SeekStart)

	if err != nil {
		return "", false
	}
	// read/get the header.
	header := make([]byte, 1+4)

	if _, err := db.dataFile.Read(header); err != nil {
		return "", false
	}

	buf := bytes.NewReader(header)

	var deletedByte byte
	// read the deleteByte bool
	binary.Read(buf, binary.LittleEndian, &deletedByte)

	// read the keyLen from the buffer to get length of key.
	var keyLen uint32
	binary.Read(buf, binary.LittleEndian, &keyLen)
	// read actual data this offsets sets the file at the of the key
	// after this offset points to the start of the value
	keyBytes := make([]byte, keyLen)
	db.dataFile.Read(keyBytes)

	// read the actual data like convert the val size to unsigned int from the db itself. no buffers
	valLenBuf := make([]byte, 4)
	db.dataFile.Read(valLenBuf)
	valLen := binary.LittleEndian.Uint32(valLenBuf)

	// read the actual value
	valByte := make([]byte, valLen)
	db.dataFile.Read(valByte)

	// if value is marked deleted then return nothing
	if deletedByte == 1 {
		return "", false
	}
	// return the value.
	return string(valByte), true

}

func (db *TinyDB) Delete(key string){
	
} 
