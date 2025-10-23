package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

var (
	ErrorNotFound = errors.New("key not found")
	ErrorNotSaveData= errors.New("data could not be saved in file")
)

type TinyDB struct {
	dataFile *os.File
	wal      *WAL
	index    map[string]int64
	mu       sync.RWMutex
}

func Open(path string) (*TinyDB, error) {
    // 1️⃣ Open main data file
    dataFile, err := os.OpenFile("data/store.data", os.O_CREATE|os.O_RDWR, 0644)
    if err != nil {
        return nil, err
    }

    // 2️⃣ Open WAL
    wal, err := OpenWAL("data/wal.log")
    if err != nil {
        return nil, err
    }

    db := &TinyDB{
        dataFile: dataFile,
        wal:      wal,
        index:    make(map[string]int64),
    }

    // 3️⃣ Load index from store.data
    if err := db.loadDataFile(); err != nil {
        return nil, err
    }

    // 4️⃣ Replay WAL into data file + update index
    if err := db.replayWAL(); err != nil {
        return nil, err
    }

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

	if err := db.wal.Append(rec); err != nil{
		return err
	}

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

func (db *TinyDB) Delete(key string) error{
	db.mu.Lock()

	defer db.mu.Unlock()

	if _,ok := db.index[key]; !ok {
		return ErrorNotFound
	}

	rec := &Record{
		Key: key,
		Value: "",
		Deleted: true,
	}

	if err := db.wal.Append(rec); err != nil{
		return ErrorNotSaveData
	}

	data,_ := rec.Encode()
	offset,_ := db.dataFile.Seek(0, io.SeekEnd)
	_,err := db.dataFile.Write(data)
	if err != nil{
		return ErrorNotSaveData
	}
	db.index[key] = offset
	return nil
}


func (db *TinyDB) replayWAL() error{
	file, err := os.Open("data/wal.log")
	if err != nil{
		return err
	}
	defer file.Close()

	offset,_ := db.dataFile.Seek(0, io.SeekEnd)

	for {
		rec, bytesRead, err := ReadRecord(file)

		if err != nil{
			break
		}

		data,_ := rec.Encode()
		db.dataFile.Write(data)

		if rec.Deleted{
			delete(db.index, rec.Key)
		}else{
			db.index[rec.Key] = offset
		}

		offset += int64(bytesRead)
	}
	file.Truncate(0)
	file.Seek(0,0)
	return nil
}


func (db *TinyDB) loadDataFile() error {
    file, err := os.Open("data/store.data")
    if err != nil {
        // If file doesn't exist on first startup, that's fine
        if os.IsNotExist(err) {
            return nil
        }
        return err
    }
    defer file.Close()

    offset := int64(0)

    for {
        rec, bytesRead, err := ReadRecord(file)
        if err != nil {
            // io.EOF just means end of file, not a failure
            if err == io.EOF {
                break
            }
            return err
        }

        // Update the in-memory index
        if rec.Deleted {
            delete(db.index, rec.Key)
        } else {
            db.index[rec.Key] = offset
        }

        // Move offset ahead by however many bytes were read
        offset += int64(bytesRead)
    }

    return nil
}
