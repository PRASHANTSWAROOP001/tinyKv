package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

var (
	ErrorNotFound = errors.New("key not found")
	ErrorNotSaveData= errors.New("data could not be saved in file")
)

const MaxDataFileSize = 5*1024*1024

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

    // Open WAL for read/write so we can truncate later
    walPath := filepath.Join(path, "wal.log")
    wal, err := OpenWAL(walPath)
    if err != nil {
        dataFile.Close()
        return nil, err
    }

    db := &TinyDB{
        dataFile: dataFile,
        wal:      wal,
        index:    make(map[string]int64),
    }

    if err := db.loadDataFile(); err != nil {
        db.Close()
        return nil, err
    }

    // check size -> compact if needed
    if info, err := dataFile.Stat(); err == nil && info.Size() > MaxDataFileSize {
        fmt.Println("🧹 Compacting store.data on startup...")
        if err := db.Compact(); err != nil {
            db.Close()
            return nil, err
        }
    }

    if err := db.replayWAL(); err != nil {
        db.Close()
        return nil, err
    }

    return db, nil
}



func (db *TinyDB) Close() error {
    if db.dataFile != nil {
        db.dataFile.Sync()
        db.dataFile.Close()
    }
    if db.wal != nil {
        db.wal.Close() // ensure WAL has a Close() that syncs+closes
    }
    return nil
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
    db.mu.RLock()
    offset, ok := db.index[key]
    db.mu.RUnlock()
    if !ok {
        return "", false
    }

    db.mu.RLock()
    defer db.mu.RUnlock()

    if _, err := db.dataFile.Seek(offset, io.SeekStart); err != nil {
        return "", false
    }

    // read deleted flag + keyLen (1 + 4)
    header := make([]byte, 1+4)
    if _, err := io.ReadFull(db.dataFile, header); err != nil {
        return "", false
    }
    buf := bytes.NewReader(header)

    var deletedByte byte
    if err := binary.Read(buf, binary.LittleEndian, &deletedByte); err != nil {
        return "", false
    }

    var keyLen uint32
    if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
        return "", false
    }

    // read key
    keyBytes := make([]byte, keyLen)
    if _, err := io.ReadFull(db.dataFile, keyBytes); err != nil {
        return "", false
    }

    // read valLen
    valLenBuf := make([]byte, 4)
    if _, err := io.ReadFull(db.dataFile, valLenBuf); err != nil {
        return "", false
    }
    valLen := binary.LittleEndian.Uint32(valLenBuf)

    // read value
    valBytes := make([]byte, valLen)
    if _, err := io.ReadFull(db.dataFile, valBytes); err != nil {
        return "", false
    }

    if deletedByte == 1 {
        return "", false
    }
    return string(valBytes), true
}


func (db *TinyDB) Delete(key string) error {
    db.mu.Lock()
    defer db.mu.Unlock()

    if _, ok := db.index[key]; !ok {
        return ErrorNotFound
    }

    rec := &Record{
        Key:     key,
        Value:   "",
        Deleted: true,
    }

    if err := db.wal.Append(rec); err != nil {
        return ErrorNotSaveData
    }

    data, _ := rec.Encode()
    if _, err := db.dataFile.Seek(0, io.SeekEnd); err != nil {
        return ErrorNotSaveData
    }
    if _, err := db.dataFile.Write(data); err != nil {
        return ErrorNotSaveData
    }
    // remove from in-memory index (important)
    delete(db.index, key)
    return nil
}



func (db *TinyDB) replayWAL() error {
    // Prefer using wal.file if WAL exposes it; otherwise open RW
    walPath := "data/wal.log"
    f, err := os.OpenFile(walPath, os.O_RDWR, 0o644)
    if err != nil {
        // if no WAL, nothing to do
        if os.IsNotExist(err) {
            return nil
        }
        return err
    }
    defer f.Close()

    offset, _ := db.dataFile.Seek(0, io.SeekEnd)

    for {
        rec, bytesRead, err := ReadRecord(f)
        if err != nil {
            if err == io.EOF {
                break
            }
            return err
        }

        data, _ := rec.Encode()
        if _, err := db.dataFile.Write(data); err != nil {
            return err
        }

        if rec.Deleted {
            delete(db.index, rec.Key)
        } else {
            db.index[rec.Key] = offset
        }

        offset += int64(bytesRead)
    }

    // Truncate WAL after successful replay
    if err := f.Truncate(0); err != nil {
        return err
    }
    if _, err := f.Seek(0, 0); err != nil {
        return err
    }
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

func (db *TinyDB) Compact() error {
    db.mu.Lock()
    defer db.mu.Unlock()

    dataDir := "data"
    if err := os.MkdirAll(dataDir, 0o755); err != nil {
        return err
    }

    tempPath := filepath.Join(dataDir, "store.data.compact")
    tempFile, err := os.Create(tempPath)
    if err != nil {
        return fmt.Errorf("create compact file error: %w", err)
    }
    defer tempFile.Close()

    // iterate over current index, write latest non-deleted records
    for _, offset := range db.index {
        rec, err := db.readRecordAtOffset(offset)
        if err != nil {
            return fmt.Errorf("read record offset error: %w", err)
        }
        if rec.Deleted {
            continue
        }
        data, err := rec.Encode()
        if err != nil {
            return fmt.Errorf("encode record error: %w", err)
        }
        if _, err := tempFile.Write(data); err != nil {
            return fmt.Errorf("write compacted data: %w", err)
        }
    }

    // close current data file, rename compact <- old
    oldPath := filepath.Join(dataDir, "store.data")
    if err := db.dataFile.Close(); err != nil {
        return err
    }

    if err := os.Rename(tempPath, oldPath); err != nil {
        return fmt.Errorf("rename compacted file: %w", err)
    }

    db.dataFile, err = os.OpenFile(oldPath, os.O_APPEND|os.O_RDWR, 0o644)
    if err != nil {
        return fmt.Errorf("reopen data file: %w", err)
    }

    // reload index from fresh file
    return db.loadDataFile()
}


func (db *TinyDB) readRecordAtOffset(offset int64) (*Record, error) {
    if _, err := db.dataFile.Seek(offset, io.SeekStart); err != nil {
        return nil, err
    }
    rec, _, err := ReadRecord(db.dataFile)
    if err != nil {
        return nil, err
    }
    return rec, nil
}