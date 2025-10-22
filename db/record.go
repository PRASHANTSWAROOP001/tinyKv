package db

import (
	"bytes"
	"encoding/binary"
)

type Record struct {
	Key     string
	Value   string
	Deleted bool
}

func (r *Record) Encode() ([]byte, error) {
	var buf bytes.Buffer

	if r.Deleted {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	keyByte := []byte(r.Key)
	binary.Write(&buf, binary.LittleEndian, uint32(len(keyByte)))
	buf.Write(keyByte)

	valByte := []byte(r.Value)
	binary.Write(&buf, binary.LittleEndian, uint32(len(valByte)))
	buf.Write(valByte)

	return buf.Bytes(), nil

}

func Decode(data []byte) (*Record, error) {
	buf := bytes.NewReader(data)

	var deletedByte byte
	binary.Read(buf, binary.LittleEndian, &deletedByte)

	var keyLen uint32
	binary.Read(buf, binary.LittleEndian, &keyLen)
	key := make([]byte, keyLen)
	buf.Read(key)

	var valLen uint32
	binary.Read(buf, binary.LittleEndian, &keyLen)
	val := make([]byte, valLen)
	buf.Read(val)

	return &Record{
		Key:     string(key),
		Value:   string(val),
		Deleted: deletedByte == 1,
	}, nil
}
