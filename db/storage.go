package db

import ("io" 
"encoding/binary"
"bytes"
)

func ReadRecord(r io.Reader) (*Record, int, error) {
    header := make([]byte, 1+4)
    n, err := io.ReadFull(r, header)
    if err == io.EOF || err == io.ErrUnexpectedEOF {
        return nil, n, io.EOF // just stop gracefully
    } else if err != nil {
        return nil, n, err
    }

    buf := bytes.NewReader(header)
    var deleted byte
    var keyLen uint32
    binary.Read(buf, binary.LittleEndian, &deleted)
    binary.Read(buf, binary.LittleEndian, &keyLen)

    key := make([]byte, keyLen)
    n2, err := io.ReadFull(r, key)
    if err == io.EOF || err == io.ErrUnexpectedEOF {
        return nil, n + n2, io.EOF
    } else if err != nil {
        return nil, n + n2, err
    }

    valLenBytes := make([]byte, 4)
    n3, err := io.ReadFull(r, valLenBytes)
    if err == io.EOF || err == io.ErrUnexpectedEOF {
        return nil, n + n2 + n3, io.EOF
    } else if err != nil {
        return nil, n + n2 + n3, err
    }

    valLen := binary.LittleEndian.Uint32(valLenBytes)

    val := make([]byte, valLen)
    n4, err := io.ReadFull(r, val)
    if err == io.EOF || err == io.ErrUnexpectedEOF {
        return nil, n + n2 + n3 + n4, io.EOF
    } else if err != nil {
        return nil, n + n2 + n3 + n4, err
    }

    return &Record{
        Key:     string(key),
        Value:   string(val),
        Deleted: deleted == 1,
    }, n + n2 + n3 + n4, nil
}
