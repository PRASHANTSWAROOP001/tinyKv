package db

import ("io" 
"encoding/binary"
)

func ReadRecord(r io.Reader)(*Record, int, error){
	var deletedFlag byte
	if err := binary.Read(r, binary.LittleEndian, &deletedFlag); err != nil{
		return  nil,0, err
	}
	
	var keyLen, valLen uint32

	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil{
		return nil,0,err
	}

	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil{
		return nil,0,err
	}

	keyBytes := make([]byte, keyLen)

	if _,err := io.ReadFull(r,keyBytes); err != nil{
		return nil,0,err
	}


	valBytes := make([]byte, valLen)

	if _,err := io.ReadFull(r,valBytes); err != nil{
		return nil,0,err
	}

	rec := &Record{
		Deleted: deletedFlag ==1,
		Key: string(keyBytes),
		Value: string(valBytes),
	}

	totalBytes := 1 + 4 + 4 + int(keyLen) + int(valLen)
    return rec, totalBytes, nil
}