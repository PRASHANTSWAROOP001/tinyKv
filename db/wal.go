package db

import (
	"os"
)


type WAL struct{
	file *os.File
}

func OpenWAL(path string)(*WAL,error){
	f,err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR,0644)
	if err != nil{
		return nil,err
	}
	return &WAL{file: f},nil
}

func (wal *WAL) Append(rec *Record) error{
	data,_ := rec.Encode()

	_,err := wal.file.Write(data)

	if err != nil{
		return err
	}

	return nil
}


func (wal *WAL)Close(){
	wal.file.Close()
}