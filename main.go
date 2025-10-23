package main

import (
	"fmt"
	"github.com/PRASHANTSWAROOP101/tinyKv/db"
)

func main() {
	database, err := db.Open("./data")
	if err != nil {
		panic(err)
	}
	defer database.Close()

	database.Set("name", "Prashant")
	value, ok1 := database.Get("password")
	if !ok1{
		fmt.Println("missing data")
	}else{
		fmt.Println(value)
	}
	val, ok := database.Get("name")
	if ok {
		fmt.Println("name:", val)
	} else {
		fmt.Println("not found")
	}
}
