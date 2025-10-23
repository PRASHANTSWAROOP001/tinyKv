package main

import (
	"fmt"
	"github.com/PRASHANTSWAROOP101/tinyKv/db"
	"log"
	"math/rand"
	"sync"
	"time"
)

func main() {
	database, err := db.Open("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()

	var wg sync.WaitGroup

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	values := []string{"one", "two", "three", "four", "five"}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				k := keys[rand.Intn(len(keys))] + fmt.Sprintf("-%d", j)
				v := values[rand.Intn(len(values))] + fmt.Sprintf("-%d", id)
				if err := database.Set(k, v); err != nil {
					log.Printf("[writer-%d] Set error: %v\n", id, err)
				}
				time.Sleep(time.Millisecond * 20)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				k := keys[rand.Intn(len(keys))] + fmt.Sprintf("-%d", rand.Intn(50))
				if val, ok := database.Get(k); ok {
					log.Printf("[reader-%d] got key=%s val=%s\n", id, k, val)
				}
				time.Sleep(time.Millisecond * 15)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("✅ Concurrent test completed successfully")
}
