package main

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/lib/pq"
)

func main() {
	db, err := sql.Open("postgres", "postgres://localhost/artifiscal_development?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	limit := 40
	for offset := 0; offset < 400; offset += limit {
		log.Printf("%v of %v", offset, 400)
		crows, err := db.Query("SELECT id, symbol FROM companies LIMIT $1 OFFSET $2", limit, offset)
		if err != nil {
			log.Fatal(err)
		}
		defer crows.Close()
		var (
			cid    int
			symbol string
		)
		var wg sync.WaitGroup
		for crows.Next() {
			err := crows.Scan(&cid, &symbol)
			if err != nil {
				log.Fatal(err)
			}
			log.Println("computing: " + symbol)
			wg.Add(1)
			go func() {
				defer wg.Done()
				prows, err := db.Query("SELECT id, price FROM prices WHERE company_id = $1", cid)
				if err != nil {
					log.Fatal(err)
				}
				defer prows.Close()
				var (
					pid   int
					price float64
				)
				for prows.Next() {
					err := prows.Scan(&pid, &price)
					if err != nil {
						log.Fatal(err)
					}
					// log.Printf("price: %v", price)
					sma := price + 10
					errRow := db.QueryRow(`UPDATE prices SET sma = $1 WHERE id = $2 RETURNING id`, sma, pid).Scan(&pid)
					if errRow != nil {
						log.Fatalf("here: %+v", errRow)
					}
				}
			}()
			// Wait for all to complete.
		}
		wg.Wait()
	}
}
