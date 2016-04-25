package main

import (
	"database/sql"
	"log"

	"fmt"
	"github.com/yydzero/mnt/util/reflect"
	_ "github.com/lib/pq"
	"flag"
	"sync"
)

var port int
var count int

func main() {
	log.SetFlags(log.Ltime | log.Lshortfile)
	flag.IntVar(&port, "p", 5432, "Default port to connect")
	flag.IntVar(&count, "c", 10, "Default port to connect")
	flag.Parse()

	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)
		go testOneServer(&wg, port + i)
	}

	wg.Wait()
}

func testOneServer(wg *sync.WaitGroup, port int) {
	defer wg.Done()

	// Now use lib/pq to send some info.
	url := fmt.Sprintf("user=%s dbname=test host=localhost port=%d sslmode=disable", reflect.GetCurrentUsername(), port)
	db, err := sql.Open("postgres", url)
	if err != nil {
		log.Fatal(err)
	}

	testSimpleQuery(db)
	//testExtendedQuery(db)
}

func testSimpleQuery(db *sql.DB) {
	// Simple Query
	age := 27
	rows, err := db.Query("SELECT name, age, description FROM users WHERE age>20")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate Query Result.
	for rows.Next() {
		var name string
		var desc string
		if err := rows.Scan(&name, &age, &desc); err != nil {
			log.Fatal(err)
		}
		//fmt.Printf("%s is %d, %q\n", name, age, desc)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func testExtendedQuery(db *sql.DB) {
	age := 20
	rows, err := db.Query("SELECT name, age, description FROM users WHERE age > $1", age)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	// Iterate Query Result.
	for rows.Next() {
		var name string
		var desc string
		if err := rows.Scan(&name, &age, &desc); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s is %d, %q\n", name, age, desc)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
