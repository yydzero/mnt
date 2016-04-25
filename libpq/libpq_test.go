package libpq_test

import (
	. "github.com/yydzero/mnt/libpq"

	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
	"net"
	"time"
)

var _ = Describe("libpq spec", func() {
	port := "8899"

	It("should able to establish connection", func() {
		log.SetFlags(log.Ltime | log.Lshortfile)

		go startServer(port)

		time.Sleep(10 * time.Millisecond)

		// Now use lib/pq to send some info.
		url := fmt.Sprintf("user=pqgotest dbname=pqgotest port=%s sslmode=disable", port)
		db, err := sql.Open("postgres", url)
		if err != nil {
			panic(err)
		}

		age := 20
		rows, err := db.Query("SELECT name FROM users WHERE age = $1", age)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			panic(err)
		}

		Expect(len(cols)).Should(Equal(3))

		if rows.Next() {
			var name string
			var desc string
			if err := rows.Scan(&name, &age, &desc); err != nil {
				log.Fatal(err)
			}
			Expect(name).Should(Equal("xiaowang"))
			Expect(desc).Should(Equal("SMTS"))
		} else {
			log.Fatalln("Expect rows, while has no row")
		}

	})
})

func startServer(port string) {
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	log.Println("Listening on :8899")

	for {
		conn, err := ln.Accept()
		if err != nil {
			panic(err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	s := NewServer()
	s.Serve(conn)
}
