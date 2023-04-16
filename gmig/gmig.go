package gmig

import (
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func Gmig() {
	db, err := sqlx.Open("mysql", "root:mysql@tcp(localhost:33306)/sakila")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Queryx("SELECT * FROM customer")
	if err != nil {
		log.Fatal(err)
	}
	data := map[string]interface{}{}
	for rows.Next() {
		rows.MapScan(data)
		fmt.Println(data)
	}

}
