package gmig

import (
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type selectSet struct {
	tables []table
}

type table struct {
	name   string
	masks  []mask
	wheres []where
	result map[string][]string
}

type mask struct {
	key   string
	value string
}

type where struct {
	key      string
	value    string
	operator string
}

func (w where) build() string {
	return fmt.Sprintf("%s %s %s", w.key, w.operator, w.value)
}

func (t table) getWhereStatement() string {
	var WhereStatement []string
	for _, v := range t.wheres {
		WhereStatement = append(WhereStatement, v.build())
	}
	return strings.Join(WhereStatement, ",")
}

func (t table) checkMask(s string) (string, bool) {
	for _, v := range t.masks {
		if v.key == s {
			return v.value, true
		}
	}
	return "", false
}

func (t table) createSelectSQL() string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", t.name, t.getWhereStatement())
}

func (t table) exec(db *sqlx.DB) {
	rows, err := db.Queryx(t.createSelectSQL())
	if err != nil {
		log.Fatal(err)
	}
	t.result = make(map[string][]string)
	data := map[string]interface{}{}
	for rows.Next() {
		rows.MapScan(data)
		var key []string
		var value []string
		for i, v := range data {
			key = append(key, i)
			if val, ok := t.checkMask(i); ok {
				value = append(value, val)
				t.result[i] = append(t.result[i], val)
			} else {
				value = append(value, fmt.Sprintf("%s", v))
				t.result[i] = append(t.result[i], fmt.Sprintf("%s", v))
			}
		}
		fmt.Printf("insert INTO %s (%s) VALUES (%s);\n", t.name, strings.Join(key, ","), strings.Join(value, ","))
	}
}

func Gmig() {
	selectSet := selectSet{
		tables: []table{
			{
				name: "customer",
				masks: []mask{
					{
						key:   "email",
						value: "*****",
					},
				},
				wheres: []where{
					{
						key:      "last_name",
						value:    "'VANHORN'",
						operator: "=",
					},
				},
			},
			{
				name: "payment",
				masks: []mask{
					{
						key:   "rental_id",
						value: "0",
					},
				},
				wheres: []where{
					{
						key:      "customer_id",
						value:    "1",
						operator: "=",
					},
				},
			},
		},
	}

	db, err := sqlx.Open("mysql", "root:mysql@tcp(localhost:33306)/sakila")
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range selectSet.tables {
		v.exec(db)
	}
}
