package gmig

import (
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Dataset struct {
	Tables []Table `yaml:Tables`
}

type Table struct {
	Name   string              `yaml:Name`
	Masks  []Mask              `yaml:Masks`
	Wheres []where             `yaml:Wheres`
	Result map[string][]string `yaml:Result`
}

type Mask struct {
	Key   string `yaml:Key`
	Value string `yaml:Value`
}

type where struct {
	Key      string `yaml:Key`
	Value    string `yaml:Value`
	Operator string `yaml:Operator`
}

func (w where) build() string {
	return fmt.Sprintf("%s %s %s", w.Key, w.Operator, w.Value)
}

func (t Table) getWheretatement() string {
	var Wheretatement []string
	for _, v := range t.Wheres {
		Wheretatement = append(Wheretatement, v.build())
	}
	return strings.Join(Wheretatement, ",")
}

func (t Table) checkMask(s string) (string, bool) {
	for _, v := range t.Masks {
		if v.Key == s {
			return v.Value, true
		}
	}
	return "", false
}

func (t Table) createSelectSQL() string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s", t.Name, t.getWheretatement())
}

func (t Table) exec(db *sqlx.DB) {
	rows, err := db.Queryx(t.createSelectSQL())
	if err != nil {
		log.Fatal(err)
	}
	t.Result = make(map[string][]string)
	data := map[string]interface{}{}
	for rows.Next() {
		rows.MapScan(data)
		var key []string
		var value []string
		for i, v := range data {
			key = append(key, i)
			if val, ok := t.checkMask(i); ok {
				value = append(value, val)
				t.Result[i] = append(t.Result[i], val)
			} else {
				value = append(value, fmt.Sprintf("%s", v))
				t.Result[i] = append(t.Result[i], fmt.Sprintf("%s", v))
			}
		}
		fmt.Printf("insert INTO %s (%s) ValueS (%s);\n", t.Name, strings.Join(key, ","), strings.Join(value, ","))
	}
}

func (d Dataset) Exec() {
	db, err := sqlx.Open("mysql", "root:mysql@tcp(localhost:33306)/sakila")
	if err != nil {
		log.Fatal(err)
	}
	for _, v := range d.Tables {
		v.exec(db)
	}
}
