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
	Name   string  `yaml:Name`
	Masks  []Mask  `yaml:Masks`
	Wheres []where `yaml:Wheres`
	Result map[string][]string
}

type Mask struct {
	Key   string `yaml:Key`
	Value string `yaml:Value`
}

type where struct {
	Key      string `yaml:Key`
	Value    string `yaml:Value`
	Operator string `yaml:Operator`
	DependOn string `yaml:DependOn`
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

func (t *Table) exec(db *sqlx.DB) {
	rows, err := db.Queryx(t.createSelectSQL())
	if err != nil {
		log.Fatal(err)
	}
	Result := make(map[string][]string)

	data := map[string]interface{}{}
	for rows.Next() {
		rows.MapScan(data)
		var key []string
		var value []string
		for i, v := range data {
			key = append(key, i)
			if val, ok := t.checkMask(i); ok {
				value = append(value, val)
				Result[i] = append(Result[i], val)
			} else {
				value = append(value, fmt.Sprintf("%s", v))
				Result[i] = append(Result[i], fmt.Sprintf("%s", v))
			}
		}
		fmt.Printf("insert INTO %s (%s) Values (%s);\n", t.Name, strings.Join(key, ","), strings.Join(value, ","))
	}
	t.Result = Result
}

func (d Dataset) Exec() {
	db, err := sqlx.Open("mysql", "root:mysql@tcp(localhost:33306)/sakila")
	if err != nil {
		log.Fatal(err)
	}
	for i, v := range d.Tables {
		d.setChainWhere(v.Name)
		v.exec(db)
		d.Tables[i] = v
	}
}

func (d *Dataset) setChainWhere(target string) {
	i := position(d.Tables, target)
	if i == -1 {
		return
	}
	key := d.Tables[i].haveDependon()
	if key == -1 {
		return
	}
	j := position(d.Tables, d.Tables[i].Wheres[key].DependOn)
	d.Tables[i].Wheres[key].Value = strings.Join(d.Tables[j].Result[d.Tables[i].Wheres[key].Key], ",")
}

func position(target []Table, find string) int {
	for i, v := range target {
		if v.Name == find {
			return i
		}
	}
	return -1
}
func (t Table) haveDependon() int {
	for i, v := range t.Wheres {
		if v.DependOn != "" {
			return i
		}
	}
	return -1
}
