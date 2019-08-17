package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	_ "github.com/lib/pq"
	"github.com/knakk/rdf"
)
// https://newfivefour.com/postgresql-information-schema.html

var user = os.Getenv("VIS_MONDIAL_USER")
var dbname = os.Getenv("VIS_MONDIAL_DBNAME")
var password = os.Getenv("VIS_MONDIAL_PASSWORD")
var host = os.Getenv("VIS_MONDIAL_HOST")
var port = os.Getenv("VIS_MONDIAL_PORT")
var sslmode = os.Getenv("VIS_MONDIAL_SSLMODE")

var db *sql.DB

func init() {
	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=%s", user, dbname, password, host, port, sslmode)
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
}


func main() {
	fmt.Printf("starting db graph extractor for %s on %s:%s\n", dbname, host, port)
	//q := "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
	//WriteQuery(os.Stdout,q,false,false)
	writeTableColS(os.Stdout,100)
}

//– discrete dimensions have a relatively small number of distinct values, that
//may nor may not have a natural ordering; they are used to choose a mark
//or to vary a channel of a mark.
//– scalar dimensions have a relatively large number of distinct values with a
//natural numeric ordering (e.g. integers, floats, timestamps, dates); these are
//represented by a channel associated with a mark.

//when extracting use limit to decide if its scalar or discrete
func writeScalarOrDiscrete(w io.Writer, limit int) {
	//q := "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"

	q := `SELECT columns.table_name,
		  columns.column_name,
		  columns.data_type,
		  columns.is_nullable
	FROM information_schema.columns
	LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	WHERE tables.table_schema = 'public' 
	`

	rows, _ := db.Query(q)

	data := struct{
		tableName string
		colName string
		dataType string
		isNullable string
	}{}

	defer rows.Close()

	triples := []rdf.Triple{}

	for rows.Next() {
		err := rows.Scan(&(data.tableName),&(data.colName),&(data.dataType),&(data.isNullable))
		if err != nil {
			fmt.Println(err)
		}

		subject, _ := rdf.NewIRI("http://dooodle/" + data.tableName)
		pred,_ := rdf.NewIRI("http://dooodle/hasColumn")
		object, _ := rdf.NewIRI("http://dooodle/" + data.colName)
		triples = append(triples,rdf.Triple{
			Subj : subject,
			Pred: pred,
			Obj: object,
		})
		//fmt.Println(data)
	}
	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}

}

func writeTableColS(w io.Writer, limit int) {
	//q := "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
fmt.Println("entering")
	q := `SELECT columns.table_name,
		  columns.column_name
	FROM information_schema.columns
	LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	WHERE tables.table_schema = 'public' 
	`

	rows, _ := db.Query(q)

	data := struct{
		tableName string
		colName string
	}{}

	defer rows.Close()
	triples := []rdf.Triple{}

	for rows.Next() {
		err := rows.Scan(&(data.tableName),&(data.colName))
		if err != nil {
			fmt.Println(err)
		}

		subject, _ := rdf.NewIRI("http://dooodle/" + data.tableName)
		pred,_ := rdf.NewIRI("http://dooodle/hasColumn")
		object, _ := rdf.NewIRI("http://dooodle/" + data.colName)
		triple := rdf.Triple{
			Subj : subject,
			Pred: pred,
			Obj: object,
		}
		triples = append(triples,triple)
	}

	//for _, t := range triples {
	//	str := t.Serialize(rdf.NTriples)
	//	w.Write([]byte(str))
	//}

	for _, t := range triples {
		str := t.Serialize(rdf.Turtle)
		w.Write([]byte(str))
	}
}

func WriteQuery(w io.Writer, q string, header bool, null bool) {
	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=%s", user, dbname, password, host, port, sslmode)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		w.Write([]byte(err.Error()))
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(q)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if header {
		w.Write([]byte(strings.Join(cols, ",") + "\n"))
	}
	if err != nil {
		log.Fatalf("Columns: %v", err)
	}

	_vals := make([]sql.NullString, len(cols))
	vals := make([]interface{}, len(cols))
	for i, _ := range cols {
		vals[i] = &_vals[i]
	}

	hasOneInvalidCol := false
	for rows.Next() {
		err := rows.Scan(vals...)
		if err != nil {
			log.Fatal(err)
		}
		outs := make([]string, 0)
		for i := range cols {
			v := vals[i].(*sql.NullString)
			if v.Valid {
				outs = append(outs, strconv.Quote(v.String))
			} else {
				hasOneInvalidCol = true
				outs = append(outs, "")
			}
		}
		if !hasOneInvalidCol {
			csv := strings.Join(outs, ",")
			w.Write([]byte(csv + "\n"))
		}
		//reset flag
		hasOneInvalidCol = false
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}

