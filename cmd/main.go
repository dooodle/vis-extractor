package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/knakk/rdf"
	_ "github.com/lib/pq"
	"io"
	"log"
	"os"
)

// note file suffix for a n triple is .nt
var fileName = flag.String("f", "", "filename to save N-Triple DB")
var verbose = flag.Bool("v", false, "output extra logging")

// https://newfivefour.com/postgresql-information-schema.html
// https://www.w3.org/TR/n-triples/
// https://en.wikipedia.org/wiki/N-Triples

var user = os.Getenv("VIS_MONDIAL_USER")
var dbname = os.Getenv("VIS_MONDIAL_DBNAME")
var password = os.Getenv("VIS_MONDIAL_PASSWORD")
var host = os.Getenv("VIS_MONDIAL_HOST")
var port = os.Getenv("VIS_MONDIAL_PORT")
var sslmode = os.Getenv("VIS_MONDIAL_SSLMODE")

var db *sql.DB

const (
	rootPrefix        = "http://dooodle/"
	colMiddle         = "/column/"
	compoundMiddle    = "/compound/"
	tablePrefix       = rootPrefix + "entity/"
	predPrefix        = rootPrefix + "predicate/"
	dataTypePrefix    = rootPrefix + "dataType/"
	discreteDimension = rootPrefix + "dimension/discrete"
	scalarDimension   = rootPrefix + "dimension/scalar"
	similarCond       = rootPrefix + "cond/similar"
	complete          = rootPrefix + "cond/complete"
)

func init() {
	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=%s", user, dbname, password, host, port, sslmode)
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	flag.Parse()
	var w io.Writer = os.Stdout
	if *fileName != "" {
		f, err := os.Create(*fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		w = f
	}
	if *verbose {
		fmt.Printf("starting db graph extractor for %s on %s:%s\n", dbname, host, port)
	}
	writeTableColS(w)
	writeColsDataType(w)
	counts := writeScalarOrDiscrete(w, 100)
	writeKeys(w)
	_ = writeCompoundKeys(w, counts)
	//	fmt.Println(keys)
}

//– discrete dimensions have a relatively small number of distinct values, that
//may nor may not have a natural ordering; they are used to choose a mark
//or to vary a channel of a mark.
//– scalar dimensions have a relatively large number of distinct values with a
//natural numeric ordering (e.g. integers, floats, timestamps, dates); these are
//represented by a channel associated with a mark.

//when extracting use limit to decide if its scalar or discrete
func writeScalarOrDiscrete(w io.Writer, limit int) map[string]int {
	counts := map[string]int{}
	// q := `SELECT columns.table_name,
	// 	  columns.column_name
	// FROM information_schema.columns
	// LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	// WHERE tables.table_schema = 'public'
	// `
	q := `SELECT 
		  columns.table_name,
		  columns.column_name,
		  columns.data_type,
		  columns.udt_name
	FROM information_schema.columns
	LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	WHERE tables.table_schema = 'public' 
	`

	rows, _ := db.Query(q)

	data := struct {
		tableName string
		colName   string
		dataType  string
		udtName   string
	}{}

	defer rows.Close()
	triples := []rdf.Triple{}

	for rows.Next() {
		err := rows.Scan(&(data.tableName), &(data.colName), &(data.dataType), &(data.udtName))
		if err != nil {
			fmt.Println(err)
		}

		subQuery := fmt.Sprintf("SELECT COUNT (DISTINCT %s) FROM %s", data.colName, data.tableName)
		subRows, err := db.Query(subQuery)
		if err != nil {
			fmt.Println(err)
		}

		for subRows.Next() {
			var count int
			err := subRows.Scan(&count)
			if err != nil {
				fmt.Println(err)
			}
			subject, _ := rdf.NewIRI(tablePrefix + data.tableName + colMiddle + data.colName)
			pred, _ := rdf.NewIRI(predPrefix + "numDistinct")
			object, _ := rdf.NewLiteral(count)
			triple := rdf.Triple{
				Subj: subject,
				Pred: pred,
				Obj:  object,
			}
			triples = append(triples, triple)
			counts[data.tableName+"/"+data.colName] = count
			dSubject, _ := rdf.NewIRI(tablePrefix + data.tableName + colMiddle + data.colName)
			dPred, _ := rdf.NewIRI(predPrefix + "hasDimension")
			var dObject rdf.IRI
			switch {
			case count <= 100:
				dObject, err = rdf.NewIRI(discreteDimension)
				if err != nil {
					fmt.Println(err)
				}
			case data.dataType == "integer" || data.dataType == "numeric":
				dObject, err = rdf.NewIRI(scalarDimension)
				if err != nil {
					fmt.Println(err)
				}
			}
			dtriple := rdf.Triple{
				Subj: dSubject,
				Pred: dPred,
				Obj:  dObject,
			}
			triples = append(triples, dtriple)
		}
	}

	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}

	return counts
}

func writeTableColS(w io.Writer) {
	q := `SELECT columns.table_name,
		  columns.column_name
	FROM information_schema.columns
	LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	WHERE tables.table_schema = 'public' 
	`

	rows, _ := db.Query(q)

	data := struct {
		tableName string
		colName   string
	}{}

	defer rows.Close()
	triples := []rdf.Triple{}

	for rows.Next() {
		err := rows.Scan(&(data.tableName), &(data.colName))
		if err != nil {
			fmt.Println(err)
		}

		subject, _ := rdf.NewIRI(tablePrefix + data.tableName)
		pred, _ := rdf.NewIRI(predPrefix + "hasColumn")
		object, _ := rdf.NewIRI(tablePrefix + data.tableName + colMiddle + data.colName)
		triple := rdf.Triple{
			Subj: subject,
			Pred: pred,
			Obj:  object,
		}
		triples = append(triples, triple)
	}

	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}
}

func writeKeys(w io.Writer) {
	q := `select tc.table_schema, tc.table_name, kc.column_name
             from information_schema.table_constraints tc
             join information_schema.key_column_usage kc 
             on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
             where tc.constraint_type = 'PRIMARY KEY'
             and kc.ordinal_position is not null
             order by tc.table_schema,
             tc.table_name,
             kc.position_in_unique_constraint;`

	rows, _ := db.Query(q)

	data := struct {
		tableSchema string
		tableName   string
		colName     string
	}{}

	defer rows.Close()
	triples := []rdf.Triple{}

	for rows.Next() {
		err := rows.Scan(&(data.tableSchema), &(data.tableName), &(data.colName))
		if err != nil {
			fmt.Println(err)
		}

		subject, _ := rdf.NewIRI(tablePrefix + data.tableName)
		pred, _ := rdf.NewIRI(predPrefix + "hasKey")
		object, _ := rdf.NewIRI(tablePrefix + data.tableName + colMiddle + data.colName)
		triple := rdf.Triple{
			Subj: subject,
			Pred: pred,
			Obj:  object,
		}
		triples = append(triples, triple)
	}

	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}

}

func writeCompoundKeys(w io.Writer, counts map[string]int) map[string][]string {
	q := `select tc.table_schema, tc.table_name, kc.column_name
             from information_schema.table_constraints tc
             join information_schema.key_column_usage kc 
             on kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name
             where tc.constraint_type = 'PRIMARY KEY'
             and kc.ordinal_position is not null
             order by tc.table_schema,
             tc.table_name,
             kc.position_in_unique_constraint;`

	rows, _ := db.Query(q)

	data := struct {
		tableSchema string
		tableName   string
		colName     string
	}{}

	defer rows.Close()

	//collect the keys

	keys := map[string][]string{}
	for rows.Next() {
		err := rows.Scan(&(data.tableSchema), &(data.tableName), &(data.colName))
		if err != nil {
			fmt.Println(err)
		}
		vals, ok := keys[data.tableName]
		if !ok {
			vals = []string{}
		}
		vals = append(vals, data.colName)
		keys[data.tableName] = vals
	}

	for k, v := range keys {
		// need to write out all possible poirs of keys
		if len(v) > 1 {
			subsets(w, counts, k, v)
		}
	}

	return keys
}

func subsets(w io.Writer, counts map[string]int, entity string, keys []string) {
	n := len(keys)
	var subset = make([]string, 0, n)
	triples := []rdf.Triple{}
	var search func(int)
	search = func(i int) {
		if i == n {
			if len(subset) == 2 {
				subject, _ := rdf.NewIRI(tablePrefix + entity)
				pred, _ := rdf.NewIRI(predPrefix + "hasCompoundKey")
				object, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + subset[0] + "/" + subset[1])
				triple := rdf.Triple{
					Subj: subject,
					Pred: pred,
					Obj:  object,
				}
				triples = append(triples, triple)
				switch {
				case counts[entity+"/"+subset[0]] > counts[entity+"/"+subset[1]]:
					subject, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + subset[0] + "/" + subset[1])
					pred, _ := rdf.NewIRI(predPrefix + "hasStrongKey")
					object, _ := rdf.NewIRI(tablePrefix + entity + colMiddle + subset[0])
					triple := rdf.Triple{
						Subj: subject,
						Pred: pred,
						Obj:  object,
					}
					triples = append(triples, triple)
					subject, _ = rdf.NewIRI(tablePrefix + entity + compoundMiddle + subset[0] + "/" + subset[1])
					pred, _ = rdf.NewIRI(predPrefix + "hasWeakKey")
					object, _ = rdf.NewIRI(tablePrefix + entity + colMiddle + subset[1])
					triple = rdf.Triple{
						Subj: subject,
						Pred: pred,
						Obj:  object,
					}
					triples = append(triples, triple)

				}
			}
			return
		}
		// include k in the subset
		subset = append(subset, keys[i])
		search(i + 1)
		// dont include k in the subset
		subset = subset[:len(subset)-1]
		search(i + 1)
	}

	search(0)
	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}

}

func writeColsDataType(w io.Writer) {

	q := `SELECT 
		  columns.table_name,
		  columns.column_name,
		  columns.data_type,
		  columns.udt_name
	FROM information_schema.columns
	LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	WHERE tables.table_schema = 'public' 
	`

	rows, _ := db.Query(q)

	data := struct {
		tableName string
		colName   string
		dataType  string
		udtName   string
	}{}

	defer rows.Close()
	triples := []rdf.Triple{}

	for rows.Next() {
		err := rows.Scan(&(data.tableName), &(data.colName), &(data.dataType), &(data.udtName))
		if err != nil {
			fmt.Println(err)
		}
		//		fmt.Println(data.colName, data.dataType)

		subject, _ := rdf.NewIRI(tablePrefix + data.tableName + colMiddle + data.colName)
		pred, _ := rdf.NewIRI(predPrefix + "hasDataType")
		object, _ := rdf.NewIRI(dataTypePrefix + data.udtName)
		triple := rdf.Triple{
			Subj: subject,
			Pred: pred,
			Obj:  object,
		}
		triples = append(triples, triple)
	}

	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}
}
