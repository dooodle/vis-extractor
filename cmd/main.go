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
	"strings"
)

// note file suffix for a n triple is .nt
var fileName = flag.String("f", "", "filename to save N-Triple DB")
var verbose = flag.Bool("v", false, "output extra logging")

// useful reading material
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
	one2mMiddle    	  = "/one2many/"
	m2mMiddle    	  = "/many2many/"
	tablePrefix       = rootPrefix + "entity/"
	predPrefix        = rootPrefix + "predicate/"
	dataTypePrefix    = rootPrefix + "dataType/"
	discreteDimension = rootPrefix + "dimension/discrete"
	scalarDimension   = rootPrefix + "dimension/scalar"
	similarCond       = rootPrefix + "cond/similar"
	complete          = rootPrefix + "cond/complete"

	similarHeuristic = 15
)

func init() {
	connStr := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%s sslmode=%s",
		user, dbname, password, host, port, sslmode)
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
	//write out the triples
	writeTableColS(w)
	writeColsDataType(w)
	counts := writeScalarOrDiscrete(w, 100)
	writeKeys(w)
	_ = writeCompoundKeys(w, counts)
	writeOneOrManyToManyRels(w)
}

//some reference definitions from the principal paper.
//– discrete dimensions have a relatively small number of distinct values, that
//may nor may not have a natural ordering; they are used to choose a mark
//or to vary a channel of a mark.
//– scalar dimensions have a relatively large number of distinct values with a
//natural numeric ordering (e.g. integers, floats, timestamps, dates); these are
//represented by a channel associated with a mark.

//when extracting use limit to decide if its scalar or discrete
func writeScalarOrDiscrete(w io.Writer, limit int) map[string]int {
	counts := map[string]int{}
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
			case !strings.Contains(data.colName,"latitude") && !strings.Contains(data.colName, "longitude") && (data.dataType == "integer" || data.dataType == "numeric"): // need a better way to exclude geo data like this
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

	singleTriples := []rdf.Triple{}
	for k, v := range keys {
		// need to write out all possible poirs of keys
		if len(v) == 1 {
			//single key
			subject, _ := rdf.NewIRI(tablePrefix + k)
			pred, _ := rdf.NewIRI(predPrefix + "hasSingleKey")
			object, _ := rdf.NewIRI(tablePrefix + k + colMiddle + v[0])
			triple := rdf.Triple{
				Subj: subject,
				Pred: pred,
				Obj:  object,
			}
			singleTriples = append(singleTriples, triple)
		}

		if len(v) > 1 {
			subsetsForCompound(w, k, v, writeCompoundItem)
		}
	}
	for _, t := range singleTriples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}
	return keys
}

func writeOneOrManyToManyRels(w io.Writer) map[string][]string {
	//compare all possible cols for all tables
	if *verbose {
		log.Println("extracting one to many")
	}

	q := `SELECT columns.table_name,
		  columns.column_name
	FROM information_schema.columns
	LEFT JOIN information_schema.tables ON columns.table_name = tables.table_name
	WHERE tables.table_schema = 'public' 
	`

	rows, _ := db.Query(q)

	data := struct {
		tableName   string
		colName     string
	}{}

	defer rows.Close()

	//collect the keys
	keys := map[string][]string{}
	for rows.Next() {
		err := rows.Scan(&(data.tableName), &(data.colName))
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

	singleTriples := []rdf.Triple{}
	for k, v := range keys {
		if len(v) > 1 {
			subsetsForOneOrManyToMany(w, k, v)
		}
	}
	for _, t := range singleTriples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}
	return keys
}

func subsetsForOneOrManyToMany(w io.Writer, entity string, keys []string) {
	if *verbose {
		log.Printf("entering subset streamer for %s:%v",entity,keys)
	}
	n := len(keys)
	var subset = make([]string, 0, n)
	triples := []rdf.Triple{}
	var search func(int)
	search = func(i int) {
		if i == n {
			if len(subset) == 2 {
				writeOneOrManyToManyItem(w, entity, subset[0], subset[1])
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

//example sql
//select iata_code, count(distinct city)  from airport group by iata_code having count(distinct city) > 1;
//select max(output) from (select iata_code, count(distinct city) as output from airport group by iata_code) as Derived ;
func writeOneOrManyToManyItem(w io.Writer, entity string, col1 string, col2 string) {
	if *verbose {
		log.Printf("entering one to many checker for %s:%s,%s",entity,col1,col2)
	}

	q1 := fmt.Sprintf("select max(output) from (select %s, count(distinct %s) as output from %s group by %s) as Derived",col1,col2,entity,col1)
	q2 := fmt.Sprintf("select max(output) from (select %s, count(distinct %s) as output from %s group by %s) as Derived",col2,col1,entity,col2)

	i1,i2 := 0,0
	rows1, err := db.Query(q1)
	if err != nil {
		fmt.Println(err)
	}
	rows2, err := db.Query(q2)
	if err != nil {
		fmt.Println(err)
	}
	defer rows1.Close()
	defer rows2.Close()

	for rows1.Next() {
		err := rows1.Scan(&i1)
		if err != nil && *verbose { // could be null
			fmt.Println(err)
		}
	}

	for rows2.Next() {
		err := rows2.Scan(&i2)
		if err != nil && *verbose { // could be null
			fmt.Println(err)
		}
	}
	triples := []rdf.Triple{}
	if *verbose {
		log.Printf("%s -> %v",col1,i1)
		log.Printf("%s -> %v",col2,i2)
	}
	switch {
	//one to many key relationships
	case i1 == 1 && i2 > 1 :
		subject, _ := rdf.NewIRI(tablePrefix + entity)
		pred, _ := rdf.NewIRI(predPrefix + "hasOne2ManyKey")
		object, _ := rdf.NewIRI(tablePrefix + entity + one2mMiddle + col2 + "/" + col1)
		triple := rdf.Triple{
			Subj: subject,
			Pred: pred,
			Obj:  object,
		}
		triples = append(triples, triple)
		subjectOne, _ := rdf.NewIRI(tablePrefix + entity + one2mMiddle + col2 + "/" + col1)
		predOne, _ := rdf.NewIRI(predPrefix + "hasOneKey")
		objectOne, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col2)
		tripleOne := rdf.Triple{
			Subj: subjectOne,
			Pred: predOne,
			Obj:  objectOne,
		}
		triples = append(triples, tripleOne)
		subjectMany, _ := rdf.NewIRI(tablePrefix + entity + one2mMiddle + col2 + "/" + col1)
		predMany, _ := rdf.NewIRI(predPrefix + "hasManyKey")
		objectMany, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col1)
		tripleMany := rdf.Triple{
			Subj: subjectMany,
			Pred: predMany,
			Obj:  objectMany,
		}
		triples = append(triples, tripleMany)


	case i2 == 1 && i1 > 1 :
		subject, _ := rdf.NewIRI(tablePrefix + entity)
		pred, _ := rdf.NewIRI(predPrefix + "hasOne2ManyKey")
		object, _ := rdf.NewIRI(tablePrefix + entity + one2mMiddle + col1 + "/" + col2)
		triple := rdf.Triple{
			Subj: subject,
			Pred: pred,
			Obj:  object,
		}
		triples = append(triples, triple)
		subjectOne, _ := rdf.NewIRI(tablePrefix + entity + one2mMiddle + col1 + "/" + col2)
		predOne, _ := rdf.NewIRI(predPrefix + "hasOneKey")
		objectOne, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col1)
		tripleOne := rdf.Triple{
			Subj: subjectOne,
			Pred: predOne,
			Obj:  objectOne,
		}
		triples = append(triples, tripleOne)
		subjectMany, _ := rdf.NewIRI(tablePrefix + entity + one2mMiddle + col1 + "/" + col2)
		predMany, _ := rdf.NewIRI(predPrefix + "hasManyKey")
		objectMany, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col2)
		tripleMany := rdf.Triple{
			Subj: subjectMany,
			Pred: predMany,
			Obj:  objectMany,
		}
		triples = append(triples, tripleMany)
		// many to many key relatioships
	case i1 > 1 && i2 > 1 :
		subject, _ := rdf.NewIRI(tablePrefix + entity)
		pred, _ := rdf.NewIRI(predPrefix + "hasMany2ManyKey")
		object, _ := rdf.NewIRI(tablePrefix + entity + m2mMiddle + col1 + "/" + col2)
		triple := rdf.Triple{
			Subj: subject,
			Pred: pred,
			Obj:  object,
		}
		triples = append(triples, triple)
		triples = append(triples, triple)
		subjectManyOne, _ := rdf.NewIRI(tablePrefix + entity + m2mMiddle + col1 + "/" + col2)
		predManyOne, _ := rdf.NewIRI(predPrefix + "hasManyKey")
		objectManyOne, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col1)
		tripleManyOne := rdf.Triple{
			Subj: subjectManyOne,
			Pred: predManyOne,
			Obj:  objectManyOne,
		}
		triples = append(triples, tripleManyOne)
		subjectManyTwo, _ := rdf.NewIRI(tablePrefix + entity + m2mMiddle + col1 + "/" + col2)
		predManyTwo, _ := rdf.NewIRI(predPrefix + "hasManyKey")
		objectManyTwo, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col2)
		tripleManyTwo := rdf.Triple{
			Subj: subjectManyTwo,
			Pred: predManyTwo,
			Obj:  objectManyTwo,
		}
		triples = append(triples, tripleManyTwo)
	}

	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}


}

func subsetsForCompound(w io.Writer, entity string, keys []string, f func(io.Writer, string, string, string)) {
	n := len(keys)
	var subset = make([]string, 0, n)
	triples := []rdf.Triple{}
	var search func(int)
	search = func(i int) {
		if i == n {
			if len(subset) == 2 {
				f(w , entity, subset[0],subset[1])
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

func writeCompoundItem(w io.Writer, entity string, col1 string, col2 string) {
	if *verbose {
		log.Printf("entering one to many checker for %s:%s,%s",entity,col1,col2)
	}

	q1 := fmt.Sprintf("select max(output) from (select %s, count(distinct %s) as output from %s group by %s) as Derived",col1,col2,entity,col1)
	q2 := fmt.Sprintf("select max(output) from (select %s, count(distinct %s) as output from %s group by %s) as Derived",col2,col1,entity,col2)

	i1,i2 := 0,0
	rows1, err := db.Query(q1)
	if err != nil {
		fmt.Println(err)
	}
	rows2, err := db.Query(q2)
	if err != nil {
		fmt.Println(err)
	}
	defer rows1.Close()
	defer rows2.Close()

	for rows1.Next() {
		err := rows1.Scan(&i1)
		if err != nil && *verbose { // could be null
			fmt.Println(err)
		}
	}

	for rows2.Next() {
		err := rows2.Scan(&i2)
		if err != nil && *verbose { // could be null
			fmt.Println(err)
		}
	}
	triples := []rdf.Triple{}
	if *verbose {
		log.Printf("%s -> %v",col1,i1)
		log.Printf("%s -> %v",col2,i2)
	}

	subject, _ := rdf.NewIRI(tablePrefix + entity)
	pred, _ := rdf.NewIRI(predPrefix + "hasCompoundKey")
	object, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + col1 + "/" + col2)
	triple := rdf.Triple{
		Subj: subject,
		Pred: pred,
		Obj:  object,
	}
	triples = append(triples, triple)
	switch {
	//one to many key relationships
	case i1 >= 10 &&  i2 >=10 && i1 < i2 :
		if *verbose {
			log.Printf(" in check :: compound checker for %s:%s->%d,%s->%d", entity, col1, i1, col2, i2)
		}
		subjectOne, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + col1 + "/" + col2)
		predOne, _ := rdf.NewIRI(predPrefix + "hasStrongKey")
		objectOne, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col1)
		tripleOne := rdf.Triple{
			Subj: subjectOne,
			Pred: predOne,
			Obj:  objectOne,
		}
		triples = append(triples, tripleOne)
		subjectMany, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + col1 + "/" + col2)
		predMany, _ := rdf.NewIRI(predPrefix + "hasWeakKey")
		objectMany, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col2)
		tripleMany := rdf.Triple{
			Subj: subjectMany,
			Pred: predMany,
			Obj:  objectMany,
		}
		triples = append(triples, tripleMany)


	case i1 >= 10 &&  i2 >=10 && i1 > i2 :
		if *verbose {
			log.Printf(" in check :: compound checker for %s:%s->%d,%s->%d", entity, col1, i1, col2, i2)
		}
		subjectOne, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + col1 + "/" + col2)
		predOne, _ := rdf.NewIRI(predPrefix + "hasStrongKey")
		objectOne, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col2)
		tripleOne := rdf.Triple{
			Subj: subjectOne,
			Pred: predOne,
			Obj:  objectOne,
		}
		triples = append(triples, tripleOne)
		subjectMany, _ := rdf.NewIRI(tablePrefix + entity + compoundMiddle + col1 + "/" + col2)
		predMany, _ := rdf.NewIRI(predPrefix + "hasWeakKey")
		objectMany, _  := rdf.NewIRI(tablePrefix + entity + colMiddle + col1)
		tripleMany := rdf.Triple{
			Subj: subjectMany,
			Pred: predMany,
			Obj:  objectMany,
		}
		triples = append(triples, tripleMany)
		// many to many key relatioships
	case i1 > 1 && i2 > 1 :
		//no op
	}

	for _, t := range triples {
		str := t.Serialize(rdf.NTriples)
		w.Write([]byte(str))
	}


}

func isSimilarIsComplete(entity string, key1 string, key2 string) (bool, bool) {
	q := `SELECT 
		  ` + key1 + `,
	          count(` + key2 + `)
	FROM ` + entity + `
        GROUP BY ` + key1 + `
	`

	rows, err := db.Query(q)
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	// fmt.Println("-----")
	// fmt.Println(entity, key1, key2)
	isFirst := true
	isSimilar := false
	i := 0
	for rows.Next() {
		var val1 string
		var val2 int
		err := rows.Scan(&val1, &val2)
		if err != nil {
			fmt.Println(err)
		}
		//		fmt.Println(val1, val2)
		if val2 > similarHeuristic {
			isSimilar = true
		}
		if isFirst {
			i = val2
			isFirst = false
			continue
		}
		if val2 != i {
			return isSimilar, false
		}

	}
	//	fmt.Println("---------> Complete??")
	return isSimilar, true
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
