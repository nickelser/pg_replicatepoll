package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/lib/pq"
)

/*
   General heuristic for the stupidest replication mechanism known
   ------------------

		State:
			- timestamp of the last known update
			- current offset count

		Algorithm:
			- find the last updated at for the target table
			- fetch a batch of records from the source table that are newer than the previous step's value
			- copy them into the target DB
			- if we reached our batch size for
*/

const version string = "v0.1.0"
const usage string = `pg_replicatepoll: the stupidest replication

Usage:
  pg_replicatepoll --source=postgres://user:pass@db.freepostgresservers.com/sourcedb --target=postgres://user:pass@db2.freepostgresservers.com/targetdb --table=users

Options:
  -h --help     Show this screen.
  --source      Source Postgres database URL. (required)
  --target      Target Postgres database URL. (required)
  --table       Table to transfer. (required)
  --tscol       Timestamp column name on the table. [default: updated_at]
  --limit       Limit (in rows) per copy. [default: 2000]
  --polling-hz  Times to sync the two tables, per second. Maximum rows/s is simply limit * polling-hz. [default: 3]
`

const reporterUpdateInterval uint64 = 5 // seconds
const defaultMaxBatchSize int64 = 2000  // rows
const defaultPollHz float64 = 3         // per/s
const minPollInterval time.Duration = 100 * time.Millisecond

/* Global state (hooray) */
var l = log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)

var sourceDB *sql.DB
var targetDB *sql.DB

var table string
var pk string
var tsCol string
var offset uint64
var firstRun string
var limit uint64
var last time.Time
var updates uint64
var sleepPeriod time.Duration

type tableConfig struct {
	ready          bool
	cols           []string
	colSort        string
	onConflictStmt string
	castColList    string
	colList        string
	lastUpdate     time.Time
}

var globalTableConfig atomic.Value

func fatal(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		l.Fatalln(file, ":", line, "-", err)
	}
}

func info(err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		l.Println(file, ":", line, "-", err)
	}
}

func main() {
	sourceDBURL := flag.String("source", "", "Source Postgres database URL. (Required)")
	targetDBURL := flag.String("target", "", "Target Postgres database URL. (Required)")
	tableArg := flag.String("table", "", "Table to transfer. (Required)")
	tsColArg := flag.String("tscol", "updated_at", "Timestamp column.")
	limitArg := flag.Int64("limit", defaultMaxBatchSize, "Limit on rows transferred at once.")
	pollHzArg := flag.Float64("polling-hz", defaultPollHz, "Times to poll the source DB, per second (can be < 1).")

	flag.Parse()

	if *sourceDBURL == "" || *targetDBURL == "" || *tableArg == "" || *tsColArg == "" {
		fmt.Fprintf(os.Stderr, usage)
		os.Exit(1)
	}

	table = *tableArg
	tsCol = *tsColArg

	sourceDB = openDB(*sourceDBURL)
	targetDB = openDB(*targetDBURL)

	globalTableConfig.Store(tableConfig{ready: false})

	offset = 0
	firstRun = "="
	limit = uint64(*limitArg)
	sleepPeriod = time.Duration(float64(1)/(*pollHzArg)*1000) * time.Millisecond

	var wg sync.WaitGroup

	wg.Add(3)

	go copy()
	go schemer()
	go reporter()

	l.Println(fmt.Sprintf("connected to source and target DBs, and beginning to poll for changes at %vhz", *pollHzArg))

	wg.Wait()
}

func openDB(uri string) *sql.DB {
	db, err := sql.Open("postgres", uri)
	fatal(err)

	if err = db.Ping(); err != nil {
		fatal(err)
	}

	return db
}

func scanStringsIntoArray(rows *sql.Rows) []string {
	var str string
	var ret []string

	for rows.Next() {
		err := rows.Scan(&str)
		fatal(err)

		ret = append(ret, str)
	}

	return ret
}

func schemer() {
	stmt := fmt.Sprintf(
		`SELECT column_name
				FROM information_schema.columns
				WHERE table_name = '%s'`, table)

	sourceStmt, err := sourceDB.Prepare(stmt)
	fatal(err)

	targetStmt, err := targetDB.Prepare(stmt)
	fatal(err)

	// find sortable constraints
	// https://dba.stackexchange.com/a/75905
	sortStmt, err := sourceDB.Prepare(fmt.Sprintf(`
		SELECT string_agg('%s.' || quote_ident(attname), ',')
			FROM (
			   SELECT conrelid AS attrelid, conkey
			   FROM   pg_catalog.pg_constraint
			   WHERE  conrelid = '%s'::regclass::oid
			   AND    contype = ANY ('{p,u}'::"char"[]) -- primary or unique constraint
			   ORDER  BY contype                        -- pk has priority ('p' < 'u')
			           , array_length(conkey, 1)        -- else, fewest columns as quick tie breaker
			   LIMIT  1                                 -- ... but any 1 of them is good
			   )   c
			JOIN   unnest(c.conkey) attnum ON TRUE      -- unnest array, possibly mult. cols
			JOIN   pg_catalog.pg_attribute a USING (attrelid, attnum)

			UNION  ALL                                  -- Default if first query returns no row
			SELECT '%s::text COLLATE "C"'
			LIMIT  1`, table, table, table))
	fatal(err)

	// https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
	err = sourceDB.QueryRow(fmt.Sprintf(`
		SELECT a.attname
			FROM   pg_index i
			JOIN   pg_attribute a ON a.attrelid = i.indrelid
		                     	  AND a.attnum = ANY(i.indkey)
			WHERE  i.indrelid = '%s'::regclass
			AND    i.indisprimary
			LIMIT 1`, table)).Scan(&pk)
	fatal(err)

	l.Println(fmt.Sprintf("found primary key for %s: %s", table, pk))

	for {
		var cols []string
		var colSort string

		sourceRows, err := sourceStmt.Query()
		fatal(err)

		targetRows, err := targetStmt.Query()
		fatal(err)

		sourceColumns := scanStringsIntoArray(sourceRows)
		targetColumns := scanStringsIntoArray(targetRows)

		for _, targetColname := range targetColumns {
			found := false

			for _, sourceColname := range sourceColumns {
				if sourceColname == targetColname {
					cols = append(cols, targetColname)
					found = true
					break
				}
			}

			if found {
				continue
			}

			if targetColname == pk {
				panic("missing primary key on source/target db")
			}

			if targetColname == tsCol {
				panic("missing timestamp column on source/target db")
			}
		}

		err = sortStmt.QueryRow().Scan(&colSort)
		fatal(err)

		onConflictStmt := fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET ", pk)

		for _, v := range cols {
			if v == pk {
				continue // no reason to conflict on the primary key
			}

			// "excluded" is a special table that references the newly inserted
			// now-conflicting row (that we want to use to override whatever is in the table already)
			onConflictStmt += fmt.Sprintf("%s = excluded.%s,", v, v)
		}

		onConflictStmt = strings.TrimSuffix(onConflictStmt, ",")

		castColList := strings.Trim(strings.Replace(fmt.Sprint(cols), " ", "::text, ", -1), "[]")
		colList := strings.Trim(strings.Replace(fmt.Sprint(cols), " ", ", ", -1), "[]")

		globalTableConfig.Store(tableConfig{true, cols, colSort, onConflictStmt, castColList, colList, time.Now()})

		time.Sleep(15 * time.Second)
	}
}

func reporter() {
	lastStmt := fmt.Sprintf("SELECT MAX(%s) FROM %s", tsCol, table)

	sourceStmt, err := sourceDB.Prepare(lastStmt)
	fatal(err)

	targetStmt, err := targetDB.Prepare(lastStmt)
	fatal(err)

	var sourceLast time.Time
	var targetLast time.Time

	for {
		err = sourceStmt.QueryRow().Scan(&sourceLast)
		info(err)

		err = targetStmt.QueryRow().Scan(&targetLast)
		info(err)

		delta := sourceLast.Sub(targetLast)

		updatesPerSecond := atomic.LoadUint64(&updates) / reporterUpdateInterval
		atomic.StoreUint64(&updates, 0)

		l.Println(fmt.Sprintf("current lag for %s is %vms, %v updates/s", table, delta.Nanoseconds()/1000000, updatesPerSecond))

		time.Sleep(time.Duration(reporterUpdateInterval) * time.Second)
	}
}

func copy() {
	for {
		start := time.Now()

		offsetWas := offset

		rowCount, err := copyRows()
		info(err)

		delta := time.Since(start)

		if rowCount > 0 {
			atomic.AddUint64(&updates, rowCount)
			l.Println(fmt.Sprintf("%vms elapsed for %v rows, offset %v rows", int(delta.Seconds()*1000), rowCount, offset))
		}

		if sleepPeriod-delta < minPollInterval {
			if offset == 0 && offsetWas == 0 {
				l.Println(fmt.Sprintf("unable to keep up with updates, consider lowering the poll frequency; last poll took %vs", delta))
			}

			time.Sleep(minPollInterval)
		} else {
			time.Sleep(sleepPeriod - delta)
		}
	}
}

func copyRows() (uint64, error) {
	var rows *sql.Rows
	var lastStmt *sql.Stmt
	var err error
	var txn *sql.Tx
	var insertStmt *sql.Stmt
	var filterClause string

	c := globalTableConfig.Load().(tableConfig)

	if !c.ready {
		return 0, nil
	}

	rowCount := uint64(0)

	if offset == 0 {
		// our basic case -- refresh the last timestamp from the target,
		// and fetch updated rows
		if lastStmt == nil {
			lastStmt, err = targetDB.Prepare(fmt.Sprintf("SELECT MAX(%s) FROM %s", tsCol, table))

			if err != nil {
				return 0, err
			}
		}

		err = lastStmt.QueryRow().Scan(&last)

		// base case for an empty table
		if err != nil {
			last = time.Unix(0, 0).UTC()
		}

		rows, err = sourceDB.Query(fmt.Sprintf(
			`SELECT %s FROM %s
				WHERE %s >%s $1
				%s
				ORDER BY %s ASC, %s
				LIMIT %d`, c.castColList, table, tsCol, firstRun, filterClause, tsCol, c.colSort, limit), last)

		firstRun = ""
	} else {
		// we are continuing to transfer rows which have the same timestamp
		rows, err = sourceDB.Query(fmt.Sprintf(
			`SELECT %s FROM %s
				WHERE %s = $1
				%s
				ORDER BY %s ASC, %s
				LIMIT %d
				OFFSET %d`, c.castColList, table, tsCol, filterClause, tsCol, c.colSort, limit, offset), last)
	}

	if err != nil {
		return 0, err
	}

	defer rows.Close()

	for rows.Next() {
		pointers := make([]interface{}, len(c.cols))
		scanArgs := make([]interface{}, len(c.cols))

		for i := range pointers {
			scanArgs[i] = &pointers[i]
		}

		err = rows.Scan(scanArgs...)

		if err != nil {
			return 0, err
		}

		if txn == nil {
			// lazily create our transaction only once we've hit our first result row
			txn, err = targetDB.Begin()

			if err != nil {
				return 0, err
			}

			// create a temporary table into which we can copy the rows
			_, err = txn.Exec(fmt.Sprintf(`
				CREATE TEMP TABLE tmp ON COMMIT DROP
					AS SELECT %s FROM %s LIMIT 0`, c.colList, table))

			if err != nil {
				return 0, err
			}

			// begin our copy using the target/source overlap cols
			insertStmt, err = txn.Prepare(pq.CopyIn("tmp", c.cols...))

			if err != nil {
				return 0, err
			}
		}

		_, err = insertStmt.Exec(scanArgs...)

		if err != nil {
			return 0, err
		}

		rowCount++
	}

	if rowCount == limit {
		// in this case, we are at a batch limit, so we assume for this timestamp
		// there are more records available (so start fetching offsets)
		offset += limit
	} else {
		offset = 0

		// we didn't actually find any changes, so keep spinnin'
		if rowCount == 0 {
			return 0, nil
		}
	}

	// recommended by the lib/pq docs to find any errors during Exec (as it's async)
	_, err = insertStmt.Exec()

	if err != nil {
		return 0, err
	}

	// finish the COPY
	err = insertStmt.Close()

	if err != nil {
		return 0, err
	}

	// and transfer the COPY'd rows to the actual table
	// this also updates existing rows (using the conflict statement)
	_, err = txn.Exec(fmt.Sprintf(`
		INSERT INTO %s
			SELECT * FROM tmp
			%s`, table, c.onConflictStmt))

	if err != nil {
		return 0, err
	}

	// if we are fetching batches, find the value we need to match against
	if offset > 0 {
		err = txn.QueryRow(fmt.Sprintf("SELECT MAX(%s) FROM tmp", tsCol)).Scan(&last)

		if err != nil {
			return 0, err
		}
	}

	// actually insert the records!
	err = txn.Commit()

	if err != nil {
		return 0, err
	}

	return rowCount, nil
}
