package frontbase

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var database = frontbaseTestDB{}

// An empty type used as a namespace for test runner functions.
type frontbaseTestDB struct{}

// A test runner that
//  1. Creates a temporary database.
//  2. Prepares the database by executing the SQL in `setupSQL`;
//     that SQL must create a table named "t0".
//  3. Selects all rows from "t0".
//  4. Loops all rows and calls `verify` for the first row.
//  5. Checks that there was only one row and that no errors occurred.
func (frontbaseTestDB) RunTestOneRow(t *testing.T, setupSQL string, verify func(*testing.T, *sql.Rows)) {
	tdb := createTempdb(t)
	defer tdb.tearDown()

	tdb.mustExec(setupSQL)

	rows, err := tdb.db.Query("select * from t0;")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	nrows := 0
	for rows.Next() {
		nrows += 1

		if nrows == 1 {
			verify(t, rows)
		}
	}

	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}

	if nrows != 1 {
		t.Errorf("expected 1 row, got %d", nrows)
	}
}

// A verifier. Returns a function that scans a single value from
// the supplied *sql.Rows and compares it to the `expected` value.
func expectOneCol[V comparable](expected V) func(*testing.T, *sql.Rows) {
	return func(t *testing.T, rows *sql.Rows) {
		var actual V

		if err := rows.Scan(&actual); err != nil {
			t.Error(err)
		}

		if actual != expected {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	}
}

// A temporary in-process, FrontBase database.
type tempdb struct {
	dir string
	db  *sql.DB
	t   *testing.T
}

// Create a temporary database within the context of test `t`.
// If anything goes wrong `t` is aborted.
func createTempdb(t *testing.T) tempdb {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}

	dbpath := filepath.Join(tempDir, "foo.db")
	dburl := fmt.Sprintf("file:///%s", dbpath)

	db, err := sql.Open("frontbase", dburl)
	if err != nil {
		t.Fatalf("foo.db open failed: %v", err)
	}

	return tempdb{
		dir: tempDir,
		db:  db,
		t:   t,
	}
}

// Close the database and remove the associated files from disk.
func (tdb tempdb) tearDown() {
	tdb.db.Close()
	os.RemoveAll(tdb.dir)
}

// Exec() the supplied `sql` and `args` and return the result.
// The associated test `tdb.t` is aborted if the there's a failure.
func (tdb tempdb) mustExec(sql string, args ...interface{}) sql.Result {
	res, err := tdb.db.Exec(sql, args...)
	if err != nil {
		tdb.t.Fatalf("Error running %q: %v", sql, err)
	}
	return res
}

// Create a Time from `spec`. The specification is on the
// form "yyyy-MM-dd HH:mm:ss.SSS" and is interpreted in UTC.
func utcTime(t *testing.T, spec string) time.Time {
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		t.Fatal(err)
	}

	stamp, err := time.ParseInLocation("2006-01-02 03:04:05.000", spec, loc)
	if err != nil {
		t.Fatal(err)
	}

	return stamp
}
