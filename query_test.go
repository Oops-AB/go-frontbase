package frontbase

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var database = frontbaseTestDB{}

func TestQuery_tinyint(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val tinyint ); insert into t0 values ( 127 );",
		expectOneCol(int8(127)))

	database.RunTestOneRow(t,
		"create table t0 ( val tinyint ); insert into t0 values ( -128 );",
		expectOneCol(int8(-128)))
}

func TestQuery_smallint(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val smallint ); insert into t0 values ( 32767 );",
		expectOneCol(int16(32767)))

	database.RunTestOneRow(t,
		"create table t0 ( val smallint ); insert into t0 values ( -32768 );",
		expectOneCol(int16(-32768)))
}

func TestQuery_int(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val int ); insert into t0 values ( 2147483647 );",
		expectOneCol(int32(2147483647)))

	database.RunTestOneRow(t,
		"create table t0 ( val int ); insert into t0 values ( -2147483648 );",
		expectOneCol(int32(-2147483648)))
}

func TestQuery_longint(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val longint ); insert into t0 values ( 9223372036854775807 );",
		expectOneCol(int64(9223372036854775807)))

	database.RunTestOneRow(t,
		"create table t0 ( val longint ); insert into t0 values ( -9223372036854775808 );",
		expectOneCol(int64(-9223372036854775808)))
}

func TestQuery_boolean(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val boolean ); insert into t0 values ( true );",
		expectOneCol(bool(true)))

	database.RunTestOneRow(t,
		"create table t0 ( val boolean ); insert into t0 values ( false );",
		expectOneCol(bool(false)))
}

func TestQuery_timestamp(t *testing.T) {
	expected := utcTime(t, "2024-03-16 01:02:03.000").Local()

	// Note that the driver always sets the session timezone to UTC.
	// Timestamp encoding and decoding depend on this.
	database.RunTestOneRow(t,
		"create table t0 ( val timestamp ); insert into t0 values ( timestamp '2024-03-16 01:02:03' );",
		expectOneCol(expected))
}

func TestQuery_character(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val character(10) ); insert into t0 values ( 'helo world' );",
		expectOneCol("helo world"))

	database.RunTestOneRow(t,
		"create table t0 ( val character(10) ); insert into t0 values ( '' );",
		expectOneCol(""))
}

func TestQuery_character_varying(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( val character varying(9797) ); insert into t0 values ( 'helo world' );",
		expectOneCol("helo world"))

	database.RunTestOneRow(t,
		"create table t0 ( val character varying(9797) ); insert into t0 values ( '' );",
		expectOneCol(""))
}

func TestQuery_bit(t *testing.T) {
	tdb := createTempdb(t)
	defer tdb.tearDown()

	scanAndCompare := func (sqlPart string, expected []byte) {
		t.Logf("scanning: %s", sqlPart)
		var actual []byte
		err := tdb.db.QueryRow("values (cast(" + sqlPart + "));").Scan(&actual)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(expected, actual) {
			t.Errorf("expected %#v got %#v", expected, actual)
		}
	}

	for _, btype := range []string{"bit","bit varying"} {
		scanAndCompare(fmt.Sprintf("x'00' as %s(8)", btype), []byte{0x00})
		scanAndCompare(fmt.Sprintf("x'01' as %s(8)", btype), []byte{0x01})
		scanAndCompare(fmt.Sprintf("x'42' as %s(8)", btype), []byte{0x42})
		scanAndCompare(fmt.Sprintf("x'ff' as %s(8)", btype), []byte{0xff})

		scanAndCompare(fmt.Sprintf("x'0000' as %s(16)", btype), []byte{0,0})
		scanAndCompare(fmt.Sprintf("x'0100' as %s(16)", btype), []byte{1,0})
		scanAndCompare(fmt.Sprintf("x'0001' as %s(16)", btype), []byte{0,1})
		scanAndCompare(fmt.Sprintf("x'abcd' as %s(16)", btype), []byte{0xab, 0xcd})

		scanAndCompare(fmt.Sprintf("x'000000' as %s(24)", btype), []byte{0,0,0})
		scanAndCompare(fmt.Sprintf("x'000100' as %s(24)", btype), []byte{0,1,0})
		scanAndCompare(fmt.Sprintf("x'010000' as %s(24)", btype), []byte{1,0,0})

		scanAndCompare(fmt.Sprintf("x'012345678901234567890fec' as %s(96)", btype), []byte{0x01,0x23,0x45,0x67,0x89,0x01,0x23,0x45,0x67,0x89,0x0f,0xec})
	}
}

func TestQuery_double(t *testing.T) {
	tdb := createTempdb(t)
	defer tdb.tearDown()

	scanAndCompare := func (sqlPart string, expected float64) {
		t.Logf("scanning: %s", sqlPart)
		var actual float64
		err := tdb.db.QueryRow("values (cast(" + sqlPart + "));").Scan(&actual)
		if err != nil {
			t.Fatal(err)
		}
		if expected != actual {
			t.Errorf("expected %#v got %#v", expected, actual)
		}
	}

	// from a test in github.com/lib/pq
	scanAndCompare("0.10000122 as float", float64(0.10000122))
	scanAndCompare("35.03554004971999 as double precision", float64(35.03554004971999))
	scanAndCompare("1.2 as float", float64(1.2))

	// and with double precision substituted for float.
	scanAndCompare("0.10000122 as double precision", float64(0.10000122))
	scanAndCompare("1.2 as double precision", float64(1.2000000000000002))
}

func TestQuery_decimal_defaults_to_double(t *testing.T) {
	tdb := createTempdb(t)
	defer tdb.tearDown()

	scanAndCompare := func (sqlPart string, expected float64) {
		t.Logf("scanning: %s", sqlPart)
		var actual float64
		err := tdb.db.QueryRow("values (cast(" + sqlPart + "));").Scan(&actual)
		if err != nil {
			t.Fatal(err)
		}
		if expected != actual {
			t.Errorf("expected %#v got %#v", expected, actual)
		}
	}

	scanAndCompare("0.10000122 as decimal(8,8)", float64(0.10000122))
	scanAndCompare("35.03554004971999 as decimal(16,14)", float64(35.03554004971999))
	scanAndCompare("1.2 as decimal(2,1)", float64(1.2000000000000002))
}

func TestQuery_NULL_tinyint(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val tinyint ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[int8]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_smallint(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val smallint ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[int16]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_int(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val int ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[int32]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_longint(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val longint ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[int64]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_boolean(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val boolean ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[bool]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_timestamp(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val timestamp(6) ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[time.Time]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_character(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val character(10) ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[string]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_character_varying(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val character varying(9797) ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[string]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_bits(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val bit(16) ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[[]byte]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_double(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val double precision ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[float64]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

func TestQuery_NULL_decimal(t *testing.T) {
	database.RunTestOneRow(t,
		"create table t0 ( id int not null, val decimal(3,5) ); insert into t0 values ( 1, NULL );",
		func(t *testing.T, rows *sql.Rows) {
			var id int32
			var val sql.Null[float64]
			if err := rows.Scan(&id, &val); err != nil {
				t.Error(err)
			}
			if val.Valid {
				t.Errorf("expected NULL but got %v", val)
			}
		})
}

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
