package frontbase

import (
	"bytes"
	"fmt"
	"testing"
)

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

	scanAndCompare := func(sqlPart string, expected []byte) {
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

	for _, btype := range []string{"bit", "bit varying"} {
		scanAndCompare(fmt.Sprintf("x'00' as %s(8)", btype), []byte{0x00})
		scanAndCompare(fmt.Sprintf("x'01' as %s(8)", btype), []byte{0x01})
		scanAndCompare(fmt.Sprintf("x'42' as %s(8)", btype), []byte{0x42})
		scanAndCompare(fmt.Sprintf("x'ff' as %s(8)", btype), []byte{0xff})

		scanAndCompare(fmt.Sprintf("x'0000' as %s(16)", btype), []byte{0, 0})
		scanAndCompare(fmt.Sprintf("x'0100' as %s(16)", btype), []byte{1, 0})
		scanAndCompare(fmt.Sprintf("x'0001' as %s(16)", btype), []byte{0, 1})
		scanAndCompare(fmt.Sprintf("x'abcd' as %s(16)", btype), []byte{0xab, 0xcd})

		scanAndCompare(fmt.Sprintf("x'000000' as %s(24)", btype), []byte{0, 0, 0})
		scanAndCompare(fmt.Sprintf("x'000100' as %s(24)", btype), []byte{0, 1, 0})
		scanAndCompare(fmt.Sprintf("x'010000' as %s(24)", btype), []byte{1, 0, 0})

		scanAndCompare(fmt.Sprintf("x'012345678901234567890fec' as %s(96)", btype), []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0x01, 0x23, 0x45, 0x67, 0x89, 0x0f, 0xec})
	}
}

func TestQuery_double(t *testing.T) {
	tdb := createTempdb(t)
	defer tdb.tearDown()

	scanAndCompare := func(sqlPart string, expected float64) {
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

	scanAndCompare := func(sqlPart string, expected float64) {
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
