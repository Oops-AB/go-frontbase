package frontbase

// Check out query_test.go for test support infrastructure

import (
	"database/sql"
	"testing"
	"time"
)

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
