package prepared

import (
	"database/sql/driver"
	"log"
	"testing"
	"time"
)

func TestBindNamed(t *testing.T) {
	fixture := []struct {
		name     string
		sql      string
		values   []driver.NamedValue
		expected string
		failure  string
	}{
		{
			"one named value",
			"select * from t where a = @n1;",
			[]driver.NamedValue{
				{Ordinal: 1, Name: "n1", Value: int64(42)},
			},
			"select * from t where a = 42;",
			"",
		},
		{
			"two named values",
			"select * from t where a = @n1 and b = @n2;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
				{Name: "n2", Value: "fourty-two", Ordinal: 2},
			},
			"select * from t where a = 42 and b = 'fourty-two';",
			"",
		},
		{
			"repeated named values",
			"select * from t where a = @n1 and b = @n1;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
			},
			"select * from t where a = 42 and b = 42;",
			"",
		},
		{
			"repeated named values, another in-between",
			"select * from t where a = @n1 and b = @n2 and c = @n1;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
				{Name: "n2", Value: "fourty-two", Ordinal: 2},
			},
			"select * from t where a = 42 and b = 'fourty-two' and c = 42;",
			"",
		},
		{
			"missing named value",
			"select * from t where a = @n1 and b = @n2;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
			},
			"",
			"can't bind, missing named arg n2",
		},
		{
			"named value args but no placeholder",
			"select * from t where a = ?;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
			},
			"",
			"can't bind named value n1, statement has no named placeholders",
		},
		{
			"too many named value args",
			"select * from t where a = @n1;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
				{Name: "n2", Value: "fourty-two", Ordinal: 2},
			},
			"",
			"can't bind named value n2, no matching named placeholder",
		},
		{
			"mixed named and ordinal values",
			"select * from t where a = @n1 and b = ?;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
				{Name: "", Value: "fourty-two", Ordinal: 2},
			},
			"select * from t where a = 42 and b = 'fourty-two';",
			"",
		},
		{
			"mixed named and ordinal values, reverse order of args",
			"select * from t where a = @n1 and b = ?;",
			[]driver.NamedValue{
				{Name: "", Value: "fourty-two", Ordinal: 1},
				{Name: "n1", Value: int64(42), Ordinal: 2},
			},
			"select * from t where a = 42 and b = 'fourty-two';",
			"",
		},
		{
			"mixed ordinal and named values",
			"select * from t where a = ? and b = @n1;",
			[]driver.NamedValue{
				{Name: "n1", Value: int64(42), Ordinal: 1},
				{Name: "", Value: "fourty-two", Ordinal: 2},
			},
			"select * from t where a = 'fourty-two' and b = 42;",
			"",
		},
		{
			"one ordinal value",
			"select * from t where a = ?;",
			[]driver.NamedValue{
				{Name: "", Value: "fourty-two", Ordinal: 1},
			},
			"select * from t where a = 'fourty-two';",
			"",
		},
		{
			"two ordinal values",
			"select * from t where a = ? and b = ?;",
			[]driver.NamedValue{
				{Name: "", Value: "fourty-two", Ordinal: 1},
				{Name: "", Value: int64(42), Ordinal: 2},
			},
			"select * from t where a = 'fourty-two' and b = 42;",
			"",
		},
		{
			"missing ordinal value",
			"select * from t where a = ? and b = ?;",
			[]driver.NamedValue{
				{Name: "", Value: "fourty-two", Ordinal: 1},
			},
			"",
			"can't bind, expected 2 ordinal args, got 1",
		},
	}

	numFails := 0

	for _, tcase := range fixture {
		prepped, err := ParseSQL(tcase.sql)

		if err != nil {
			t.Errorf("case '%s' unexpected parse error '%v'", tcase.name, err)
			numFails += 1
			continue
		}

		actual, err := prepped.BindNamed(tcase.values)

		if err != nil && tcase.failure == "" {
			t.Errorf("case '%s' unexpected error '%v'", tcase.name, err)
			numFails += 1
			continue
		}

		if tcase.failure != "" {
			if err == nil {
				t.Errorf("case '%s' expected error '%s', got '%s'", tcase.name, tcase.failure, actual)
				numFails += 1
				continue
			}

			if err.Error() != tcase.failure {
				t.Errorf("case '%s' expected error '%s' but got '%v'", tcase.name, tcase.failure, err)
				numFails += 1
				continue
			}
		}

		if tcase.expected != actual {
			t.Errorf("case '%s' expected '%s' but got '%s'", tcase.name, tcase.expected, actual)
			numFails += 1
			continue
		}
	}

	if numFails > 0 {
		t.Errorf("%d test cases, %d failed", len(fixture), numFails)
	}
}

func TestBind(t *testing.T) {
	prepped, err := ParseSQL("select * from t where c1 = ? and c2 = ?;")
	if err != nil {
		t.Fatal("ParseSQL failed:", err)
	}

	args := []driver.Value{
		int64(42),
		"fourty-two",
	}

	sql, err := prepped.Bind(args)
	if err != nil {
		t.Fatal("Bind failed:", err)
	}

	expected := "select * from t where c1 = 42 and c2 = 'fourty-two';"
	if sql != expected {
		t.Fatal("expected", expected, "but got", sql)
	}
}

func TestEncode(t *testing.T) {
	fixture := []struct {
		name     string
		value    driver.Value
		expected string
	}{
		{"int", int64(1), "1"},
		{"int", int64(-123), "-123"},

		{"float", float64(1), "1"},
		{"float", float64(1.23), "1.23"},
		{"float", float64(-1.897), "-1.897"},

		{"bool", true, "true"},
		{"bool", false, "false"},

		{"bytes", []byte{0xde, 0xad, 0xbe, 0xef}, "x'deadbeef'"},
		{"bytes", []byte{0xde}, "x'de'"},
		{"bytes", []byte{0x00}, "x'00'"},
		{"bytes", []byte{0x00, 0x00}, "x'0000'"},
		{"bytes", []byte{0x01, 0x00}, "x'0100'"},
		{"bytes", []byte{0x00, 0x10}, "x'0010'"},

		{"null", nil, "NULL"},

		{"string", "hello", "'hello'"},
		{"string", "'", "''''"},
		{"string", "'nice'", "'''nice'''"},

		{"local time expressed in UTC", swedishTime("2022-10-14 10:23:59.123"), "TIMESTAMP '2022-10-14 08:23:59.123'"},
		{"UTC time stays in UTC", utcTime("2022-10-14 10:23:59.123"), "TIMESTAMP '2022-10-14 10:23:59.123'"},
	}

	for _, tcase := range fixture {
		acutal := encodeValue(tcase.value)
		if acutal != tcase.expected {
			t.Errorf("case '%s' expected <%s> but got <%v>", tcase.name, tcase.expected, acutal)
		}
	}
}

func swedishTime(spec string) time.Time {
	loc, err := time.LoadLocation("Europe/Stockholm")
	if err != nil {
		log.Fatal(err)
	}

	t, err := time.ParseInLocation("2006-01-02 03:04:05.000", spec, loc)
	if err != nil {
		log.Fatal(err)
	}

	return t
}

func utcTime(spec string) time.Time {
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatal(err)
	}

	t, err := time.ParseInLocation("2006-01-02 03:04:05.000", spec, loc)
	if err != nil {
		log.Fatal(err)
	}

	return t
}
