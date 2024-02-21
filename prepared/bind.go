package prepared

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type bindError struct {
	Msg string
}

func (err bindError) Error() string {
	return err.Msg
}

//
// Bind values
//

func (stmt Stmt) BindNamed(args []driver.NamedValue) (string, error) {
	// each arg has .Name, .Ordinal (starting at 1) and .Value)

	// loop nodes
	// on text, write
	// on placeholder,
	//  has name, find the value with corresponding name, or err
	//  has ordinal n, find n'th unamed arg

	// from args,
	// create map from vname => idx in args
	//   (only if prepped has named placehold?)
	//   no always, but if prepped has no named, barf on first named in args
	// create map from ordinal of unamed arg (renumber from 1) => idx in args
	//   if prepped has no unamed, barf on first unamed in args

	namedArgs := make(map[string]int, len(args))
	ordinalArgs := make([]driver.NamedValue, 0)

	for i, arg := range args {
		if arg.Name != "" {
			if len(stmt.namedPlaceholderNames) == 0 {
				return "", bindError{
					Msg: fmt.Sprintf("can't bind named value %s, statement has no named placeholders", arg.Name)}
			}

			namedArgs[arg.Name] = i
		} else {
			if stmt.numOrdinalPlaceholders == 0 {
				return "", bindError{Msg: "can't bind ordinal value when statement has no ordinal placeholders"}
			}
			ordinalArgs = append(ordinalArgs, arg)
		}
	}

	if stmt.numOrdinalPlaceholders != len(ordinalArgs) {
		return "", bindError{
			Msg: fmt.Sprintf("can't bind, expected %d ordinal args, got %d", stmt.numOrdinalPlaceholders, len(ordinalArgs)),
		}
	}

	sql := strings.Builder{}

	nextOrdinalIdx := 0
	notVisitedNamedArgs := make(map[string]int, len(namedArgs))
	for k := range namedArgs {
		notVisitedNamedArgs[k] = 42
	}

	for _, node := range stmt.nodes {
		switch node.Type {

		case text:
			sql.WriteString(node.Text)

		case placeholder:
			var val interface{}

			if node.Text != "" {
				valIdx, ok := namedArgs[node.Text]
				if !ok {
					return "", bindError{Msg: fmt.Sprintf("can't bind, missing named arg %s", node.Text)}
				}
				val = args[valIdx].Value
				delete(notVisitedNamedArgs, node.Text)
			} else {
				val = ordinalArgs[nextOrdinalIdx].Value
				nextOrdinalIdx++
			}
			encoded := encodeValue(val)
			sql.WriteString(encoded)
		default:
			panic("won't happen")
		}
	}

	for left := range notVisitedNamedArgs {
		return "", bindError{Msg: fmt.Sprintf("can't bind named value %s, no matching named placeholder", left)}
	}

	return sql.String(), nil
}

func (stmt Stmt) Bind(args []driver.Value) (string, error) {
	sql := strings.Builder{}

	nextValueIdx := 0

	for _, node := range stmt.nodes {
		switch node.Type {
		case text:
			sql.WriteString(node.Text)
		case placeholder:
			// todo: check args len
			encoded := encodeValue(args[nextValueIdx])
			sql.WriteString(encoded)
			nextValueIdx++
		default:
			panic("won't happen")
		}
	}

	// todo: check args left

	return sql.String(), nil
}

//
// Utilities
//

func encodeValue(x interface{}) string {
	switch v := x.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10) // may overflow SQL long int!
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		result := make([]byte, 3+hex.EncodedLen(len(v)))
		result[0] = 'x'
		result[1] = '\''
		result[len(result)-1] = '\''
		hex.Encode(result[2:len(result)-1], v)
		return string(result)
	case nil:
		return "NULL"
	case string:
		return fmt.Sprintf("'%s'", strings.Replace(v, `'`, `''`, -1))
	case time.Time:
		return fmt.Sprintf("TIMESTAMP '%s'", v.UTC().Format("2006-01-02 15:04:05.000"))
	default:
		panic(fmt.Sprintf("encode: unknown type for %T", v))
	}

	panic("never reached")
}
