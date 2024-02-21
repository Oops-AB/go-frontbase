package prepared

import (
	"testing"
)

// test text
// - delimited identifiers (also quoted identifiers). Examples:
//	   "table"    -> table
//     ""         ->
//     "'"        -> '
//     "''"       -> ''
//     "'''"      -> '''
//     """"       -> "
//     "a""b"     -> a"b
//     """name""" -> "name"
//     ...
// - string constants. Examples:
//     'str'     -> str
//     ''        ->
//     '"'       -> "
//     '""'      -> ""
//     '"""'     -> """
//     ''''      -> '
//     'a''b'    -> a'b
//     '''str''' -> 'str'
//     ...
// =>
//     '?'       -> text('?')
//     ''?       -> [text(''),placeholder]
//     '''?      -> illegal!
//       -> ch 1 enter str const
//       -> ch 2 single quote esc char, wait...
//       -> ch 3 single quote esc char, confirm
//       -> ch 4 char '?'
//       -> EOF in str const => error!
// ...

// DONE barf on unbalanced quoted identifiers?

// DONE test only text
// DONE test one placeholder
// DONE test several placeholders

// DONE test ignore placeholder within factor
// DONE test ignore placeholder within literal

// test ignore comments

// DONE test named placeholder

func TestParseSQL(t *testing.T) {
	fixture := []struct {
		name     string
		sql      string
		expected *Stmt
		failure  string
	}{
		{
			"single text node",
			"select * from table;",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table;", 0},
				},
			},
			"",
		},

		//
		// Ordinal placeholders
		//

		{
			"one placeholder",
			"select * from table where col = ?;",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table where col = ", 0},
					{placeholder, "", 1},
					{text, ";", 0},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"several placeholders",
			"select * from table where col1 = ? and col2 = ? and col3 = ?;",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table where col1 = ", 0},
					{placeholder, "", 1},
					{text, " and col2 = ", 0},
					{placeholder, "", 2},
					{text, " and col3 = ", 0},
					{placeholder, "", 3},
					{text, ";", 0},
				},
				numOrdinalPlaceholders: 3,
			},
			"",
		},
		{
			"placeholder at end",
			"select * from table where col = ?",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table where col = ", 0},
					{placeholder, "", 1},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"placeholder at start (degenerate)",
			"? some sql",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "", 1},
					{text, " some sql", 0},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"two placeholders back to back (degenerate)",
			"some ?? sql",
			&Stmt{
				nodes: []statementNode{
					{text, "some ", 0},
					{placeholder, "", 1},
					{placeholder, "", 2},
					{text, " sql", 0},
				},
				numOrdinalPlaceholders: 2,
			},
			"",
		},
		{
			"three placeholders back to back (degenerate)",
			"some ??? sql",
			&Stmt{
				nodes: []statementNode{
					{text, "some ", 0},
					{placeholder, "", 1},
					{placeholder, "", 2},
					{placeholder, "", 3},
					{text, " sql", 0},
				},
				numOrdinalPlaceholders: 3,
			},
			"",
		},
		{
			"lone placeholder (degenerate)",
			"?",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "", 1},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},

		//
		// Named placeholders
		//

		{
			"one named placeholder",
			"select * from table where col = @col;",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table where col = ", 0},
					{placeholder, "col", 1},
					{text, ";", 0},
				},
				namedPlaceholderNames: []string{"col"},
			},
			"",
		},
		{
			"several named placeholders",
			"select * from table where col1 = @col1 and col2 = @COL2 and col3 = @col3;",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table where col1 = ", 0},
					{placeholder, "col1", 1},
					{text, " and col2 = ", 0},
					{placeholder, "COL2", 2},
					{text, " and col3 = ", 0},
					{placeholder, "col3", 3},
					{text, ";", 0},
				},
				namedPlaceholderNames: []string{"col1", "COL2", "col3"},
			},
			"",
		},
		{
			"named placeholder at end",
			"select * from table where col = @col",
			&Stmt{
				nodes: []statementNode{
					{text, "select * from table where col = ", 0},
					{placeholder, "col", 1},
				},
				namedPlaceholderNames: []string{"col"},
			},
			"",
		},
		{
			"empty named placeholder",
			"select * from table where c1 = @ and c2 = @c2;",
			nil,
			"empty named placeholder",
		},
		{
			"empty named placeholder at end",
			"select * from table where col = @",
			nil,
			"empty named placeholder",
		},
		{
			"named placeholder at start (degenerate)",
			"@place some sql",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "place", 1},
					{text, " some sql", 0},
				},
				namedPlaceholderNames: []string{"place"},
			},
			"",
		},
		{
			"two named placeholders back to back (degenerate)",
			"some @place1@place2 sql",
			&Stmt{
				nodes: []statementNode{
					{text, "some ", 0},
					{placeholder, "place1", 1},
					{placeholder, "place2", 2},
					{text, " sql", 0},
				},
				namedPlaceholderNames: []string{"place1", "place2"},
			},
			"",
		},
		{
			"three named placeholders back to back (degenerate)",
			"some @place1@place2@place3 sql",
			&Stmt{
				nodes: []statementNode{
					{text, "some ", 0},
					{placeholder, "place1", 1},
					{placeholder, "place2", 2},
					{placeholder, "place3", 3},
					{text, " sql", 0},
				},
				namedPlaceholderNames: []string{"place1", "place2", "place3"},
			},
			"",
		},
		{
			"lone named placeholder (degenerate)",
			"@place",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "place", 1},
				},
				namedPlaceholderNames: []string{"place"},
			},
			"",
		},

		//
		// String constants
		//

		{
			"string constant with single placeholder",
			"'?'",
			&Stmt{
				nodes: []statementNode{
					{text, "'?'", 0},
				},
			},
			"",
		},
		{
			"string constant followed by placeholder, 1",
			"''?",
			&Stmt{
				nodes: []statementNode{
					{text, "''", 0},
					{placeholder, "", 1},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"string constant followed by placeholder, 2",
			"''''?",
			&Stmt{
				nodes: []statementNode{
					{text, "''''", 0},
					{placeholder, "", 1},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"placeholder followed by string constant, 1",
			"?''",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "", 1},
					{text, "''", 0},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"placeholder followed by string constant, 2",
			"?''''",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "", 1},
					{text, "''''", 0},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"string constant with escaped single-quotes and placeholder, 1",
			"'''?'''",
			&Stmt{
				nodes: []statementNode{
					{text, "'''?'''", 0},
				},
			},
			"",
		},
		{
			"string constant with escaped single-quotes and placeholder, 2",
			"'?'''",
			&Stmt{
				nodes: []statementNode{
					{text, "'?'''", 0},
				},
			},
			"",
		},
		{
			"string constant with escaped single-quotes and placeholder, 3",
			"'''?'",
			&Stmt{
				nodes: []statementNode{
					{text, "'''?'", 0},
				},
			},
			"",
		},
		{
			"string constant with single named placeholder",
			"'@place'",
			&Stmt{
				nodes: []statementNode{
					{text, "'@place'", 0},
				},
			},
			"",
		},
		{
			"string constant not closed, 1",
			"'",
			nil,
			"string constant not closed",
		},
		{
			"string constant not closed, 2",
			"'''",
			nil,
			"string constant not closed",
		},
		{
			"string constant not closed, 3",
			"?'",
			nil,
			"string constant not closed",
		},
		{
			"string constant not closed, 4",
			"a'",
			nil,
			"string constant not closed",
		},

		//
		// Quoted identifiers
		//

		{
			"quoted identifier with single placeholder",
			"\"?\"",
			&Stmt{
				nodes: []statementNode{
					{text, "\"?\"", 0},
				},
			},
			"",
		},
		{
			"quoted identifier followed by placeholder, 1",
			"\"\"?",
			&Stmt{
				nodes: []statementNode{
					{text, "\"\"", 0},
					{placeholder, "", 1},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"quoted identifier followed by placeholder, 2",
			"\"\"\"\"?",
			&Stmt{
				nodes: []statementNode{
					{text, "\"\"\"\"", 0},
					{placeholder, "", 1},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"placeholder followed by quoted identifier, 1",
			"?\"\"",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "", 1},
					{text, "\"\"", 0},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"placeholder followed by quoted identifier, 2",
			"?\"\"\"\"",
			&Stmt{
				nodes: []statementNode{
					{placeholder, "", 1},
					{text, "\"\"\"\"", 0},
				},
				numOrdinalPlaceholders: 1,
			},
			"",
		},
		{
			"quoted identifier with escaped double-quotes and placeholder, 1",
			"\"\"\"?\"\"\"",
			&Stmt{
				nodes: []statementNode{
					{text, "\"\"\"?\"\"\"", 0},
				},
			},
			"",
		},
		{
			"quoted identifier with escaped double-quotes and placeholder, 2",
			"\"?\"\"\"",
			&Stmt{
				nodes: []statementNode{
					{text, "\"?\"\"\"", 0},
				},
			},
			"",
		},
		{
			"quoted identifier with escaped double-quotes and placeholder, 3",
			"\"\"\"?\"",
			&Stmt{
				nodes: []statementNode{
					{text, "\"\"\"?\"", 0},
				},
			},
			"",
		},
		{
			"quoted identifier with single named placeholder",
			"\"@place\"",
			&Stmt{
				nodes: []statementNode{
					{text, "\"@place\"", 0},
				},
			},
			"",
		},
		{
			"quoted identifier not closed, 1",
			"\"",
			nil,
			"quoted identifier not closed",
		},
		{
			"quoted identifier not closed, 2",
			"\"\"\"",
			nil,
			"quoted identifier not closed",
		},
		{
			"quoted identifier not closed, 3",
			"?\"",
			nil,
			"quoted identifier not closed",
		},
		{
			"quoted identifier not closed, 4",
			"a\"",
			nil,
			"quoted identifier not closed",
		},

		//
		// Mixed string constants and quoted identifiers
		//

		{
			"single-quote in quoted identifier",
			"\"'\"",
			&Stmt{
				nodes: []statementNode{
					{text, "\"'\"", 0},
				},
			},
			"",
		},
		{
			"double-quote in string constant",
			"'\"'",
			&Stmt{
				nodes: []statementNode{
					{text, "'\"'", 0},
				},
			},
			"",
		},

		//
		// Mixed ordinal and named placeholders
		//

		{
			"three named and ordinal placeholders back to back (degenerate), 1",
			"some @place1?@place2 sql",
			&Stmt{
				nodes: []statementNode{
					{text, "some ", 0},
					{placeholder, "place1", 1},
					{placeholder, "", 2},
					{placeholder, "place2", 3},
					{text, " sql", 0},
				},
				numOrdinalPlaceholders: 1,
				namedPlaceholderNames:  []string{"place1", "place2"},
			},
			"",
		},
		{
			"three named and ordinal placeholders back to back (degenerate), 1",
			"some ?@place? sql",
			&Stmt{
				nodes: []statementNode{
					{text, "some ", 0},
					{placeholder, "", 1},
					{placeholder, "place", 2},
					{placeholder, "", 3},
					{text, " sql", 0},
				},
				numOrdinalPlaceholders: 2,
				namedPlaceholderNames:  []string{"place"},
			},
			"",
		},
	}

	numFails := 0

	for _, tcase := range fixture {
		actual, err := ParseSQL(tcase.sql)

		if err != nil && tcase.failure == "" {
			t.Errorf("case '%s' unexpected error %v", tcase.name, err)
			numFails += 1
			continue
		}

		if tcase.failure != "" {
			if err == nil {
				t.Errorf("case '%s' expected error '%s', got %v", tcase.name, tcase.failure, actual)
				numFails += 1
				continue
			}

			if err.Error() != tcase.failure {
				t.Errorf("case '%s' expected error %s but got %v", tcase.name, tcase.failure, err)
				numFails += 1
				continue
			}
		}

		if !EqualStatements(tcase.expected, actual) {
			t.Errorf("case '%s' expected %v but got %v", tcase.name, tcase.expected, actual)
			numFails += 1
			continue
		}
	}

	if numFails > 0 {
		t.Errorf("%d test cases, %d failed", len(fixture), numFails)
	}
}

func EqualStatements(expected *Stmt, actual *Stmt) bool {
	return (expected == nil && actual == nil) ||
		(expected != nil && expected.Equal(actual))
}
