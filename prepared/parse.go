package prepared

import (
	"fmt"
)

//
// Parsing a SQL string into a Stmt
//

type parseState int8

const (
	parsingText parseState = iota
	parsingStringConstant
	endStringConstantOrEscapeSingleQuote
	parsingQuotedIdentifier
	endQuotedIdentifierOfEscapeDoubleQuote
	parsingNamedPlaceholder
)

type parseError struct {
	Msg string
}

func (err parseError) Error() string {
	return err.Msg
}

func ParseSQL(sql string) (*Stmt, error) {
	nodes := make([]statementNode, 0)

	state := parsingText
	start := 0

	for pos, char := range sql {
		switch state {

		case parsingText:
			state = processStateParsingText(&start, pos, char, sql, &nodes)

		case parsingNamedPlaceholder:
			if !isPlaceholderName(char) {
				plen := pos - start

				if plen > 0 {
					nodes = append(nodes, statementNode{Type: placeholder, Text: sql[start:pos]})
				} else {
					return nil, parseError{Msg: "empty named placeholder"}
				}

				// the named placeholder ended, we're back at parsing text
				start = pos
				state = processStateParsingText(&start, pos, char, sql, &nodes)
			}

		case parsingStringConstant:
			if char == '\'' {
				state = endStringConstantOrEscapeSingleQuote
			}

		case endStringConstantOrEscapeSingleQuote:
			if char == '\'' {
				// it was an escaped single-quote
				state = parsingStringConstant
			} else {
				// the string constant ended, we're back at parsing text
				state = processStateParsingText(&start, pos, char, sql, &nodes)
			}

		case parsingQuotedIdentifier:
			if char == '"' {
				state = endQuotedIdentifierOfEscapeDoubleQuote
			}

		case endQuotedIdentifierOfEscapeDoubleQuote:
			if char == '"' {
				// it was an escaped double-quote
				state = parsingQuotedIdentifier
			} else {
				// the quoted identifier ended, we're back at parsing text
				state = processStateParsingText(&start, pos, char, sql, &nodes)
			}

		default:
			panic("unknown state")
		}

	}

	var concludingNodeType = text

	switch state {

	case parsingText:
		concludingNodeType = text
		break

	case parsingNamedPlaceholder:
		concludingNodeType = placeholder
		break

	case parsingStringConstant:
		return nil, parseError{Msg: "string constant not closed"}

	case endStringConstantOrEscapeSingleQuote:
		concludingNodeType = text
		break

	case parsingQuotedIdentifier:
		return nil, parseError{Msg: "quoted identifier not closed"}

	case endQuotedIdentifierOfEscapeDoubleQuote:
		concludingNodeType = text
		break

	default:
		panic(fmt.Sprintf("conclude in unknown state %v", state))
	}

	left := start < len(sql)

	if !left && concludingNodeType == placeholder {
		return nil, parseError{Msg: "empty named placeholder"}
	}

	if left {
		nodes = append(nodes, statementNode{Type: concludingNodeType, Text: sql[start:]})
	}

	placeholderOrdinal := 1
	numOrdinals := 0
	names := []string{}

	for i := 0; i < len(nodes); i++ {
		each := &nodes[i]

		if each.Type != placeholder {
			continue
		}

		each.Ordinal = placeholderOrdinal
		placeholderOrdinal += 1

		if each.Text == "" {
			numOrdinals += 1
		} else {
			names = append(names, each.Text)
		}
	}

	return &Stmt{
		nodes:                  nodes,
		numOrdinalPlaceholders: numOrdinals,
		namedPlaceholderNames:  names,
	}, nil
}

func isPlaceholderName(c rune) bool {
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

func processStateParsingText(start *int, pos int, char rune, sql string, nodes *[]statementNode) parseState {
	if char == '\'' {
		return parsingStringConstant
	}

	if char == '"' {
		return parsingQuotedIdentifier
	}

	if char == '@' {
		plen := pos - *start

		if plen > 0 {
			*nodes = append(*nodes, statementNode{Type: text, Text: sql[*start:pos]})
		}

		*start = pos + 1
		return parsingNamedPlaceholder
	}

	if char == '?' {
		plen := pos - *start

		if plen > 0 {
			*nodes = append(*nodes, statementNode{Type: text, Text: sql[*start:pos]})
		}

		*nodes = append(*nodes, statementNode{Type: placeholder, Text: ""})
		*start = pos + 1
	}

	return parsingText
}
