package prepared

import (
	"fmt"
	"strings"
)

//
// The Stmt and its associated types
//

type Stmt struct {
	nodes                  []statementNode
	numOrdinalPlaceholders int
	namedPlaceholderNames  []string
}

type statementNode struct {
	Type    nodeType
	Text    string
	Ordinal int
}

type nodeType int8

const (
	text nodeType = iota
	placeholder
)

//
// Utility functions
//

func (n statementNode) String() string {
	switch n.Type {
	case text:
		return fmt.Sprintf("text(%s)", n.Text)
	default: // placeholder
		return fmt.Sprintf("placeholder(%d,%v)", n.Ordinal, n.Text)
	}
}

func (n Stmt) String() string {
	s := strings.Builder{}
	s.WriteString("(")

	if n.numOrdinalPlaceholders > 0 {
		s.WriteString(fmt.Sprintf("\n  ?=%d", n.numOrdinalPlaceholders))
	}

	if len(n.namedPlaceholderNames) > 0 {
		s.WriteString(fmt.Sprintf("\n  @=%v", n.namedPlaceholderNames))
	}

	for _, node := range n.nodes {
		s.WriteString("\n  ")
		s.WriteString(node.String())
	}

	s.WriteString("\n)")
	return s.String()
}

func (n statementNode) Equal(other *statementNode) bool {
	return other != nil && n.Type == other.Type && n.Text == other.Text && n.Ordinal == other.Ordinal
}

func (n Stmt) Equal(other *Stmt) bool {
	if other == nil ||
		n.numOrdinalPlaceholders != other.numOrdinalPlaceholders ||
		len(n.nodes) != len(other.nodes) ||
		len(n.namedPlaceholderNames) != len(other.namedPlaceholderNames) {
		return false
	}

	for i, each := range n.namedPlaceholderNames {
		if each != other.namedPlaceholderNames[i] {
			return false
		}
	}

	for i, each := range n.nodes {
		if !each.Equal(&other.nodes[i]) {
			return false
		}
	}

	return true
}
