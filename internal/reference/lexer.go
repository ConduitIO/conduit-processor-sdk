// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reference

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

// lexer is a subset of the text/template lexer. It does not expect to see any
// template action delimiters (e.g. {{ or }}), only field references. It also
// does not support the "pipeline" syntax (e.g. .X.Y | func) or any of the
// other template features.
// However, it additionally supports accessing map values using the standard
// bracket notation (e.g. .X["Y"]).
type lexer struct {
	input string
	pos   int  // current position in the input
	start int  // start position of this item
	item  item // item to return to parser
	atEOF bool // we have hit the end of input and returned eof
}

type item struct {
	typ itemType
	pos int
	val string
}

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q", i.val)
}

type itemType int

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

const (
	itemError itemType = iota
	itemEOF
	itemDot
	itemField
	itemVariable // variable starting with '$', such as '$' or  '$1' or '$hello'
	itemString
	itemLeftBracket
	itemRightBracket
)

const eof = -1

func newLexer(input string) *lexer {
	return &lexer{
		input: input,
	}
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.atEOF = true
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.pos += w
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune.
func (l *lexer) backup() {
	if !l.atEOF && l.pos > 0 {
		_, w := utf8.DecodeLastRuneInString(l.input[:l.pos])
		l.pos -= w
	}
}

// thisItem returns the item at the current input point with the specified type
// and advances the input.
func (l *lexer) thisItem(t itemType) item {
	i := item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
	return i
}

// emit passes the trailing text as an item back to the parser.
func (l *lexer) emit(t itemType) stateFn {
	return l.emitItem(l.thisItem(t))
}

// emitItem passes the specified item to the parser.
func (l *lexer) emitItem(i item) stateFn {
	l.item = i
	return nil
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.nextItem.
func (l *lexer) errorf(format string, args ...any) stateFn {
	l.item = item{itemError, l.start, fmt.Sprintf(format, args...)}
	l.start = 0
	l.pos = 0
	l.input = l.input[:0]
	return nil
}

// atTerminator reports whether the input is at valid termination character to
// appear after an identifier. Breaks .X.Y into two pieces.
func (l *lexer) atTerminator() bool {
	r := l.peek()
	if l.isSpace(r) {
		return true
	}
	switch r {
	case eof, '.', '[', ']':
		return true
	}
	return false
}

// isSpace reports whether r is a space character.
func (*lexer) isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func (*lexer) isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

func (l *lexer) Next() item {
	l.item = item{itemEOF, l.pos, "EOF"}
	state := (*lexer).lexReference
	for {
		state = state(l)
		if state == nil {
			return l.item
		}
	}
}

// lexReference scans a reference: .Alphanumeric or $.Alphanumeric.
func (l *lexer) lexReference() stateFn {
	switch r := l.next(); r {
	case eof:
		return l.emit(itemEOF)
	case '$':
		return (*lexer).lexVariable
	case '.':
		// special look-ahead for ".field" so we don't break l.backup().
		if l.pos < len(l.input) {
			r := l.input[l.pos]
			if r >= '0' && r <= '9' {
				return l.errorf("unrecognized character at the start of a field name: %#U", r)
			}
		}
		return (*lexer).lexField
	case '[':
		return l.emit(itemLeftBracket)
	case ']':
		return l.emit(itemRightBracket)
	case '"':
		return (*lexer).lexQuote
	default:
		return l.errorf("unrecognized character in reference: %#U", r)
	}
}

// lexVariable scans a Variable: $Alphanumeric.
// The $ has been scanned.
func (l *lexer) lexVariable() stateFn {
	if l.atTerminator() { // Nothing interesting follows -> "$".
		return l.emit(itemVariable)
	}
	// We do not allow named variables, you can just access fields inside the
	// root variable.
	return l.errorf("bad character %#U", l.peek())
}

// lexField scans a field: .Alphanumeric.
// The . has been scanned.
func (l *lexer) lexField() stateFn {
	if l.atTerminator() { // Nothing interesting follows -> ".".
		return l.emit(itemDot)
	}
	var r rune
	for {
		r = l.next()
		if !l.isAlphaNumeric(r) {
			l.backup()
			break
		}
	}
	if !l.atTerminator() {
		return l.errorf("bad character %#U", r)
	}
	return l.emit(itemField)
}

// lexQuote scans a quoted string.
func (l *lexer) lexQuote() stateFn {
Loop:
	for {
		switch l.next() {
		case '\\':
			if r := l.next(); r != eof && r != '\n' {
				break
			}
			fallthrough
		case eof, '\n':
			return l.errorf("unterminated quoted string")
		case '"':
			break Loop
		}
	}
	return l.emit(itemString)
}
