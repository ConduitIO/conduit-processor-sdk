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
	"testing"

	"github.com/matryer/is"
)

func mkItem(typ itemType, text string) item {
	return item{
		typ: typ,
		val: text,
	}
}

func assertEqual(t *testing.T, i1, i2 []item, checkPos bool) bool {
	is := is.New(t)
	is.Helper()
	is.Equal(len(i1), len(i2))

	for k := range i1 {
		is.Equal(i1[k].typ, i2[k].typ)
		is.Equal(i1[k].val, i2[k].val)
		if checkPos {
			is.Equal(i1[k].pos, i2[k].pos)
		}
	}
	return true
}

func TestLex(t *testing.T) {
	var testCases = []struct {
		name  string
		input string
		items []item
	}{
		{"empty", "", []item{
			mkItem(itemEOF, ""),
		}},
		{"dot", ".", []item{
			mkItem(itemDot, "."),
			mkItem(itemEOF, ""),
		}},
		{"fields", ".x.y.z", []item{
			mkItem(itemField, ".x"),
			mkItem(itemField, ".y"),
			mkItem(itemField, ".z"),
			mkItem(itemEOF, ""),
		}},
		{"variable", "$.x", []item{
			mkItem(itemVariable, "$"),
			mkItem(itemField, ".x"),
			mkItem(itemEOF, ""),
		}},
		{"map index", `.x["my string"]`, []item{
			mkItem(itemField, ".x"),
			mkItem(itemLeftBracket, "["),
			mkItem(itemString, `"my string"`),
			mkItem(itemRightBracket, "]"),
			mkItem(itemEOF, ""),
		}},
		{"named variable", "$x", []item{
			mkItem(itemError, "bad character U+0078 'x'"),
		}},
		{"space", " ", []item{
			mkItem(itemError, "unrecognized character in reference: U+0020 ' '"),
		}},
		{"filed starting with number", ".0abc", []item{
			mkItem(itemError, "unrecognized character at the start of a field name: U+0030 '0'"),
		}},
		{"parentheses", "(.x)", []item{
			mkItem(itemError, "unrecognized character in reference: U+0028 '('"),
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := newLexer(tc.input)
			var items []item
			for {
				item := l.Next()
				items = append(items, item)
				if item.typ == itemEOF || item.typ == itemError {
					break
				}
			}
			assertEqual(t, items, tc.items, false)
		})
	}
}
