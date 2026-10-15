// Copyright © 2026 Meroxa, Inc.
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

package sdk

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/rabin"
	"github.com/conduitio/conduit-commons/schema"
)

// collidingAvroSchemas returns two different, valid Avro record schemas with
// the same 64-bit Rabin fingerprint (schema.Schema.Fingerprint). s1 has fields
// x and y, s2 has fields x and z. The doc string of s2 is chosen so the CRC
// difference cancels out: the CRC is affine, so fp(a) ^ fp(a^d) is linear in d.
func collidingAvroSchemas(t *testing.T) (s1, s2 []byte) {
	t.Helper()
	const docLen = 128
	tmpl := func(doc, field string) []byte {
		return []byte(`{"type":"record","name":"r","doc":"` + doc + `","fields":[{"name":"x","type":"string"},{"name":"` + field + `","type":["null","string"],"default":null}]}`)
	}
	doc := strings.Repeat("a", docLen)
	base1 := tmpl(doc, "y")
	base2 := tmpl(doc, "z")
	docOff := bytes.Index(base2, []byte(doc))
	L := func(delta []byte) uint64 { // linear part of the CRC
		x := make([]byte, len(base2))
		for i := range x {
			x[i] = base2[i] ^ delta[i]
		}
		return rabin.Bytes(base2) ^ rabin.Bytes(x)
	}
	target := rabin.Bytes(base1) ^ rabin.Bytes(base2)
	// basis vectors: flipping bit 1 of doc char i ('a' <-> 'c')
	type vec struct {
		v    uint64
		mask []bool
	}
	var basis [64]*vec
	for i := 0; i < docLen; i++ {
		d := make([]byte, len(base2))
		d[docOff+i] = 0x02
		cur := &vec{v: L(d), mask: make([]bool, docLen)}
		cur.mask[i] = true
		for b := 63; b >= 0 && cur.v != 0; b-- {
			if cur.v>>uint(b)&1 == 0 {
				continue
			}
			if basis[b] == nil {
				basis[b] = cur
				break
			}
			cur.v ^= basis[b].v
			for j := range cur.mask {
				cur.mask[j] = cur.mask[j] != basis[b].mask[j]
			}
		}
	}
	sel := make([]bool, docLen)
	rem := target
	for b := 63; b >= 0; b-- {
		if rem>>uint(b)&1 == 0 {
			continue
		}
		if basis[b] == nil {
			t.Fatalf("no solution")
		}
		rem ^= basis[b].v
		for j := range sel {
			sel[j] = sel[j] != basis[b].mask[j]
		}
	}
	doc2 := []byte(doc)
	for i, s := range sel {
		if s {
			doc2[i] = 'c'
		}
	}
	return base1, tmpl(string(doc2), "z")
}

// TestSchemaCaches_FingerprintCollision pins the cache key of the dropped-field
// guard and the fit pre-check. Two schemas that share a 64-bit fingerprint must
// not share a cache entry; if they did, the guard would check a record against
// the other schema's fields and the encoder would drop a field silently
// (invariant 6). See conduit-processor-sdk#179.
func TestSchemaCaches_FingerprintCollision(t *testing.T) {
	b1, b2 := collidingAvroSchemas(t)
	s1 := schema.Schema{Subject: "s1", Version: 1, Type: schema.TypeAvro, Bytes: b1}
	s2 := schema.Schema{Subject: "s2", Version: 1, Type: schema.TypeAvro, Bytes: b2}
	if bytes.Equal(b1, b2) || s1.Fingerprint() != s2.Fingerprint() {
		t.Fatalf("no collision: %x %x", s1.Fingerprint(), s2.Fingerprint())
	}
	t.Logf("s1=%s\ns2=%s\nfp=%x", b1, b2, s1.Fingerprint())

	data := opencdc.StructuredData{"x": "keep", "y": "LOST"}

	// Control: with a cold cache, s2 is correctly rejected (y is not in s2).
	parsedAvroSchemas = sync.Map{}
	if err := checkNoDroppedFields(s2, data); !errors.Is(err, ErrFieldNotInSchema) {
		t.Fatalf("control: expected ErrFieldNotInSchema, got %v", err)
	}

	// Warm the cache with s1 (some other pipeline/record), then check s2.
	parsedAvroSchemas = sync.Map{}
	if err := checkNoDroppedFields(s1, data); err != nil {
		t.Fatalf("s1 should accept data: %v", err)
	}
	err := checkNoDroppedFields(s2, data)
	if err == nil {
		// Show the consequence: the real encoder drops y without error.
		srd, _ := s2.Serde()
		enc, merr := srd.Marshal(map[string]any(data))
		var out opencdc.StructuredData
		uerr := srd.Unmarshal(enc, &out)
		t.Errorf("dropped-field check passed for s2 using s1's cached walker; encode err=%v decode err=%v; round trip = %v (y silently dropped)", merr, uerr, out)
	}

	// Same collision on the fit pre-check cache.
	recordFields = sync.Map{}
	f1, _ := topLevelFields(s1)
	f2, _ := topLevelFields(s2)
	if _, ok := f1["y"]; !ok {
		t.Errorf("topLevelFields(s1) = %v, want field y", f1)
	}
	if _, ok := f2["z"]; !ok {
		t.Errorf("topLevelFields(s2) returned s1's fields %v", f2)
	}
	if _, ok := f2["y"]; ok {
		t.Errorf("topLevelFields(s2) = %v, must not have field y", f2)
	}
}

// TestSchemaSerde_FingerprintCollision checks the encoder side of the same
// collision: Schema.Serde() must return each schema's own serde. On
// conduit-commons v0.8.0 and v0.8.1 its cache is keyed by the 64-bit
// fingerprint, so s2 got s1's serde and the field z was dropped silently
// (invariant 6). Fixed in conduit-commons v0.8.2, which this module requires.
func TestSchemaSerde_FingerprintCollision(t *testing.T) {
	b1, b2 := collidingAvroSchemas(t)
	s1 := schema.Schema{Subject: "s1", Version: 1, Type: schema.TypeAvro, Bytes: b1}
	s2 := schema.Schema{Subject: "s2", Version: 1, Type: schema.TypeAvro, Bytes: b2}
	if s1.Fingerprint() != s2.Fingerprint() || bytes.Equal(b1, b2) {
		t.Fatal("fixture does not collide")
	}

	srd1, err := s1.Serde()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srd1.Marshal(map[string]any{"x": "keep", "y": "kept"}); err != nil {
		t.Fatal(err)
	}

	srd2, err := s2.Serde()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := srd2.Marshal(map[string]any{"x": "keep", "z": "kept"})
	if err != nil {
		t.Fatal(err)
	}
	var out opencdc.StructuredData
	if err := srd2.Unmarshal(enc, &out); err != nil {
		t.Fatal(err)
	}
	if out["z"] != "kept" {
		t.Errorf("s2 round trip = %v, want z kept (s2.Serde() returned another schema's serde)", out)
	}
}
