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

package sdk

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/lang"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-commons/schema/avro"
	sdkschema "github.com/conduitio/conduit-processor-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// -- ProcessorWithSchemaDecode ------------------------------------------

func TestProcessorWithSchemaDecodeConfig_Apply(t *testing.T) {
	is := is.New(t)

	wantCfg := ProcessorWithSchemaDecodeConfig{
		PayloadEnabled: lang.Ptr(true),
		KeyEnabled:     lang.Ptr(true),
	}

	have := &ProcessorWithSchemaDecode{}
	wantCfg.Apply(have)

	is.Equal(have.Config, wantCfg)
}

func TestProcessorWithSchemaDecode_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)

	s := (&ProcessorWithSchemaDecode{}).Wrap(proc)

	want := Specification{
		Name: "ProcessorWithSchemaDecode",
		Parameters: config.Parameters{
			"foo": {
				Default:     "bar",
				Description: "baz",
			},
		},
	}

	proc.EXPECT().Specification().Return(want, nil)

	got, err := s.Specification()
	is.NoErr(err)
	is.Equal(got.Parameters["foo"], want.Parameters["foo"])
	is.Equal(len(got.Parameters), 3) // expected middleware to inject 2 parameters
}

func TestProcessorWithSchemaDecode_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	ctx := context.Background()

	testCases := []struct {
		name       string
		middleware ProcessorWithSchemaDecode
		have       config.Config

		wantErr            error
		wantPayloadEnabled bool
		wantKeyEnabled     bool
	}{{
		name:       "empty config",
		middleware: ProcessorWithSchemaDecode{},
		have:       config.Config{},

		wantPayloadEnabled: true,
		wantKeyEnabled:     true,
	}, {
		name: "disabled by default",
		middleware: ProcessorWithSchemaDecode{
			Config: ProcessorWithSchemaDecodeConfig{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
		},
		have: config.Config{},

		wantPayloadEnabled: false,
		wantKeyEnabled:     false,
	}, {
		name:       "disabled by config",
		middleware: ProcessorWithSchemaDecode{},
		have: config.Config{
			configProcessorWithSchemaDecodePayloadEnabled: "false",
			configProcessorWithSchemaDecodeKeyEnabled:     "false",
		},

		wantPayloadEnabled: false,
		wantKeyEnabled:     false,
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			s := tt.middleware.Wrap(proc).(*processorWithSchemaDecode)

			proc.EXPECT().Configure(ctx, tt.have).Return(nil)

			err := s.Configure(ctx, tt.have)
			if tt.wantErr != nil {
				is.True(errors.Is(err, tt.wantErr))
				return
			}

			is.NoErr(err)

			is.Equal(s.payloadEnabled, tt.wantPayloadEnabled)
			is.Equal(s.keyEnabled, tt.wantKeyEnabled)
		})
	}
}

func TestProcessorWithSchemaDecode_Process(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	ctx := context.Background()

	d := (&ProcessorWithSchemaDecode{}).Wrap(proc)

	proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	err := d.Configure(ctx, config.Config{})
	is.NoErr(err)

	testStructuredData := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}

	srd, err := avro.SerdeForType(testStructuredData)
	is.NoErr(err)
	sch, err := sdkschema.Create(ctx, schema.TypeAvro, "TestProcessorWithSchemaDecode_Process", []byte(srd.String()))
	is.NoErr(err)

	b, err := sch.Marshal(testStructuredData)
	is.NoErr(err)
	testRawData := opencdc.RawData(b)

	testCases := []struct {
		name   string
		record opencdc.Record
	}{{
		name: "no metadata, no key, no payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "metadata attached, structured key",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject: sch.Subject,
				opencdc.MetadataKeySchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "metadata attached, raw key",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject: sch.Subject,
				opencdc.MetadataKeySchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: testRawData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "no metadata, structured key",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be decoded"),
				After:  nil,
			},
		},
	}, {
		name: "no metadata, raw key",
		record: opencdc.Record{
			Key: testRawData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  opencdc.RawData("this should not be decoded"),
			},
		},
	}, {
		name: "metadata attached, structured payload",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: opencdc.RawData("this should not be decoded"),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
	}, {
		name: "metadata attached, raw payload (both)",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  testRawData.Clone(),
			},
		},
	}, {
		name: "metadata attached, raw payload.before, structured payload.after",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
	}, {
		name: "metadata attached, structured payload.before, raw payload.after",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testRawData.Clone(),
			},
		},
	}, {
		name: "metadata attached, raw payload.before, no payload.after",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  nil,
			},
		},
	}, {
		name: "metadata attached, no payload.before, raw payload.after",
		record: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: sch.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(sch.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  testRawData.Clone(),
			},
		},
	}, {
		name: "no metadata, structured payload",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be decoded"),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
	}, {
		name: "no metadata, raw payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: testRawData.Clone(),
				After:  testRawData.Clone(),
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			subject, _ := tc.record.Metadata.GetKeySchemaSubject()
			version, _ := tc.record.Metadata.GetKeySchemaVersion()
			wantDecodedKey := tc.record.Key != nil &&
				bytes.Equal(tc.record.Key.Bytes(), testRawData.Bytes()) &&
				subject != "" && version != 0

			subject, _ = tc.record.Metadata.GetPayloadSchemaSubject()
			version, _ = tc.record.Metadata.GetPayloadSchemaVersion()
			wantDecodedPayloadBefore := tc.record.Payload.Before != nil &&
				bytes.Equal(tc.record.Payload.Before.Bytes(), testRawData.Bytes()) &&
				subject != "" && version != 0
			wantDecodedPayloadAfter := tc.record.Payload.After != nil &&
				bytes.Equal(tc.record.Payload.After.Bytes(), testRawData.Bytes()) &&
				subject != "" && version != 0

			wantRecord := tc.record.Clone()
			if wantDecodedKey {
				t.Logf("expect decoded key")
				wantRecord.Key = testStructuredData
			}
			if wantDecodedPayloadBefore {
				t.Logf("expect decoded payload.before")
				wantRecord.Payload.Before = testStructuredData
			}
			if wantDecodedPayloadAfter {
				t.Logf("expect decoded payload.after")
				wantRecord.Payload.After = testStructuredData
			}

			proc.EXPECT().Process(ctx, []opencdc.Record{wantRecord}).Return([]ProcessedRecord{SingleRecord(wantRecord)})

			_ = d.Process(ctx, []opencdc.Record{tc.record})
		})
	}
}

// -- ProcessorWithSchemaEncode -----------------------------------------------

func TestProcessorWithSchemaEncodeConfig_Apply(t *testing.T) {
	is := is.New(t)

	wantCfg := ProcessorWithSchemaEncodeConfig{
		PayloadEnabled: lang.Ptr(true),
		KeyEnabled:     lang.Ptr(true),
	}

	have := &ProcessorWithSchemaEncode{}
	wantCfg.Apply(have)

	is.Equal(have.Config, wantCfg)
}

func TestProcessorWithSchemaEncode_Parameters(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)

	s := (&ProcessorWithSchemaEncode{}).Wrap(proc)

	want := Specification{
		Name: "ProcessorWithSchemaEncode",
		Parameters: config.Parameters{
			"foo": {
				Default:     "bar",
				Description: "baz",
			},
		},
	}

	proc.EXPECT().Specification().Return(want, nil)

	got, err := s.Specification()
	is.NoErr(err)
	is.Equal(got.Parameters["foo"], want.Parameters["foo"])
	is.Equal(len(got.Parameters), 3) // expected middleware to inject 2 parameters
}

func TestProcessorWithSchemaEncode_Configure(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	ctx := context.Background()

	testCases := []struct {
		name       string
		middleware ProcessorWithSchemaEncode
		have       config.Config

		wantErr            error
		wantPayloadEnabled bool
		wantKeyEnabled     bool
	}{{
		name:       "empty config",
		middleware: ProcessorWithSchemaEncode{},
		have:       config.Config{},

		wantPayloadEnabled: true,
		wantKeyEnabled:     true,
	}, {
		name: "disabled by default",
		middleware: ProcessorWithSchemaEncode{
			Config: ProcessorWithSchemaEncodeConfig{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
		},
		have: config.Config{},

		wantPayloadEnabled: false,
		wantKeyEnabled:     false,
	}, {
		name:       "disabled by config",
		middleware: ProcessorWithSchemaEncode{},
		have: config.Config{
			configProcessorSchemaEncodePayloadEnabled: "false",
			configProcessorSchemaEncodeKeyEnabled:     "false",
		},

		wantPayloadEnabled: false,
		wantKeyEnabled:     false,
	}}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			s := tt.middleware.Wrap(proc).(*processorWithSchemaEncode)

			proc.EXPECT().Configure(ctx, tt.have).Return(nil)

			err := s.Configure(ctx, tt.have)
			if tt.wantErr != nil {
				is.True(errors.Is(err, tt.wantErr))
				return
			}

			is.NoErr(err)

			is.Equal(s.payloadEnabled, tt.wantPayloadEnabled)
			is.Equal(s.keyEnabled, tt.wantKeyEnabled)
		})
	}
}

func TestProcessorWithSchemaEncode_Process(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	ctx := context.Background()

	s := (&ProcessorWithSchemaEncode{}).Wrap(proc)

	proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	err := s.Configure(ctx, config.Config{})
	is.NoErr(err)

	prepareTestData := func(data opencdc.StructuredData, avroSchema string) (schema.Schema, opencdc.RawData) {
		sch, err := sdkschema.Create(ctx, schema.TypeAvro, "custom-test-schema", []byte(avroSchema))
		is.NoErr(err)

		encoded, err := sch.Marshal(data)
		is.NoErr(err)

		return sch, encoded
	}

	structuredData1 := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}
	avroSchema1 := `{"name":"record","type":"record","fields":[{"name":"float","type":"double"},{"name":"foo","type":"string"},{"name":"long","type":"long"},{"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}}]}`
	schema1, rawData1 := prepareTestData(structuredData1, avroSchema1)

	structuredData2 := opencdc.StructuredData{
		"foo":   true,
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}
	avroSchema2 := `{"name":"record","type":"record","fields":[{"name":"float","type":"double"},{"name":"foo","type":"boolean"},{"name":"long","type":"long"},{"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}}]}`
	schema2, rawData2 := prepareTestData(structuredData2, avroSchema2)

	testCases := []struct {
		name       string
		inRecord   opencdc.Record // record to process
		outRecord  SingleRecord   // record returned by processor (if empty, inRecord is used)
		wantRecord SingleRecord
	}{{
		name: "no key, no payload, no metadata",
		inRecord: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantRecord: SingleRecord{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "raw key, raw payload, no metadata",
		inRecord: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantRecord: SingleRecord{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
	}, {
		name: "raw key, raw payload, with metadata",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema1.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema1.Version),
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema1.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema1.Version),
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
	}, {
		name: "structured key, structured payload, no metadata",
		inRecord: opencdc.Record{
			Key: structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
		wantRecord: SingleRecord{
			Key: structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
	}, {
		name: "structured key",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject: schema1.Subject,
				opencdc.MetadataKeySchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject: schema1.Subject,
				opencdc.MetadataKeySchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: rawData1.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
	}, {
		name: "structured payload before",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  nil,
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: rawData1.Clone(),
				After:  nil,
			},
		},
	}, {
		name: "structured payload after",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  structuredData1.Clone(),
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  rawData1.Clone(),
			},
		},
	}, {
		name: "all structured",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema1.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema1.Version),
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema1.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema1.Version),
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: rawData1.Clone(),
			Payload: opencdc.Change{
				Before: rawData1.Clone(),
				After:  rawData1.Clone(),
			},
		},
	}, {
		name: "metadata removed",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema1.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema1.Version),
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
		outRecord: SingleRecord{
			Metadata: opencdc.Metadata{},
			Key:      structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{},
			Key:      structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
	}, {
		name: "metadata changed",
		inRecord: opencdc.Record{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema1.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema1.Version),
				opencdc.MetadataPayloadSchemaSubject: schema1.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema1.Version),
			},
			Key: structuredData1.Clone(),
			Payload: opencdc.Change{
				Before: structuredData1.Clone(),
				After:  structuredData1.Clone(),
			},
		},
		outRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema2.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema2.Version),
				opencdc.MetadataPayloadSchemaSubject: schema2.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema2.Version),
			},
			Key: structuredData2.Clone(),
			Payload: opencdc.Change{
				Before: structuredData2.Clone(),
				After:  structuredData2.Clone(),
			},
		},
		wantRecord: SingleRecord{
			Metadata: opencdc.Metadata{
				opencdc.MetadataKeySchemaSubject:     schema2.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(schema2.Version),
				opencdc.MetadataPayloadSchemaSubject: schema2.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(schema2.Version),
			},
			Key: rawData2.Clone(),
			Payload: opencdc.Change{
				Before: rawData2.Clone(),
				After:  rawData2.Clone(),
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			outRecord := tc.outRecord
			if outRecord.Key == nil &&
				tc.outRecord.Payload == (opencdc.Change{}) &&
				tc.outRecord.Metadata == nil &&
				tc.outRecord.Position == nil &&
				tc.outRecord.Operation == 0 {
				// if tc.outRecord is empty use tc.inRecord (easier to write tests)
				outRecord = SingleRecord(tc.inRecord)
			}

			proc.EXPECT().Process(ctx, []opencdc.Record{tc.inRecord}).Return([]ProcessedRecord{outRecord})

			gotRecs := s.Process(ctx, []opencdc.Record{tc.inRecord})
			is.Equal(len(gotRecs), 1)

			got, ok := gotRecs[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %+v", gotRecs[0])
			}

			is.Equal("", cmp.Diff(tc.wantRecord, got, cmp.AllowUnexported(SingleRecord{})))
		})
	}
}
