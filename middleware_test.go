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
	procschema "github.com/conduitio/conduit-processor-sdk/schema"
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

func TestProcessorWithSchemaDecode_Write(t *testing.T) {
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
	sch, err := procschema.Create(ctx, schema.TypeAvro, "TestProcessorWithSchemaDecode_Write", []byte(srd.String()))
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
			Metadata: map[string]string{
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
		PayloadSubject: lang.Ptr("foo"),
		KeySubject:     lang.Ptr("bar"),
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
	is.Equal(len(got.Parameters), 6) // expected middleware to inject 5 parameters
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
		wantSchemaType     schema.Type
		wantPayloadSubject string
		wantKeySubject     string
	}{{
		name:       "empty config",
		middleware: ProcessorWithSchemaEncode{},
		have:       config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "payload",
		wantKeySubject:     "key",
	}, {
		name:       "invalid schema type",
		middleware: ProcessorWithSchemaEncode{},
		have: config.Config{
			configProcessorSchemaEncodeType: "foo",
		},
		wantErr: schema.ErrUnsupportedType,
	}, {
		name: "disabled by default",
		middleware: ProcessorWithSchemaEncode{
			Config: ProcessorWithSchemaEncodeConfig{
				PayloadEnabled: lang.Ptr(false),
				KeyEnabled:     lang.Ptr(false),
			},
		},
		have: config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "",
		wantKeySubject:     "",
	}, {
		name:       "disabled by config",
		middleware: ProcessorWithSchemaEncode{},
		have: config.Config{
			configProcessorSchemaEncodePayloadEnabled: "false",
			configProcessorSchemaEncodeKeyEnabled:     "false",
		},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "",
		wantKeySubject:     "",
	}, {
		name: "static default payload subject",
		middleware: ProcessorWithSchemaEncode{
			Config: ProcessorWithSchemaEncodeConfig{
				PayloadSubject: lang.Ptr("foo"),
				KeySubject:     lang.Ptr("bar"),
			},
		},
		have: config.Config{},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "foo",
		wantKeySubject:     "bar",
	}, {
		name:       "payload subject by config",
		middleware: ProcessorWithSchemaEncode{},
		have: config.Config{
			configProcessorSchemaEncodePayloadSubject: "foo",
			configProcessorSchemaEncodeKeySubject:     "bar",
		},

		wantSchemaType:     schema.TypeAvro,
		wantPayloadSubject: "foo",
		wantKeySubject:     "bar",
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

			is.Equal(s.schemaType, tt.wantSchemaType)
			is.Equal(s.payloadSubject, tt.wantPayloadSubject)
			is.Equal(s.keySubject, tt.wantKeySubject)
		})
	}
}

func TestProcessorWithSchemaEncode_Read(t *testing.T) {
	is := is.New(t)
	ctrl := gomock.NewController(t)
	proc := NewMockProcessor(ctrl)
	ctx := context.Background()

	s := (&ProcessorWithSchemaEncode{}).Wrap(proc)

	proc.EXPECT().Configure(ctx, gomock.Any()).Return(nil)
	err := s.Configure(ctx, config.Config{})
	is.NoErr(err)

	testStructuredData := opencdc.StructuredData{
		"foo":   "bar",
		"long":  int64(1),
		"float": 2.34,
		"time":  time.Now().UTC().Truncate(time.Microsecond), // avro precision is microseconds
	}
	wantSchema := `{"name":"record","type":"record","fields":[{"name":"float","type":"double"},{"name":"foo","type":"string"},{"name":"long","type":"long"},{"name":"time","type":{"type":"long","logicalType":"timestamp-micros"}}]}`

	customTestSchema, err := procschema.Create(ctx, schema.TypeAvro, "custom-test-schema", []byte(wantSchema))
	is.NoErr(err)

	testCases := []struct {
		name               string
		record             opencdc.Record
		wantKeySubject     string
		wantPayloadSubject string
	}{{
		name: "no key, no payload",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw key",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured key",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: nil,
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw payload before",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured payload before",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "raw payload after",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "structured payload after",
		record: opencdc.Record{
			Key: nil,
			Payload: opencdc.Change{
				Before: nil,
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all structured",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all raw",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  opencdc.RawData("this should not be encoded"),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "key raw payload structured",
		record: opencdc.Record{
			Key: opencdc.RawData("this should not be encoded"),
			Payload: opencdc.Change{
				Before: nil,
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "key structured payload raw",
		record: opencdc.Record{
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: opencdc.RawData("this should not be encoded"),
				After:  nil,
			},
		},
		wantKeySubject:     "key",
		wantPayloadSubject: "payload",
	}, {
		name: "all structured with collection",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection: "foo",
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     "foo.key",
		wantPayloadSubject: "foo.payload",
	}, {
		name: "all structured with collection and predefined schema",
		record: opencdc.Record{
			Metadata: map[string]string{
				opencdc.MetadataCollection:           "foo",
				opencdc.MetadataKeySchemaSubject:     customTestSchema.Subject,
				opencdc.MetadataKeySchemaVersion:     strconv.Itoa(customTestSchema.Version),
				opencdc.MetadataPayloadSchemaSubject: customTestSchema.Subject,
				opencdc.MetadataPayloadSchemaVersion: strconv.Itoa(customTestSchema.Version),
			},
			Key: testStructuredData.Clone(),
			Payload: opencdc.Change{
				Before: testStructuredData.Clone(),
				After:  testStructuredData.Clone(),
			},
		},
		wantKeySubject:     customTestSchema.Subject,
		wantPayloadSubject: customTestSchema.Subject,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc.EXPECT().Process(ctx, []opencdc.Record{tc.record}).Return([]ProcessedRecord{SingleRecord(tc.record)})

			var wantKey, wantPayloadBefore, wantPayloadAfter opencdc.Data
			if tc.record.Key != nil {
				wantKey = tc.record.Key.Clone()
			}
			if tc.record.Payload.Before != nil {
				wantPayloadBefore = tc.record.Payload.Before.Clone()
			}
			if tc.record.Payload.After != nil {
				wantPayloadAfter = tc.record.Payload.After.Clone()
			}

			gotRecs := s.Process(ctx, []opencdc.Record{tc.record})
			is.Equal(len(gotRecs), 1)

			got, ok := gotRecs[0].(SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %+v", gotRecs[0])
			}

			gotKey := got.Key
			gotPayloadBefore := got.Payload.Before
			gotPayloadAfter := got.Payload.After

			if _, ok := wantKey.(opencdc.StructuredData); ok {
				subject, err := got.Metadata.GetKeySchemaSubject()
				is.NoErr(err)
				version, err := got.Metadata.GetKeySchemaVersion()
				is.NoErr(err)

				is.Equal(subject, tc.wantKeySubject)

				sch, err := procschema.Get(ctx, subject, version)
				is.NoErr(err)

				is.Equal("", cmp.Diff(wantSchema, string(sch.Bytes)))

				var sd opencdc.StructuredData
				err = sch.Unmarshal(gotKey.Bytes(), &sd)
				is.NoErr(err)

				gotKey = sd
			} else {
				_, err := got.Metadata.GetKeySchemaSubject()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
				_, err = got.Metadata.GetKeySchemaVersion()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
			}

			_, isPayloadBeforeStructured := wantPayloadBefore.(opencdc.StructuredData)
			_, isPayloadAfterStructured := wantPayloadAfter.(opencdc.StructuredData)
			if isPayloadBeforeStructured || isPayloadAfterStructured {
				subject, err := got.Metadata.GetPayloadSchemaSubject()
				is.NoErr(err)
				version, err := got.Metadata.GetPayloadSchemaVersion()
				is.NoErr(err)

				is.Equal(subject, tc.wantPayloadSubject)

				sch, err := procschema.Get(ctx, subject, version)
				is.NoErr(err)

				is.Equal("", cmp.Diff(wantSchema, string(sch.Bytes)))

				if isPayloadBeforeStructured {
					var sd opencdc.StructuredData
					err = sch.Unmarshal(gotPayloadBefore.Bytes(), &sd)
					is.NoErr(err)
					gotPayloadBefore = sd
				}
				if isPayloadAfterStructured {
					var sd opencdc.StructuredData
					err = sch.Unmarshal(gotPayloadAfter.Bytes(), &sd)
					is.NoErr(err)
					gotPayloadAfter = sd
				}
			} else {
				_, err := got.Metadata.GetPayloadSchemaSubject()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
				_, err = got.Metadata.GetPayloadSchemaVersion()
				is.True(errors.Is(err, opencdc.ErrMetadataFieldNotFound))
			}

			is.Equal("", cmp.Diff(gotKey, wantKey))
			is.Equal("", cmp.Diff(gotPayloadBefore, wantPayloadBefore))
			is.Equal("", cmp.Diff(gotPayloadAfter, wantPayloadAfter))
		})
	}
}
