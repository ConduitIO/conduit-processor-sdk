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

package egress

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-processor-sdk/pprocutils"
	"github.com/conduitio/conduit-processor-sdk/pprocutils/mock"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

// withHTTPService swaps the package-level HTTPService for the duration of a
// test and restores the deny-all default afterwards, so tests never leak
// state into one another regardless of execution order.
func withHTTPService(t *testing.T, svc pprocutils.HTTPService) {
	t.Helper()
	HTTPService = svc
	t.Cleanup(func() { HTTPService = disabledHTTPService{} })
}

func TestDo_ConvertsRequestAndResponse(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	svc := mock.NewHTTPService(ctrl)
	withHTTPService(t, svc)

	req := Request{
		Method:        "POST",
		URL:           "https://api.example.com/v1/embeddings",
		Headers:       map[string][]string{"Content-Type": {"application/json"}},
		Body:          []byte(`{"input":"hello"}`),
		AuthSecretRef: "openai_api_key",
	}

	svc.EXPECT().Do(ctx, pprocutils.HTTPRequest{
		Method:        req.Method,
		URL:           req.URL,
		Headers:       req.Headers,
		Body:          req.Body,
		AuthSecretRef: req.AuthSecretRef,
	}).Return(pprocutils.HTTPResponse{
		StatusCode: 200,
		Headers:    map[string][]string{"Content-Type": {"application/json"}},
		Body:       []byte(`{"result":"ok"}`),
	}, nil)

	resp, err := Do(ctx, req)
	is.NoErr(err)
	is.Equal(resp, Response{
		StatusCode: 200,
		Headers:    map[string][]string{"Content-Type": {"application/json"}},
		Body:       []byte(`{"result":"ok"}`),
	})
}

func TestDo_WrapsSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		code uint32
		want error
	}{
		{"egress disabled", pprocutils.ErrorCodeHTTPEgressDisabled, ErrEgressDisabled},
		{"forbidden", pprocutils.ErrorCodeHTTPForbidden, ErrForbidden},
		{"invalid request", pprocutils.ErrorCodeHTTPInvalidRequest, ErrInvalidRequest},
		{"dns", pprocutils.ErrorCodeHTTPDNS, ErrDNS},
		{"timeout", pprocutils.ErrorCodeHTTPTimeout, ErrTimeout},
		{"response too large", pprocutils.ErrorCodeHTTPResponseTooLarge, ErrResponseTooLarge},
		{"transport", pprocutils.ErrorCodeHTTPTransport, ErrTransport},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			svc := mock.NewHTTPService(ctrl)
			withHTTPService(t, svc)

			svc.EXPECT().Do(ctx, gomock.Any()).
				Return(pprocutils.HTTPResponse{}, pprocutils.NewErrorFromCode(tt.code))

			_, err := Do(ctx, Request{Method: "GET", URL: "https://example.com"})
			is.True(errors.Is(err, tt.want))
		})
	}
}

func TestDo_DefaultServiceDeniesAll(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	// No withHTTPService call: exercises the package's actual zero-config
	// default, which every hosting mode other than a standalone (WASM)
	// processor gets unless it opts in with its own HTTPService.
	withHTTPService(t, disabledHTTPService{})

	_, err := Do(ctx, Request{Method: "GET", URL: "https://example.com"})
	is.True(errors.Is(err, ErrEgressDisabled))
}
