package schemaregistry

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewClient_with_an_invalid_baseurl(t *testing.T) {
	client, err := NewClient("%gh&%ij")

	assert.EqualError(t, err, `parse %gh&%ij: invalid URL escape "%gh"`)
	assert.Nil(t, client)
}

func Test_NewClient_with_a_custom_client(t *testing.T) {
	// Add a custom timeout
	customClient := &http.Client{Timeout: time.Hour}

	client, err := NewClient("some-url", UsingClient(customClient))

	assert.NoError(t, err)
	assert.NotNil(t, client)
	// The client should have the client with the timeout.
	assert.EqualValues(t, customClient, client.client)
}

func Test_GetSchemaByID_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/schemas/ids/42", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{ "schema": "{\"type\": \"string\"}" }`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetSchemaByID(context.Background(), 42)

	assert.NoError(t, err)
	assert.Equal(t, `{"type": "string"}`, schema)
}

func Test_GetSchemaByID_with_a_network_error(t *testing.T) {
	client, err := NewClient("http://unreachable-url")
	require.NoError(t, err)

	schema, err := client.GetSchemaByID(context.Background(), 42)

	assert.Empty(t, schema)
	assert.EqualError(t, err, "Get http://unreachable-url/schemas/ids/42: dial tcp: lookup unreachable-url: no such host")
}

func Test_GetSchemaByID_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(`{
			"error_code": 404,
			"message": "schema not found"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetSchemaByID(context.Background(), 42)

	assert.Empty(t, schema)
	assert.EqualError(t, err, fmt.Sprintf("client: (GET: %s/schemas/ids/42) failed with error code 404: schema not found", ts.URL))
}

func Test_GetSchemaByID_with_an_invalid_json_as_response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetSchemaByID(context.Background(), 42)

	assert.Empty(t, schema)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}