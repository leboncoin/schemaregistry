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

	assert.EqualError(t, err, `parse "%gh&%ij": invalid URL escape "%gh"`)
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
		assert.Equal(t, "GET", r.Method)
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
	client, err := NewClient("foobar://unreachable-url")
	require.NoError(t, err)

	schema, err := client.GetSchemaByID(context.Background(), 42)

	assert.Empty(t, schema)
	assert.EqualError(t, err, `Get "foobar://unreachable-url/schemas/ids/42": unsupported protocol scheme "foobar"`)
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

func Test_Subjects_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`["subject1", "subject2"]`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	subjects, err := client.Subjects(context.Background())

	assert.NoError(t, err)
	assert.EqualValues(t, []string{"subject1", "subject2"}, subjects)
}

func Test_Subjects_with_a_network_error(t *testing.T) {
	client, err := NewClient("foobar://unreachable-url")
	require.NoError(t, err)

	schema, err := client.Subjects(context.Background())

	assert.Empty(t, schema)
	assert.EqualError(t, err, `Get "foobar://unreachable-url/subjects": unsupported protocol scheme "foobar"`)
}

func Test_Subjects_with_a_remote_error(t *testing.T) {
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

	schema, err := client.Subjects(context.Background())

	assert.Empty(t, schema)
	assert.EqualError(t, err, fmt.Sprintf("client: (GET: %s/subjects) failed with error code 404: schema not found", ts.URL))
}

func Test_Subjects_with_an_invalid_json_as_response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.Subjects(context.Background())

	assert.Empty(t, schema)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_Versions_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/foobar/versions", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`[1, 2, 3, 4]`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	versions, err := client.Versions(context.Background(), "foobar")

	assert.NoError(t, err)
	assert.EqualValues(t, []int{1, 2, 3, 4}, versions)
}

func Test_Versions_with_an_unparsable_subject(t *testing.T) {
	client, err := NewClient("foobar://unreachable-url")
	require.NoError(t, err)

	versions, err := client.Versions(context.Background(), "%gh&%ij")

	assert.Empty(t, versions)
	assert.EqualError(t, err, `parse "subjects/%gh&%ij/versions": invalid URL escape "%gh"`)
}

func Test_Versions_with_a_network_error(t *testing.T) {
	client, err := NewClient("foobar://unreachable-url")
	require.NoError(t, err)

	versions, err := client.Versions(context.Background(), "foobar")

	assert.Empty(t, versions)
	assert.EqualError(t, err, `Get "foobar://unreachable-url/subjects/foobar/versions": unsupported protocol scheme "foobar"`)
}

func Test_Versions_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(`{
"error_code": 404,
			"message": "subject not found"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	subjects, err := client.Versions(context.Background(), "foobar")

	assert.Empty(t, subjects)
	assert.EqualError(t, err, fmt.Sprintf("client: (GET: %s/subjects/foobar/versions) failed with error code 404: subject not found", ts.URL))
}

func Test_Versions_with_an_invalid_json_as_response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	subject, err := client.Versions(context.Background(), "foobar")

	assert.Empty(t, subject)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_DeleteSubject_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/subjects/foobar", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`[1, 2, 3, 4]`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	versions, err := client.DeleteSubject(context.Background(), "foobar")

	assert.NoError(t, err)
	assert.EqualValues(t, []int{1, 2, 3, 4}, versions)
}

func Test_DeleteSubject_with_an_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(`{
"error_code": 404,
			"message": "subject not found"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	versions, err := client.DeleteSubject(context.Background(), "foobar")

	assert.Empty(t, versions)
	assert.EqualError(t, err, fmt.Sprintf("client: (DELETE: %s/subjects/foobar) failed with error code 404: subject not found", ts.URL))
}

func Test_DeleteSubject_with_an_invalid_json_as_response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	versions, err := client.DeleteSubject(context.Background(), "foobar")

	assert.Empty(t, versions)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_DeleteSubject_with_an_invalid_json_as_error_response(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	versions, err := client.DeleteSubject(context.Background(), "foobar")

	assert.Empty(t, versions)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_IsRegistered_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/test", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
		"subject": "test",
		"id": 1,
		"version": 3,
		"schema": "{ \"type\": \"record\", \"name\": \"test\", \"fields\": [{ \"type\": \"string\", \"name\": \"field1\" }] }"
	}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	exists, schema, err := client.IsRegistered(context.Background(), "test", `{
		"schema": "{ \"type\": \"record\", \"name\": \"test\", \"fields\": [{ \"type\": \"string\", \"name\": \"field1\" }]
		}"
    }`)

	assert.NoError(t, err)
	assert.True(t, exists)
	assert.EqualValues(t, &Schema{
		Subject: "test",
		ID:      1,
		Version: 3,
		Schema:  `{ "type": "record", "name": "test", "fields": [{ "type": "string", "name": "field1" }] }`,
	}, schema)
}

func Test_IsRegistered_with_a_remote_error(t *testing.T) {
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

	exists, schema, err := client.IsRegistered(context.Background(), "test", `{
		"schema": "{ \"type\": \"record\", \"name\": \"test\", \"fields\": [{ \"type\": \"string\", \"name\": \"field1\" }]
		}"
    }`)

	assert.Empty(t, schema)
	assert.False(t, exists)
	assert.EqualError(t, err, fmt.Sprintf("client: (POST: %s/subjects/test) failed with error code 404: schema not found", ts.URL))
}

func Test_IsRegistered_with_an_invalid_response_format(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("not a valid json"))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	exists, schema, err := client.IsRegistered(context.Background(), "test", `{
		"schema": "{ \"type\": \"record\", \"name\": \"test\", \"fields\": [{ \"type\": \"string\", \"name\": \"field1\" }]
		}"
    }`)

	assert.Empty(t, schema)
	assert.False(t, exists)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_RegisterNewSchema_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/subjects/test/versions", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"id": 1}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	version, err := client.RegisterNewSchema(context.Background(), "test", `{
		"type": "record",
		"name": "test",
		"fields": [{ "type": "string", "name": "field1" }]
    }`)

	assert.NoError(t, err)
	assert.Equal(t, 1, version)
}

func Test_RegisterNewSchema_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, err := w.Write([]byte(`{
"error_code": 404,
			"message": "subject not found"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	version, err := client.RegisterNewSchema(context.Background(), "test", `{
		"type": "record",
		"name": "test",
		"fields": [{ "type": "string", "name": "field1" }]
    }`)

	assert.Equal(t, -1, version)
	assert.EqualError(t, err, fmt.Sprintf("client: (POST: %s/subjects/test/versions) failed with error code 404: subject not found", ts.URL))
}

func Test_RegisterNewSchema_with_an_invalid_response_format(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("not a valid json"))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	version, err := client.RegisterNewSchema(context.Background(), "test", `{
		"type": "record",
		"name": "test",
		"fields": [{ "type": "string", "name": "field1" }]
    }`)

	assert.Equal(t, -1, version)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_GetSchemabySubjectAndVersion_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/test/versions/1", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"subject": "test",
			"version": 1,
			"schema": "{\"type\": \"string\"}"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetSchemaBySubjectAndVersion(context.Background(), "test", 1)

	assert.NoError(t, err)
	assert.EqualValues(t, &Schema{
		Subject: "test",
		Version: 1,
		Schema:  `{"type": "string"}`,
	}, schema)
}

func Test_GetLatestSchema_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/subjects/test/versions/latest", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"subject": "test",
			"version": 1,
			"schema": "{\"type\": \"string\"}"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetLatestSchema(context.Background(), "test")

	assert.NoError(t, err)
	assert.EqualValues(t, &Schema{
		Subject: "test",
		Version: 1,
		Schema:  `{"type": "string"}`,
	}, schema)
}

func Test_GetSchemabySubjectAndVersion_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, err := w.Write([]byte(`{
			"error_code": 500,
			"message": "internal server error"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetSchemaBySubjectAndVersion(context.Background(), "test", 1)

	assert.Nil(t, schema)
	assert.EqualError(t, err, fmt.Sprintf("client: (GET: %s/subjects/test/versions/1) failed with error code 500: internal server error", ts.URL))
}

func Test_GetSchemabySubjectAndVersion_with_an_invalid_response_format(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	schema, err := client.GetSchemaBySubjectAndVersion(context.Background(), "test", 1)

	assert.Nil(t, schema)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_GetConfig_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/config/test", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"compatibility": "FULL"}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	config, err := client.GetConfig(context.Background(), "test")

	assert.NoError(t, err)
	assert.EqualValues(t, &Config{
		Compatibility: "FULL",
	}, config)
}

func Test_GetConfig_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, err := w.Write([]byte(`{
			"error_code": 500,
			"message": "internal server error"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	config, err := client.GetConfig(context.Background(), "test")

	assert.Nil(t, config)
	assert.EqualError(t, err, fmt.Sprintf("client: (GET: %s/config/test) failed with error code 500: internal server error", ts.URL))
}

func Test_GetConfig_with_an_invalid_response_format(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	config, err := client.GetConfig(context.Background(), "test")

	assert.Nil(t, config)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_DeleteSchemaVersion_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/subjects/test/versions/2", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`4`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	id, err := client.DeleteSchemaVersion(context.Background(), "test", 2)

	assert.NoError(t, err)
	assert.Equal(t, 4, id)
}

func Test_DeleteSchemaVersion_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, err := w.Write([]byte(`{
			"error_code": 500,
			"message": "internal server error"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	id, err := client.DeleteSchemaVersion(context.Background(), "test", 2)

	assert.Equal(t, -1, id)
	assert.EqualError(t, err, fmt.Sprintf("client: (DELETE: %s/subjects/test/versions/2) failed with error code 500: internal server error", ts.URL))
}

func Test_DeleteSchemaVersion_with_an_invalid_response_format(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	id, err := client.DeleteSchemaVersion(context.Background(), "test", 2)

	assert.Equal(t, -1, id)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}

func Test_DeleteLatestSchemaVersion_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/subjects/test/versions/latest", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`4`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	id, err := client.DeleteLatestSchemaVersion(context.Background(), "test")

	assert.NoError(t, err)
	assert.Equal(t, 4, id)
}

func Test_SchemaCompatibleWith_success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "/compatibility/subjects/test/versions/4", r.URL.String())

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"is_compatible": true
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	isCompatible, err := client.SchemaCompatibleWith(context.Background(), `{"type": "string"}`, "test", 4)

	assert.NoError(t, err)
	assert.True(t, isCompatible)
}

func Test_SchemaCompatibleWith_with_a_remote_error(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, err := w.Write([]byte(`{
			"error_code": 500,
			"message": "internal server error"
		}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	isCompatible, err := client.SchemaCompatibleWith(context.Background(), `{"type": "string"}`, "test", 2)

	assert.False(t, isCompatible)
	assert.EqualError(t, err, fmt.Sprintf("client: (POST: %s/compatibility/subjects/test/versions/2) failed with error code 500: internal server error", ts.URL))
}

func Test_SchemaCompatibleWith_with_an_invalid_response_format(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not a valid json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL)
	require.NoError(t, err)

	isCompatible, err := client.SchemaCompatibleWith(context.Background(), `{"type": "string"}`, "test", 2)

	assert.False(t, isCompatible)
	assert.EqualError(t, err, "failed to decode the response: invalid character 'o' in literal null (expecting 'u')")
}
