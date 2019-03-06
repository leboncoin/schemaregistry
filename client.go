package schemaregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// Option function used to apply modifications to the client.
type Option func(*Client)

// Client used to interact with the registry schema REST API.
type Client struct {
	baseURL *url.URL

	client *http.Client
}

// Schema describes a schema, look `GetSchema` for more.
type Schema struct {
	// Schema is the Avro schema string.
	Schema string `json:"schema"`
	// Subject where the schema is registered for.
	Subject string `json:"subject"`
	// Version of the returned schema.
	Version int `json:"version"`
	ID      int `json:"id,omitempty"`
}

// UsingClient modifies the underline HTTP Client that schema registry is using for contact with the backend server.
func UsingClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.client = httpClient
	}
}

// NewClient instantiate a new Client.
func NewClient(baseURL string, options ...Option) (*Client, error) {
	url, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	client := &Client{
		baseURL: url,
		client:  http.DefaultClient,
	}

	for _, opt := range options {
		opt(client)
	}

	return client, nil
}

// GetSchemaByID returns the Avro schema string identified by the id.
//
// https://docs.confluent.io/current/schema-registry/docs/api.html#get--schemas-ids-int-%20id
func (c *Client) GetSchemaByID(ctx context.Context, subjectID int) (string, error) {
	type responseBody struct {
		Schema string `json:"schema"`
	}

	rawBody, err := c.execRequest(ctx, "GET", fmt.Sprintf("schemas/ids/%d", subjectID), nil)
	if err != nil {
		return "", err
	}

	var resBody responseBody
	err = json.Unmarshal(rawBody, &resBody)
	if err != nil {
		return "", fmt.Errorf("failed to decode the response: %s", err)
	}

	return resBody.Schema, nil
}

// Subjects returns a list of the available subjects(schemas).
//
// https://docs.confluent.io/current/schema-registry/docs/api.html#subjects
func (c *Client) Subjects(ctx context.Context) (subjects []string, err error) {
	type responseBody []string

	rawBody, err := c.execRequest(ctx, "GET", "subjects", nil)
	if err != nil {
		return nil, err
	}

	var resBody responseBody
	err = json.Unmarshal(rawBody, &resBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the response: %s", err)
	}

	return resBody, nil
}

// Versions returns all schema version numbers registered for this subject.
//
// https://docs.confluent.io/current/schema-registry/docs/api.html#get--subjects-(string-%20subject)-versions
func (c *Client) Versions(ctx context.Context, subject string) (versions []int, err error) {
	type responseBody []int

	rawBody, err := c.execRequest(ctx, "GET", fmt.Sprintf("subjects/%s/versions", subject), nil)
	if err != nil {
		return nil, err
	}

	var resBody responseBody
	err = json.Unmarshal(rawBody, &resBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the response: %s", err)
	}

	return resBody, nil
}

// DeleteSubject deletes the specified subject and its associated compatibility level if registered.
// It is recommended to use this API only when a topic needs to be recycled or in development environment.
// Returns the versions of the schema deleted under this subject.
//
// https://docs.confluent.io/current/schema-registry/docs/api.html#delete--subjects-(string-%20subject)
func (c *Client) DeleteSubject(ctx context.Context, subject string) (versions []int, err error) {
	type responseBody []int

	rawBody, err := c.execRequest(ctx, "DELETE", fmt.Sprintf("subjects/%s", subject), nil)
	if err != nil {
		return nil, err
	}

	var resBody responseBody
	err = json.Unmarshal(rawBody, &resBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the response: %s", err)
	}

	return resBody, nil
}

// IsRegistered tells if the given "schema" is registered for this "subject".
//
// https://docs.confluent.io/current/schema-registry/docs/api.html#post--subjects-(string-%20subject)
func (c *Client) IsRegistered(ctx context.Context, subject string, schema string) (bool, *Schema, error) {
	type requestBody struct {
		Schema string `json:"schema"`
	}

	// nolint
	// Error not possible here.
	reqBody, _ := json.Marshal(&requestBody{Schema: schema})

	rawBody, err := c.execRequest(ctx, "POST", fmt.Sprintf("subjects/%s", subject), bytes.NewReader(reqBody))
	if err != nil {
		return false, nil, err
	}

	var resBody Schema
	err = json.Unmarshal(rawBody, &resBody)
	if err != nil {
		return false, nil, fmt.Errorf("failed to decode the response: %s", err)
	}

	return true, &resBody, nil
}

// Execute the request and check for an error into the response.
//
// In case of succes it return the raw body.
//
// It return an error if:
// - an error occure with the network
// - an error occure with the remote api
// - the request the params have an invalid
// - the response have an invalid format
// - the response is an error
func (c *Client) execRequest(ctx context.Context, method string, rawPath string, body io.Reader) ([]byte, error) {
	path, err := url.Parse(rawPath)
	if err != nil {
		return nil, err
	}

	// nolint
	// The request is always valid
	req, _ := http.NewRequest(method, c.baseURL.ResolveReference(path).String(), nil)
	req.Header.Add("Accept", "application/json")

	res, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	err = parseResponseError(req, res)
	if err != nil {
		return nil, err
	}

	rawBody, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return rawBody, nil
}
