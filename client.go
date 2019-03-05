package schemaregistry

import (
	"context"
	"encoding/json"
	"fmt"
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
// id (int) – the globally unique identifier of the schema.
func (c *Client) GetSchemaByID(ctx context.Context, subjectID int) (string, error) {
	type responseBody struct {
		Schema string `json:"schema"`
	}

	// nolint
	// The path cannot be invalid
	path, _ := url.Parse(fmt.Sprintf("schemas/ids/%d", subjectID))

	// nolint
	// The request is always valid
	req, _ := http.NewRequest("GET", c.baseURL.ResolveReference(path).String(), nil)
	req.Header.Add("Accept", "application/json")

	res, err := c.client.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	err = parseResponseError(req, res)
	if err != nil {
		return "", err
	}

	var resBody responseBody
	err = json.NewDecoder(res.Body).Decode(&resBody)
	if err != nil {
		return "", fmt.Errorf("failed to decode the response: %s", err)
	}

	return resBody.Schema, nil
}

// Subjects returns a list of the available subjects(schemas).
// https://docs.confluent.io/current/schema-registry/docs/api.html#subjects
func (c *Client) Subjects(ctx context.Context) (subjects []string, err error) {
	type responseBody []string

	// nolint
	// The path cannot be invalid
	path, _ := url.Parse("subjects")

	// nolint
	// The request is always valid
	req, _ := http.NewRequest("GET", c.baseURL.ResolveReference(path).String(), nil)
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

	var resBody responseBody
	err = json.NewDecoder(res.Body).Decode(&resBody)
	if err != nil {
		return nil, fmt.Errorf("failed to decode the response: %s", err)
	}

	return resBody, nil
}
