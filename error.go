package schemaregistry

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// These numbers are used by the schema registry to communicate errors.
const (
	subjectNotFoundCode = 40401
	schemaNotFoundCode  = 40403
)

// ResourceError is being fired from all API calls when an error code is received.
type ResourceError struct {
	ErrorCode int    `json:"error_code"`
	Method    string `json:"method,omitempty"`
	URI       string `json:"uri,omitempty"`
	Message   string `json:"message,omitempty"`
}

// Error is used to implement the error interface.
func (err ResourceError) Error() string {
	return fmt.Sprintf("client: (%s: %s) failed with error code %d: %s",
		err.Method, err.URI, err.ErrorCode, err.Message)
}

// IsSubjectNotFound checks the returned error to see if it is kind of a subject
// not found  error code.
func IsSubjectNotFound(err error) bool {
	if err == nil {
		return false
	}

	if resErr, ok := err.(ResourceError); ok {
		return resErr.ErrorCode == subjectNotFoundCode
	}

	return false
}

// IsSchemaNotFound checks the returned error to see if it is kind of a schema
// not found error code.
func IsSchemaNotFound(err error) bool {
	if err == nil {
		return false
	}

	if resErr, ok := err.(ResourceError); ok {
		return resErr.ErrorCode == schemaNotFoundCode
	}

	return false
}

func parseResponseError(req *http.Request, res *http.Response) error {
	if res.StatusCode == 200 {
		return nil
	}

	var resErr ResourceError

	err := json.NewDecoder(res.Body).Decode(&resErr)
	if err != nil {
		return err
	}

	resErr.URI = req.URL.String()
	resErr.Method = req.Method

	return resErr
}
