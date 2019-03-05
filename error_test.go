package schemaregistry

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsSubjectNotFound(t *testing.T) {
	err := ResourceError{
		ErrorCode: subjectNotFoundCode,
		Method:    "GET",
		URI:       "some-uri",
		Message:   "some-error",
	}

	assert.True(t, IsSubjectNotFound(err))
	assert.False(t, IsSchemaNotFound(err))
}

func Test_IsSubjectNotFound_with_no_error(t *testing.T) {
	assert.False(t, IsSubjectNotFound(nil))
}

func Test_IsSubjectNotFound_with_system_error(t *testing.T) {
	assert.False(t, IsSubjectNotFound(fmt.Errorf("some-error")))
}

func Test_IsSchemaNotFound(t *testing.T) {
	err := ResourceError{
		ErrorCode: schemaNotFoundCode,
		Method:    "GET",
		URI:       "some-uri",
		Message:   "some-error",
	}

	assert.True(t, IsSchemaNotFound(err))
	assert.False(t, IsSubjectNotFound(err))
}

func Test_IsSchemaNotFound_with_no_error(t *testing.T) {
	assert.False(t, IsSchemaNotFound(nil))
}

func Test_IsSchemaNotFound_with_system_error(t *testing.T) {
	assert.False(t, IsSchemaNotFound(fmt.Errorf("some-error")))
}

func Test_ResourceError_Error_format(t *testing.T) {
	err := ResourceError{
		ErrorCode: schemaNotFoundCode,
		Method:    "GET",
		URI:       "some-uri",
		Message:   "some-error",
	}

	assert.Equal(t, "client: (GET: some-uri) failed with error code 40403: some-error", err.Error())
}
