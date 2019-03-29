package schemaregistry

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// ClientMock is a mock implementation of Client.
type ClientMock struct {
	mock.Mock
}

// GetSchemaByID method mock
func (c *ClientMock) GetSchemaByID(ctx context.Context, subjectID int) (string, error) {
	args := c.Called(subjectID)

	return args.String(0), args.Error(1)
}

// Subjects method mock
func (c *ClientMock) Subjects(ctx context.Context) (subjects []string, err error) {
	args := c.Called()

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]string), args.Error(1)
}

// Versions method mock
func (c *ClientMock) Versions(ctx context.Context, subject string) (versions []int, err error) {
	args := c.Called(subject)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]int), args.Error(1)
}

// DeleteSubject method mock
func (c *ClientMock) DeleteSubject(ctx context.Context, subject string) (versions []int, err error) {
	args := c.Called(subject)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]int), args.Error(1)
}

// IsRegistered method mock
func (c *ClientMock) IsRegistered(ctx context.Context, subject string, schema string) (bool, *Schema, error) {
	args := c.Called(subject, schema)

	if args.Get(1) == nil {
		return args.Bool(0), nil, args.Error(2)
	}

	return args.Bool(0), args.Get(1).(*Schema), args.Error(2)
}

// RegisterNewSchema method mock
func (c *ClientMock) RegisterNewSchema(ctx context.Context, subject string, avroSchema string) (int, error) {
	args := c.Called(subject, avroSchema)

	return args.Int(0), args.Error(1)
}

// GetSchemaBySubjectAndVersion method mock
func (c *ClientMock) GetSchemaBySubjectAndVersion(ctx context.Context, subject string, version int) (*Schema, error) {
	args := c.Called(subject, version)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*Schema), args.Error(1)
}

// GetLatestSchema method mock
func (c *ClientMock) GetLatestSchema(ctx context.Context, subject string) (*Schema, error) {
	args := c.Called(subject)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*Schema), args.Error(1)
}

// GetConfig method mock
func (c *ClientMock) GetConfig(ctx context.Context, subject string) (*Config, error) {
	args := c.Called(subject)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*Config), args.Error(1)
}

// DeleteSchemaVersion method mock
func (c *ClientMock) DeleteSchemaVersion(ctx context.Context, subject string, version int) (int, error) {
	args := c.Called(subject, version)

	return args.Int(0), args.Error(1)
}

// DeleteLatestSchemaVersion method mock
func (c *ClientMock) DeleteLatestSchemaVersion(ctx context.Context, subject string) (int, error) {
	args := c.Called(subject)

	return args.Int(0), args.Error(1)
}

// SchemaCompatibleWith method mock
func (c *ClientMock) SchemaCompatibleWith(ctx context.Context, schema string, subject string, version int) (bool, error) {
	args := c.Called(schema, subject, version)

	return args.Bool(0), args.Error(1)
}
