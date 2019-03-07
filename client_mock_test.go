package schemaregistry

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MockClient_GetSchemaByID(t *testing.T) {
	mock := new(ClientMock)

	mock.On("GetSchemaByID", 42).Return("some-schema", nil)

	schema, err := mock.GetSchemaByID(context.Background(), 42)

	assert.NoError(t, err)
	assert.Equal(t, "some-schema", schema)
}

func Test_MockClient_Subjects(t *testing.T) {
	mock := new(ClientMock)

	mock.On("Subjects").Return([]string{"subject1", "subject2"}, nil)

	subjects, err := mock.Subjects(context.Background())

	assert.NoError(t, err)
	assert.EqualValues(t, []string{"subject1", "subject2"}, subjects)
}

func Test_MockClient_Subjects_with_error(t *testing.T) {
	mock := new(ClientMock)

	mock.On("Subjects").Return(nil, fmt.Errorf("some-error"))

	subjects, err := mock.Subjects(context.Background())

	assert.Nil(t, subjects)
	assert.EqualError(t, err, "some-error")
}

func Test_MockClient_Versions(t *testing.T) {
	mock := new(ClientMock)

	mock.On("Versions", "some-subject").Return([]int{1, 2, 3}, nil)

	versions, err := mock.Versions(context.Background(), "some-subject")

	assert.NoError(t, err)
	assert.EqualValues(t, []int{1, 2, 3}, versions)
}

func Test_MockClient_Versions_with_error(t *testing.T) {
	mock := new(ClientMock)

	mock.On("Versions", "some-subject").Return(nil, fmt.Errorf("some-error"))

	versions, err := mock.Versions(context.Background(), "some-subject")

	assert.Nil(t, versions)
	assert.EqualError(t, err, "some-error")
}

func Test_MockClient_DeleteSubject(t *testing.T) {
	mock := new(ClientMock)

	mock.On("DeleteSubject", "some-subject").Return([]int{1, 2, 3}, nil)

	versions, err := mock.DeleteSubject(context.Background(), "some-subject")

	assert.NoError(t, err)
	assert.EqualValues(t, []int{1, 2, 3}, versions)
}

func Test_MockClient_DeleteSubject_with_error(t *testing.T) {
	mock := new(ClientMock)

	mock.On("DeleteSubject", "some-subject").Return(nil, fmt.Errorf("some-error"))

	versions, err := mock.DeleteSubject(context.Background(), "some-subject")

	assert.Nil(t, versions)
	assert.EqualError(t, err, "some-error")
}

func Test_MockClient_IsRegistered(t *testing.T) {
	mock := new(ClientMock)

	validSchema := `{"key": "value"}`
	mock.On("IsRegistered", "some-subject", validSchema).Return(true, &Schema{
		Schema:  validSchema,
		Subject: "some-subject",
		Version: 4,
	}, nil)

	registered, schema, err := mock.IsRegistered(context.Background(), "some-subject", validSchema)

	assert.NoError(t, err)
	assert.True(t, registered)
	assert.EqualValues(t, &Schema{
		Schema:  validSchema,
		Subject: "some-subject",
		Version: 4,
	}, schema)
}

func Test_MockClient_RegisterNewSchema(t *testing.T) {
	mock := new(ClientMock)

	validSchema := `{"key": "value"}`
	mock.On("RegisterNewSchema", "some-subject", validSchema).Return(22, nil)

	id, err := mock.RegisterNewSchema(context.Background(), "some-subject", validSchema)

	assert.NoError(t, err)
	assert.Equal(t, 22, id)
}

func Test_MockClient_GetSchemaBySubjectAndVersion(t *testing.T) {
	mock := new(ClientMock)
	validSchema := `{"key": "value"}`

	mock.On("GetSchemaBySubjectAndVersion", "some-subject", 4).Return(&Schema{
		Schema:  validSchema,
		Subject: "some-subject",
		Version: 4,
	}, nil)

	schema, err := mock.GetSchemaBySubjectAndVersion(context.Background(), "some-subject", 4)

	assert.NoError(t, err)
	assert.EqualValues(t, &Schema{
		Schema:  validSchema,
		Subject: "some-subject",
		Version: 4,
	}, schema)
}

func Test_MockClient_GetSchemaBySubjectAndVersion_with_error(t *testing.T) {
	mock := new(ClientMock)

	mock.On("GetSchemaBySubjectAndVersion", "some-subject", 4).Return(nil, fmt.Errorf("some-error"))

	schema, err := mock.GetSchemaBySubjectAndVersion(context.Background(), "some-subject", 4)

	assert.Nil(t, schema)
	assert.EqualError(t, err, "some-error")
}

func Test_MockClient_GetLatestSchema(t *testing.T) {
	mock := new(ClientMock)
	validSchema := `{"key": "value"}`

	mock.On("GetLatestSchema", "some-subject").Return(&Schema{
		Schema:  validSchema,
		Subject: "some-subject",
		Version: 4,
	}, nil)

	schema, err := mock.GetLatestSchema(context.Background(), "some-subject")

	assert.NoError(t, err)
	assert.EqualValues(t, &Schema{
		Schema:  validSchema,
		Subject: "some-subject",
		Version: 4,
	}, schema)
}

func Test_MockClient_GetLatestSchema_with_error(t *testing.T) {
	mock := new(ClientMock)

	mock.On("GetLatestSchema", "some-subject").Return(nil, fmt.Errorf("some-error"))

	schema, err := mock.GetLatestSchema(context.Background(), "some-subject")

	assert.Nil(t, schema)
	assert.EqualError(t, err, "some-error")
}

func Test_MockClient_GetConfig(t *testing.T) {
	mock := new(ClientMock)

	mock.On("GetConfig", "some-subject").Return(&Config{Compatibility: "FULL"}, nil)

	config, err := mock.GetConfig(context.Background(), "some-subject")

	assert.NoError(t, err)
	assert.EqualValues(t, &Config{Compatibility: "FULL"}, config)
}

func Test_MockClient_GetConfig_with_error(t *testing.T) {
	mock := new(ClientMock)

	mock.On("GetConfig", "some-subject").Return(nil, fmt.Errorf("some-error"))

	config, err := mock.GetConfig(context.Background(), "some-subject")

	assert.Nil(t, config)
	assert.EqualError(t, err, "some-error")
}

func Test_MockClient_DeleteSchemaVersion(t *testing.T) {
	mock := new(ClientMock)

	mock.On("DeleteSchemaVersion", "some-subject", 3).Return(12, nil)

	id, err := mock.DeleteSchemaVersion(context.Background(), "some-subject", 3)

	assert.NoError(t, err)
	assert.Equal(t, 12, id)
}

func Test_MockClient_DeleteLatestSchemaVersion(t *testing.T) {
	mock := new(ClientMock)

	mock.On("DeleteLatestSchemaVersion", "some-subject").Return(12, nil)

	id, err := mock.DeleteLatestSchemaVersion(context.Background(), "some-subject")

	assert.NoError(t, err)
	assert.Equal(t, 12, id)
}

func Test_MockClient_SchemaCompatibleWith(t *testing.T) {
	mock := new(ClientMock)
	validSchema := `{"key": "value"}`

	mock.On("SchemaCompatibleWith", validSchema, "some-subject", 5).Return(true, nil)

	isCompatible, err := mock.SchemaCompatibleWith(context.Background(), validSchema, "some-subject", 5)

	assert.NoError(t, err)
	assert.True(t, isCompatible)
}
