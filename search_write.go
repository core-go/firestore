package firestore

import (
	"cloud.google.com/go/firestore"
	"reflect"
)

func NewSearchWriterWithSortAndVersion(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, versionField string, createdTimeFieldName string, updatedTimeFieldName string, options ...string) (*Searcher, *Writer) {
	writer := NewWriterWithVersion(client, collectionName, modelType, createdTimeFieldName, updatedTimeFieldName, versionField, options...)
	searcher := NewSearcherWithQueryAndSort(client, collectionName, modelType, buildQuery, getSortAndRefId, buildSort, createdTimeFieldName, updatedTimeFieldName, options...)
	return searcher, writer
}

func NewSearchWriterWithVersion(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), versionField string, createdTimeFieldName string, updatedTimeFieldName string, options ...string) (*Searcher, *Writer) {
	return NewSearchWriterWithSortAndVersion(client, collectionName, modelType, buildQuery, getSortAndRefId, BuildSort, versionField, createdTimeFieldName, updatedTimeFieldName, options...)
}

func NewSearchWriterWithSort(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, createdTimeFieldName string, updatedTimeFieldName string, options ...string) (*Searcher, *Writer) {
	return NewSearchWriterWithSortAndVersion(client, collectionName, modelType, buildQuery, getSortAndRefId, buildSort, "", createdTimeFieldName, updatedTimeFieldName, options...)
}

func NewSearchWriter(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), createdTimeFieldName string, updatedTimeFieldName string, options ...string) (*Searcher, *Writer) {
	return NewSearchWriterWithSortAndVersion(client, collectionName, modelType, buildQuery, getSortAndRefId, BuildSort, "", createdTimeFieldName, updatedTimeFieldName, options...)
}
