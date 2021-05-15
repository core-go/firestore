package firestore

import (
	"cloud.google.com/go/firestore"
	"reflect"
)

func NewSearchLoader(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), createdTimeFieldName string, updatedTimeFieldName string, options ...string) (*Searcher, *Loader) {
	return NewSearchLoaderWithSort(client, collectionName, modelType, buildQuery, getSortAndRefId, BuildSort, createdTimeFieldName, updatedTimeFieldName, options...)
}
func NewSearchLoaderWithSort(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, createdTimeFieldName string, updatedTimeFieldName string, options ...string) (*Searcher, *Loader) {
	loader := NewLoader(client, collectionName, modelType, createdTimeFieldName, updatedTimeFieldName, options...)
	searcher := NewSearcherWithQueryAndSort(client, collectionName, modelType, buildQuery, getSortAndRefId, buildSort, createdTimeFieldName, updatedTimeFieldName, options...)
	return searcher, loader
}
