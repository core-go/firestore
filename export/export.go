package export

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"google.golang.org/api/iterator"
	"reflect"
)

func NewExportAdapter[T any](collection *firestore.CollectionRef,
	getIterator func(context.Context, *firestore.CollectionRef) *firestore.DocumentIterator,
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
	options ...string,
) *Exporter[T] {
	return NewExporter[T](collection, getIterator, transform, write, close, options...)
}
func NewExportService[T any](collection *firestore.CollectionRef,
	getIterator func(context.Context, *firestore.CollectionRef) *firestore.DocumentIterator,
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
	options ...string,
) *Exporter[T] {
	return NewExporter[T](collection, getIterator, transform, write, close, options...)
}
func NewExporter[T any](collection *firestore.CollectionRef,
	getIterator func(context.Context, *firestore.CollectionRef) *firestore.DocumentIterator,
	transform func(context.Context, *T) string,
	write func(p []byte) (n int, err error),
	close func() error,
	options ...string,
) *Exporter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	idx, _, _ := FindIdField(modelType)
	var createdTimeFieldName string
	var updatedTimeFieldName string
	if len(options) > 0 && len(options[0]) > 0 {
		createdTimeFieldName = options[0]
	}
	if len(options) > 1 && len(options[1]) > 0 {
		updatedTimeFieldName = options[1]
	}
	ctIdx := -1
	if len(createdTimeFieldName) >= 0 {
		ctIdx, _, _ = FindFieldByName(modelType, createdTimeFieldName)
	}
	utIdx := -1
	if len(updatedTimeFieldName) >= 0 {
		utIdx, _, _ = FindFieldByName(modelType, updatedTimeFieldName)
	}
	return &Exporter[T]{Collection: collection, Write: write, Close: close, Transform: transform, GetIterator: getIterator, IdIndex: idx, CreateTimeIndex: ctIdx, UpdateTimeIndex: utIdx}
}

type Exporter[T any] struct {
	Collection      *firestore.CollectionRef
	Transform       func(context.Context, *T) string
	GetIterator     func(context.Context, *firestore.CollectionRef) *firestore.DocumentIterator
	Write           func(p []byte) (n int, err error)
	Close           func() error
	IdIndex         int
	CreateTimeIndex int
	UpdateTimeIndex int
}

func (s *Exporter[T]) Export(ctx context.Context) (int64, error) {
	iter := s.GetIterator(ctx, s.Collection)
	return s.ScanAndWrite(ctx, iter)
}

func (s *Exporter[T]) ScanAndWrite(ctx context.Context, iter *firestore.DocumentIterator) (int64, error) {
	defer s.Close()
	var i int64
	i = 0
	for {
		docs, er1 := iter.Next()
		if errors.Is(er1, iterator.Done) {
			break
		}
		if er1 != nil {
			return i, er1
		}
		var obj T
		er2 := docs.DataTo(&obj)
		if er2 != nil {
			return i, er2
		}
		BindCommonFields(&obj, docs, s.IdIndex, s.CreateTimeIndex, s.UpdateTimeIndex)
		er3 := s.TransformAndWrite(ctx, s.Write, &obj)
		if er3 != nil {
			return i, er3
		}
		i = i + 1
	}
	return i, nil
}

func (s *Exporter[T]) TransformAndWrite(ctx context.Context, write func(p []byte) (n int, err error), model *T) error {
	line := s.Transform(ctx, model)
	_, er := write([]byte(line))
	return er
}
