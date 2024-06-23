package writer

import (
	"cloud.google.com/go/firestore"
	"context"
	"strings"
)

func Create(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}) error {
	var docRef *firestore.DocumentRef
	// TODO apply idField.IsZero() for golang 13 or above
	if len(id) > 0 {
		docRef = collection.Doc(id)
	} else {
		docRef = collection.NewDoc()
	}
	_, err := docRef.Create(ctx, model)
	return err
}
func Update(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}) (int64, error) {
	docRef := collection.Doc(id)
	_, err := docRef.Get(ctx)
	if err != nil {
		if strings.HasSuffix(err.Error(), " not found") {
			return 0, nil
		}
		return 0, err
	}
	_, er2 := docRef.Set(ctx, model)
	if er2 != nil {
		return 0, er2
	}
	return 1, err
}
func Save(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}) error {
	_, err := collection.Doc(id).Set(ctx, model)
	return err
}
