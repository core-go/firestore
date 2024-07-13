# Firestore
- Firestore is a library to wrap [Google Firestore](cloud.google.com/go/firestore) with these purposes:
#### Simplified Database Operations
- simplify common database operations, such as CRUD (Create, Read, Update, Delete) operations, transactions, and batch processing
#### Reduced Boilerplate Code
- reduce boilerplate code associated with database interactions, allowing developers to focus more on application logic rather than low-level database handling

## Some advantage features
#### Generic Repository (CRUD repository)
#### Search Repository
#### Dynamic query builder
#### For batch job
- Creator
- Updater
- Writer
- StreamCreator
- StreamUpdater
- StreamWriter
- BatchCreator
- BatchUpdater
- BatchWriter
#### Export Service to export data

## Installation
Please make sure to initialize a Go module before installing core-go/firestore:

```shell
go get -u github.com/core-go/firestore
```

Import:
```go
import "github.com/core-go/firestore"
```