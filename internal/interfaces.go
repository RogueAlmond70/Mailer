package internal

import (
	"context"
)

/*
I'm thinking interfaces for:
- Logger
- Any databases - I've not yet decided on data storage, likely something relational

-

*/

type Database interface {
	IsDuplicate(context.Context, string) (bool, error)
	StoreEmailRecord(context.Context, []byte) error
	DeleteEmailRecord(context.Context, string) error
	UploadDeadLetter(context.Context, []byte) error
	CreateTableIfNotExists(context.Context) error
}

type Logger interface {
	Error(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
}
