package internal

/*
I'm thinking interfaces for:
- Logger
- Any databases - I've not yet decided on data storage, likely something relational

-

*/

type Database interface {
	Get(key string, content string) error
	Set(key string, content string) error
	Delete(key string, content string) error
	Update(key string, content string) error
}

type Logger interface {
	Error(string, ...any)
	Info(string, ...any)
	Warn(string, ...any)
}
