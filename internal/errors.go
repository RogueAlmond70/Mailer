package internal

import "errors"

// ErrAlreadyStored is returned when an email record insert hits the primary key.
var ErrAlreadyStored = errors.New("email record already exists")
