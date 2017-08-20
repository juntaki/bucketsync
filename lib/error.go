package bucketsync

type Errtype int

const (
	Unhandled Errtype = iota
	KeyNotFound
)

type Error struct {
	Errtype Errtype
	Message string
	OrigErr error
}

func newErrorKeyNotFound(origerr error, key ObjectKey) *Error {
	return &Error{
		Errtype: KeyNotFound,
		Message: "KeyNotFound: " + string(key),
		OrigErr: origerr,
	}
}

func IsKeyNotFound(err error) bool {
	if berr, ok := err.(*Error); ok {
		if berr.Errtype == KeyNotFound {
			return true
		}
	}
	return false
}

func (e *Error) Error() string { return e.Message }
