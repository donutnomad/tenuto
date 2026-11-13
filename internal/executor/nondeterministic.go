package executor

type NonDeterministicError struct {
	message string
}

func NewNonDeterministicError(message string) NonDeterministicError {
	return NonDeterministicError{message: message}
}

func (e NonDeterministicError) Error() string { return e.message }
