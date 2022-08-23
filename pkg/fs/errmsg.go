package fs

// error texts that are found in reported errors using strings.Contain(err.Error(), errXXX)
const (
	errBrokenPipe             = "broken pipe"
	errConnAborted            = "connection was aborted"
	errConnRefused            = "connection refused"
	errIO                     = "input/output error"
	errIOTimeout              = "i/o timeout"
	errIsDirectory            = "is a directory"
	errUnexpectedNetworkError = "unexpected network error"
)
