// Package errorcode is the authoritative registry for application error codes.
package errorcode

type Code int

const (
	OK                    Code = 0
	InvalidArgument       Code = 10001
	NotFound              Code = 10004
	RequestTimeout        Code = 10008
	TooManyRequests       Code = 10029
	Unauthorized          Code = 20001
	Forbidden             Code = 20003
	Conflict              Code = 30009
	RequestInProgress     Code = 30010
	StaleVersion          Code = 30011
	LockUnavailable       Code = 30012
	Internal              Code = 50000
	DependencyUnavailable Code = 50003
)

func (c Code) Valid() bool {
	switch c {
	case OK, InvalidArgument, NotFound, RequestTimeout, TooManyRequests,
		Unauthorized, Forbidden, Conflict, RequestInProgress, StaleVersion,
		LockUnavailable, Internal, DependencyUnavailable:
		return true
	default:
		return false
	}
}
