package siser

import (
	"fmt"
	"time"
)

func fmtArgs(args ...interface{}) string {
	if len(args) == 0 {
		return ""
	}
	format := args[0].(string)
	if len(args) == 1 {
		return format
	}
	return fmt.Sprintf(format, args[1:]...)
}

func panicWithMsg(defaultMsg string, args ...interface{}) {
	s := fmtArgs(args...)
	if s == "" {
		s = defaultMsg
	}
	panic(s)
}

func panicIfErr(err error, args ...interface{}) {
	if err == nil {
		return
	}
	panicWithMsg(err.Error(), args...)
}

func panicIf(cond bool, args ...interface{}) {
	if !cond {
		return
	}
	panicWithMsg("fatalIf: condition failed", args...)
}

// intStrLen calculates how long n would be when converted to a string
// i.e. equivalent of len(strconv.Itoa(n)) but faster
// Note: not used
func intStrLen(n int) int {
	l := 1 // count the last digit here
	if n < 0 {
		n = -n
		l = 2
	}
	for n > 9 {
		l++
		n = n / 10
	}
	return l
}

func serializableOnLine(s string) bool {
	n := len(s)
	for i := 0; i < n; i++ {
		b := s[i]
		if b < 32 || b > 127 {
			return false
		}
	}
	return true
}

// TimeToUnixMillisecond converts t into Unix epoch time in milliseconds.
// That's because seconds is not enough precision and nanoseconds is too much.
func TimeToUnixMillisecond(t time.Time) int64 {
	n := t.UnixNano()
	return n / 1e6
}

// TimeFromUnixMillisecond returns time from Unix epoch time in milliseconds.
func TimeFromUnixMillisecond(unixMs int64) time.Time {
	nano := unixMs * 1e6
	return time.Unix(0, nano)
}
