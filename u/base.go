package u

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

func Must(err error) {
	if err != nil {
		panic(err)
	}
}

func Must2[T any](x T, err error) T {
	if err != nil {
		panic(err)
	}
	return x
}

func PanicIf(cond bool, args ...interface{}) {
	if !cond {
		return
	}
	s := "condition failed"
	if len(args) > 0 {
		s = fmt.Sprintf("%s", args[0])
		if len(args) > 1 {
			s = fmt.Sprintf(s, args[1:]...)
		}
	}
	panic(s)
}

func PanicIfErr(err error, args ...interface{}) {
	if err == nil {
		return
	}
	s := err.Error()
	if len(args) > 0 {
		s = fmt.Sprintf("%s", args[0])
		if len(args) > 1 {
			s = fmt.Sprintf(s, args[1:]...)
		}
	}
	panic(s)
}

func GetErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func IsWindows() bool {
	return strings.Contains(runtime.GOOS, "windows")
}

func IsMac() bool {
	return strings.Contains(runtime.GOOS, "darwin")
}

func IsWinOrMac() bool {
	return IsWindows() || IsMac()
}

func IsLinux() bool {
	return strings.Contains(runtime.GOOS, "linux")
}

func GetCallstackFrames(skipFrames int) []string {
	var callers [32]uintptr
	n := runtime.Callers(skipFrames+1, callers[:])
	frames := runtime.CallersFrames(callers[:n])
	var cs []string
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		s := frame.File + ":" + strconv.Itoa(frame.Line)
		cs = append(cs, s)
	}
	return cs
}

func GetCallstack(skipFrames int) string {
	frames := GetCallstackFrames(skipFrames + 1)
	return strings.Join(frames, "\n")
}

func Push[S ~[]E, E any](s *S, els ...E) {
	*s = append(*s, els...)
}

func SliceLimit[S ~[]E, E any](s S, max int) S {
	if len(s) > max {
		return s[:max]
	}
	return s
}
