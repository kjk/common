package u

import (
	"fmt"
	"runtime"
	"strings"
)

func Must(err error) {
	if err != nil {
		panic(err)
	}
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

func IsWindows() bool {
	return strings.Contains(runtime.GOOS, "windows")
}

func IsMac() bool {
	return strings.Contains(runtime.GOOS, "darwin")
}

func IsWinOrMac() bool {
	return IsWindows() || IsMac()
}
