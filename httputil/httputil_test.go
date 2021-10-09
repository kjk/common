package httputil

import (
	"testing"

	"github.com/kjk/common/assert"
)

func TestJoinURL(t *testing.T) {
	tests := []string{
		"foo", "bar", "foo/bar",
		"foo", "/bar", "foo/bar",
		"foo/", "bar", "foo/bar",
		"foo/", "/bar", "foo/bar",
	}
	n := len(tests)
	for i := 0; i < n; i += 3 {
		got := JoinURL(tests[i], tests[i+1])
		exp := tests[i+2]
		assert.Equal(t, exp, got)
	}
}
