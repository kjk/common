package httputil

import (
	"net/url"
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

func TestMakeFullRedirectURL(t *testing.T) {
	tests := []string{
		"/foo.html#me;him", "/bar", "/bar#me;him",
		"/foo.html", "/bar", "/bar",
		"/foo.html?me=him", "/bar", "/bar?me=him",
		"/foo.html?me=him#me", "/bar", "/bar?me=him#me",
	}
	for i := 0; i < len(tests); i += 3 {
		u, err := url.Parse(tests[i])
		assert.NoError(t, err)
		path := tests[i+1]
		exp := tests[i+2]
		got := MakeFullRedirectURL(path, u)
		assert.Equal(t, exp, got)
	}
}
