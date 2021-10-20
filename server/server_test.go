package server

import (
	"reflect"
	"testing"

	"github.com/kjk/common/assert"
	"github.com/kjk/common/u"
)

func TestGen404Candidates(t *testing.T) {
	tests := [][]string{
		{"/foo/zat/.html", "/foo/zat/404.html", "/foo/404.html", "/404.html"},
		{"/foo/bar/", "/foo/bar/404.html", "/foo/404.html", "/404.html"},
		{"/foo/bar.html", "/foo/404.html", "/404.html"},
		{"/foo/bar", "/foo/bar/404.html", "/foo/404.html", "/404.html"},
		{"/", "/404.html"},
		{"/index.html", "/404.html"},
	}
	for _, test := range tests {
		uri := test[0]
		exp := test[1:]
		got := Gen404Candidates(uri)
		if !reflect.DeepEqual(exp, got) {
			t.Errorf("uri: '%s', got: %v, exp: %v\n", uri, got, exp)
		}
	}
}

func TestTrimExt(t *testing.T) {
	tests := []string{
		"foo.html", "foo",
		"foo.HTML", "foo",
		"foo", "foo",
		"foo.html.txt", "foo.html",
	}

	for i := 0; i < len(tests); i += 2 {
		exp := tests[i+1]
		got := u.TrimExt(tests[i])
		assert.Equal(t, exp, got)
	}
}
