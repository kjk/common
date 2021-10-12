package server

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/kjk/common/assert"
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
