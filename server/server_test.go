package server

import (
	"reflect"
	"testing"
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
