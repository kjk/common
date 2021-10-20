package u

import (
	"testing"

	"github.com/kjk/common/assert"
)

func TestTrimPrefix(t *testing.T) {
	tests := []string{
		"foo", "f", "oo",
		"foo", "o", "foo",
	}

	n := len(tests)
	for i := 0; i < n; i += 3 {
		got, trimmed := TrimPrefix(tests[i], tests[i+1])
		exp := tests[i+2]
		assert.Equal(t, exp, got)
		assert.Equal(t, trimmed, tests[i] != got, "%#v, %#v", tests[i], tests[i+1])
	}
}

func TestTrimExt(t *testing.T) {
	tests := []string{
		"foo", "foo",
		"foo.html", "foo",
		"foo.", "foo",
		"foo.html.txt", "foo.html",
	}

	n := len(tests)
	for i := 0; i < n; i += 2 {
		got := TrimExt(tests[i])
		exp := tests[i+1]
		assert.Equal(t, exp, got)
		assert.Equal(t, exp, got, "%#v, %#v", tests[i], tests[i+1])
	}
}

func TestCapitalize(t *testing.T) {
	tests := []struct {
		s   string
		exp string
	}{
		{
			s:   "foo",
			exp: "Foo",
		},
		{
			s:   "FOO",
			exp: "Foo",
		},
		{
			s:   "FOO baR",
			exp: "Foo bar",
		},
	}
	for _, test := range tests {
		got := Capitalize(test.s)
		assert.Equal(t, test.exp, got)
	}
}
