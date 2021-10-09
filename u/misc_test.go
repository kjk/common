package u

import (
	"testing"

	"github.com/kjk/common/assert"
)

func TestUrlify(t *testing.T) {
	tests := []struct {
		s    string
		sExp string
	}{
		{
			s:    "Laws of marketing #22 (resources) ",
			sExp: "laws-of-marketing-22-resources",
		},
		{
			s:    "t  -_",
			sExp: "t-_",
		},
		{
			s:    "foo.htML  ",
			sExp: "foo.html",
		},
	}
	for _, test := range tests {
		sGot := Slug(test.s)
		assert.Equal(t, test.sExp, sGot)
	}
}
