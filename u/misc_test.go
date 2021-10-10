package u

import (
	"testing"

	"github.com/kjk/common/assert"
)

func TestSlug(t *testing.T) {
	tests := []string{
		"Laws of marketing #22 (resources) ",
		"laws-of-marketing-22-resources",

		"t  -_",
		"t-_",

		"foo.htML  ",
		"foo.html",
	}
	for i := 0; i < len(tests); i += 2 {
		got := Slug(tests[i])
		assert.Equal(t, tests[i+1], got)
	}
}

func TestSlugNoLowerCase(t *testing.T) {
	tests := []string{
		"Laws of marketing #22 (resources) ",
		"Laws-of-marketing-22-resources",

		"t  -_",
		"t-_",

		"foo.HTML  ",
		"foo.HTML",
	}
	for i := 0; i < len(tests); i += 2 {
		got := SlugNoLowerCase(tests[i])
		assert.Equal(t, tests[i+1], got)
	}
}
