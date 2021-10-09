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
