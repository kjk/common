package require

import "github.com/kjk/common/assert"

// this is a subset of github.com/stretchr/testify/require
// without dependencies and only the functions I use
// TODO: get rid of spew and difflib?

// TestingT is an interface wrapper around *testing.T
type TestingT interface {
	Errorf(format string, args ...any)
	FailNow()
}

// Len asserts that the specified object has specific length.
// Len also fails if the object has a type that len() not accept.
//
//    assert.Len(t, mySlice, 3)
func Len(t TestingT, object any, length int, msgAndArgs ...any) {
	if assert.Len(t, object, length, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// Nil asserts that the specified object is nil.
//
//    assert.Nil(t, err)
func Nil(t TestingT, object any, msgAndArgs ...any) {
	if assert.Nil(t, object, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// NoError asserts that a function returned no error (i.e. `nil`).
//
//   actualObj, err := SomeFunction()
//   if assert.NoError(t, err) {
// 	   assert.Equal(t, expectedObj, actualObj)
//   }
func NoError(t TestingT, err error, msgAndArgs ...any) {
	if assert.NoError(t, err, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// NotEmpty asserts that the specified object is NOT empty.  I.e. not nil, "", false, 0 or either
// a slice or a channel with len == 0.
//
//  if assert.NotEmpty(t, obj) {
//    assert.Equal(t, "two", obj[1])
//  }
func NotEmpty(t TestingT, object any, msgAndArgs ...any) {
	if assert.NotEmpty(t, object, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// Equal asserts that two objects are equal.
//
//    assert.Equal(t, 123, 123)
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses). Function equality
// cannot be determined and will always fail.
func Equal(t TestingT, expected any, actual any, msgAndArgs ...any) {
	if assert.Equal(t, expected, actual, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// NotEqual asserts that the specified values are NOT equal.
//
//    assert.NotEqual(t, obj1, obj2)
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses).
func NotEqual(t TestingT, expected any, actual any, msgAndArgs ...any) {
	if assert.NotEqual(t, expected, actual, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// NotNil asserts that the specified object is not nil.
//
//    assert.NotNil(t, err)
func NotNil(t TestingT, object any, msgAndArgs ...any) {
	if assert.NotNil(t, object, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// True asserts that the specified value is true.
//
//    assert.True(t, myBool)
func True(t TestingT, value bool, msgAndArgs ...any) {
	if assert.True(t, value, msgAndArgs...) {
		return
	}
	t.FailNow()
}

// False asserts that the specified value is false.
//
//    assert.False(t, myBool)
func False(t TestingT, value bool, msgAndArgs ...any) {
	if assert.False(t, value, msgAndArgs...) {
		return
	}
	t.FailNow()
}
