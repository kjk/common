package appendstore

import (
	"fmt"
	"testing"
)

var f = fmt.Sprintf

func TestRoundtrip(t *testing.T) {
	tests := [][]string{
		{"k1", "v1", "k2", "v2", "k1:v1 k2:v2"},
		{"k1", "", "k2", "v2", "k1: k2:v2"},
		{"k1", "v1", "k2", "", "k1:v1 k2:"},
		{"k1", "v1", "k2", "\n", "k1:v1 k2:\"\\n\""},
		{"k", "v l", `k:"v l"`},
		{"k1", "v1", "k2", "v2", "k3", "val3", "k1:v1 k2:v2 k3:val3"},
		{"key1", "value1", "key1:value1"},
		{"k", "", "k:"},
		{"k", "la\"ba\n", "k:\"la\\\"ba\\n\""},
		{"k", "f\n", "k:\"f\\n\""},
		{"k", "lo\\la", "k:lo\\la"},
	}
	for _, test := range tests {
		n := len(test)
		kv := test[:n-1]
		exp := test[n-1]
		got, err := KeyValueMarshal(kv...)
		assert(t, err == nil, f("KeyValueMarshal(%v) returned error: %v", kv, err))
		assert(t, got == exp, f("KeyValueMarshal(%v) = %q, want %q", kv, got, exp))

		got2, err := KeyValueUnmarshal(got)
		assert(t, err == nil, f("KeyValueUnmarshal(%q) returned error: %v", got, err))
		assert(t, len(got2) == len(kv), f("KeyValueUnmarshal(%q) returned %v, want %v", got, got2, kv))
		for i := 0; i < len(kv); i += 2 {
			assert(t, got2[i] == kv[i], f("KeyValueUnmarshal(%q) returned %v, want %v", got, got2, kv))
			assert(t, got2[i+1] == kv[i+1], f("KeyValueUnmarshal(%q) returned %v, want %v", got, got2, kv))
		}
	}
}

func TestInvalidMarshal(t *testing.T) {
	tests := [][]string{
		{"k1", "v1", "k2"}, // odd number of key-value pairs
		// keys cannot contain space, tab, newline, or ':'
		{"k ", "v"},
		{"k\n", "v"},
		{"k\t", "v"},
		{"k:", "v"},
		{"k:oh", "v"},
		{"k\"", "v"},
	}
	for _, test := range tests {
		got, err := KeyValueMarshal(test...)
		assert(t, err != nil, f("KeyValueMarshal(%v) should have returned error but got %v", test, got))
	}
}

func TestInvalidUnmarshal(t *testing.T) {
	tests := []string{
		"k1:v1 k2",            // missing value for k2
		"k1:v1 k2:v2 k3",      // missing value for k3
		"k1:v1 k2:v2 k3: v4",  // space before value is not ok
		"k1:v1 k2:v2 k3: v4 ", // space after value is not ok
		"k1: v1",              // space in key is not ok
		"k1:v1 k2: v2",        // space in value is not ok
	}
	for _, test := range tests {
		got, err := KeyValueUnmarshal(test)
		assert(t, err != nil, f("KeyValueUnmarshal(%q) should have returned error but got %v", test, got))
	}
}
