package siser

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

func TestMarshalLine(t *testing.T) {
	// Create a fixed timestamp for testing
	fixedTime := time.Date(2024, 1, 15, 10, 30, 45, 123000000, time.UTC)
	fixedTimeMs := strconv.FormatInt(TimeToUnixMillisecond(fixedTime), 10)

	// Test data
	data := []byte("test data")
	dataWithNewline := []byte("test data\n")

	tests := []struct {
		name     string
		dataName string
		t        time.Time
		d        []byte
		expected string
	}{
		{
			name:     "all fields present",
			dataName: "myrecord",
			t:        fixedTime,
			d:        data,
			expected: "--- 9 " + fixedTimeMs + " myrecord\ntest data\n",
		},
		{
			name:     "empty name",
			dataName: "",
			t:        fixedTime,
			d:        data,
			expected: "--- 9 " + fixedTimeMs + "\ntest data\n",
		},
		{
			name:     "zero time",
			dataName: "myrecord",
			t:        time.Time{},
			d:        data,
			expected: "--- 9 myrecord\ntest data\n",
		},
		{
			name:     "nil data",
			dataName: "myrecord",
			t:        fixedTime,
			d:        nil,
			expected: "--- 0 " + fixedTimeMs + " myrecord\n",
		},
		{
			name:     "empty data slice",
			dataName: "myrecord",
			t:        fixedTime,
			d:        []byte{},
			expected: "--- 0 " + fixedTimeMs + " myrecord\n",
		},
		{
			name:     "empty name and zero time",
			dataName: "",
			t:        time.Time{},
			d:        data,
			expected: "--- 9\ntest data\n",
		},
		{
			name:     "empty name and nil data",
			dataName: "",
			t:        fixedTime,
			d:        nil,
			expected: "--- 0 " + fixedTimeMs + "\n",
		},
		{
			name:     "zero time and nil data",
			dataName: "myrecord",
			t:        time.Time{},
			d:        nil,
			expected: "--- 0 myrecord\n",
		},
		{
			name:     "all optional fields empty",
			dataName: "",
			t:        time.Time{},
			d:        nil,
			expected: "--- 0\n",
		},
		{
			name:     "all optional fields empty with empty slice",
			dataName: "",
			t:        time.Time{},
			d:        []byte{},
			expected: "--- 0\n",
		},
		{
			name:     "data already has newline",
			dataName: "myrecord",
			t:        fixedTime,
			d:        dataWithNewline,
			expected: "--- 10 " + fixedTimeMs + " myrecord\ntest data\n",
		},
		{
			name:     "data already has newline, empty name",
			dataName: "",
			t:        fixedTime,
			d:        dataWithNewline,
			expected: "--- 10 " + fixedTimeMs + "\ntest data\n",
		},
		{
			name:     "data already has newline, zero time",
			dataName: "myrecord",
			t:        time.Time{},
			d:        dataWithNewline,
			expected: "--- 10 myrecord\ntest data\n",
		},
		{
			name:     "data already has newline, empty name and zero time",
			dataName: "",
			t:        time.Time{},
			d:        dataWithNewline,
			expected: "--- 10\ntest data\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			result := MarshalLine(tt.dataName, tt.t, tt.d, &buf)

			if string(result) != tt.expected {
				t.Errorf("MarshalLine() = %q, want %q", string(result), tt.expected)
			}

			// Verify the buffer was used (result should point to buffer's bytes)
			if !bytes.Equal(result, buf.Bytes()) {
				t.Errorf("MarshalLine() didn't use the provided buffer correctly")
			}
		})
	}
}

func TestMarshalLineNilBuffer(t *testing.T) {
	// Test that passing nil buffer still works
	fixedTime := time.Date(2024, 1, 15, 10, 30, 45, 123000000, time.UTC)
	fixedTimeMs := TimeToUnixMillisecond(fixedTime)
	data := []byte("test")

	result := MarshalLine("name", fixedTime, data, nil)
	expected := "--- 4 " + strconv.FormatInt(fixedTimeMs, 10) + " name\ntest\n"

	if string(result) != expected {
		t.Errorf("MarshalLine() with nil buffer = %q, want %q", string(result), expected)
	}
}

func TestMarshalLineNoTime(t *testing.T) {
	data := []byte("test data")

	tests := []struct {
		name     string
		dataName string
		d        []byte
		expected string
	}{
		{
			name:     "with name",
			dataName: "myrecord",
			d:        data,
			expected: "--- 9 myrecord\ntest data\n",
		},
		{
			name:     "empty name",
			dataName: "",
			d:        data,
			expected: "--- 9\ntest data\n",
		},
		{
			name:     "nil data",
			dataName: "myrecord",
			d:        nil,
			expected: "--- 0 myrecord\n",
		},
		{
			name:     "all empty",
			dataName: "",
			d:        nil,
			expected: "--- 0\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			result := MarshalLineNoTime(tt.dataName, tt.d, &buf)

			if string(result) != tt.expected {
				t.Errorf("MarshalLineNoTime() = %q, want %q", string(result), tt.expected)
			}
		})
	}
}
