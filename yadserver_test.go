package yadserver

import (
	"testing"
)

func TestNormalizePath(t *testing.T) {
	testCase := map[string]string{
		"":        "",
		"a":       "a",
		"ab":      "ab",
		"/ab":     "ab",
		"a//":     "a/",
		"/ab//":   "ab/",
		"/ab///c": "ab/c",
		"//ab///c": "ab/c",
	}
	for k, v := range testCase {
		p := normalizePath(k)
		if p != v {
			t.Error(p, "should be", v, "(", k, ")")
		}
	}
}
