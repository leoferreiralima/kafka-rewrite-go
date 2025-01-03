package kafka

import (
	"fmt"
	"reflect"
	"testing"
)

var testParseTagCases = []struct {
	tag      string
	expected tagOpts
}{
	{
		tag: "1",
		expected: tagOpts{
			order: 1,
		},
	},
	{
		tag: "1,compact",
		expected: tagOpts{
			order:   1,
			compact: true,
		},
	},
	{
		tag: "2,minVersion=1",
		expected: tagOpts{
			order:      2,
			minVersion: 1,
		},
	},
	{
		tag: "10000,minVersion=10000,compact",
		expected: tagOpts{
			order:      10000,
			minVersion: 10000,
			compact:    true,
		},
	},
}

func TestParseTag(t *testing.T) {
	for _, testCase := range testParseTagCases {
		result, err := parseTag(testCase.tag)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(testCase.expected, result) {
			t.Errorf("expeted: %s, result: %s", fmt.Sprint(testCase.expected), fmt.Sprint(result))
		}
	}
}

var testParseInvalidTagCase = []struct {
	tag string
	err error
}{
	{
		tag: "invalid",
		err: ErrOrderInvalid,
	},
	{
		tag: "invalid,minVersion=1",
		err: ErrOrderInvalid,
	},
	{
		tag: "0,minVersion",
		err: ErrMinVersionInvalid,
	},
	{
		tag: "0,minVersion=",
		err: ErrMinVersionInvalid,
	},
	{
		tag: "0,minVersion=invalid",
		err: ErrMinVersionInvalid,
	},
}

func TestParseInvalidTag(t *testing.T) {
	for _, testCase := range testParseInvalidTagCase {
		_, err := parseTag(testCase.tag)
		if err == nil {
			t.Errorf("should return error for tag %s", testCase.tag)
		}

		if err != testCase.err {
			t.Errorf("expected err: %s\nresult err:%s", testCase.err, err)
		}

	}
}
