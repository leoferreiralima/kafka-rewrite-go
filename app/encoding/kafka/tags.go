package kafka

import (
	"errors"
	"strconv"
	"strings"
)

type tagOpts struct {
	order      int
	minVersion int
	compact    bool
}

var ErrMinVersionInvalid = errors.New("min version is invalid should be `kafka:\"orderNumberHere,minVersion=versionNumberHere\"` ")
var ErrOrderInvalid = errors.New("order is invalid should be `kafka:\"orderNumberHere\"` ")

func parseTag(tag string) (tagOpts tagOpts, err error) {
	opts := strings.Split(tag, ",")
	if tagOpts.order, err = strconv.Atoi(opts[0]); err != nil {
		return tagOpts, ErrOrderInvalid
	}

	for _, opt := range opts[1:] {
		name, value, found := strings.Cut(opt, "=")
		switch name {

		case "minVersion":
			if !found {
				return tagOpts, ErrMinVersionInvalid
			}
			if tagOpts.minVersion, err = strconv.Atoi(value); err != nil {
				return tagOpts, ErrMinVersionInvalid
			}
		case "compact":
			tagOpts.compact = true
		}

	}
	return tagOpts, nil
}
