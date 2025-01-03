package utils

type Comparable[T any] interface {
	Equals(other Comparable[T]) bool
}

func Contains[T comparable](arr []T, value T) bool {
	for _, v := range arr {
		if v == value {
			return true
		}
	}
	return false
}
