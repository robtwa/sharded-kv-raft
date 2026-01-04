package mr

import "log"

// Debugging
const Debug = false

func Printf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// An implementation of Interface can be sorted by the routines in this package.

// Used for sorting by key.
type SortByKey []KeyValue

// Len is the number of elements in the collection.
func (a SortByKey) Len() int { return len(a) }

// Less reports whether the element with index i
// must sort before the element with index j.
func (a SortByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Swap swaps the elements with indexes i and j.
func (a SortByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
