//go:build race

package bundle

// raceBuild: the race detector inflates the heap, so memory bounds are only logged.
const raceBuild = true
