// Package aggregator aggregates statistics
package aggregator

import "github.com/CapillarySoftware/gostat/stat"

type Aggregator interface {
	// Aggregate aggregates statistics, returning the average, min and max
	Aggregate(stats []stat.Stat) (average, min, max float64)
}
