// Package aggregator aggregates statistics
package aggregator

import "github.com/CapillarySoftware/gostat/stat"

type Aggregator interface {
	
	// Aggregate aggregates a collection of statistics, returning the average, min and max.
	// Aggregate only examines the 'Value' property of a stat, and ignores all other properties (i.e. Name)
	Aggregate(stats []stat.Stat) (average, min, max float64)
}
