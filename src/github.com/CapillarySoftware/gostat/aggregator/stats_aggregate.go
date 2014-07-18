// Package aggregator aggregates statistics
package aggregator

// StatsAggregate represents the computed aggregation of a collection of stats
type StatsAggregate struct {
	Average, Min, Max float64
	Count int
}