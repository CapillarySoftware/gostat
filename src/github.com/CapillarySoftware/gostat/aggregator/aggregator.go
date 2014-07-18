// Package aggregator aggregates statistics
package aggregator

import "github.com/CapillarySoftware/gostat/stat"

type Aggregator interface {

	// Aggregate aggregates a collection of statistics, returning the average, min and max.
	// Aggregate only examines the 'Value' property of a stat, and ignores all other properties (i.e. Name)
	Aggregate(stats []stat.Stat) (StatsAggregate)

	// AppendStatsAggregate appends new StatsAggregate values to an existing one,
	// safely computing new values in the process. This function enables aggregates
	// to be combined without re-computing all the original values
	AppendStatsAggregate(a, b StatsAggregate) (StatsAggregate)
}
