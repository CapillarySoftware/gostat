// Package aggregator aggregates statistics
package aggregator

import (
	"github.com/CapillarySoftware/gostat/stat"
	"math"
)

// Aggregate aggregates a collection of statistics, returning the average, min and max.
// Aggregate only examines the 'Value' property of a stat, and ignores all other properties (i.e. Name)
func Aggregate(stats []*stat.Stat) (aggregate StatsAggregate) {
	if stats == nil || len(stats) == 0 {
		return StatsAggregate{}
	}

	aggregate = StatsAggregate{Min : stats[0].Value, Max : stats[0].Value, Count : len(stats)}
	sum := 0.0
	for i := range stats {
		v := stats[i].Value

		sum += v

		if v < aggregate.Min {
			aggregate.Min = v
		}

		if v > aggregate.Max {
			aggregate.Max = v
		}
	}
	aggregate.Average = sum / float64(len(stats))

	return
}


// AppendStatsAggregate appends new StatsAggregate values to an existing one,
// safely computing new values in the process. This function enables aggregates
// to be combined without re-computing all the original values
func AppendStatsAggregate(a, b StatsAggregate) (aggregate StatsAggregate) {
	if (a.Count == 0) {
		return b
	}

	if (b.Count == 0) {
		return a
	}

	aggregate.Average = ( (a.Average * float64(a.Count)) + (b.Average * float64(b.Count)) ) / float64(a.Count + b.Count)
	aggregate.Min     = math.Min(a.Min, b.Min)
	aggregate.Max     = math.Max(a.Max, b.Max)
	aggregate.Count   = a.Count + b.Count
	return
}