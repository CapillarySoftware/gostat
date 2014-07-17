// Package aggregator aggregates statistics
package aggregator

import "github.com/CapillarySoftware/gostat/stat"

type StatsAggregator struct {
}

// Aggregate aggregates a collection of statistics, returning the average, min and max.
// Aggregate only examines the 'Value' property of a stat, and ignores all other properties (i.e. Name)
func (a StatsAggregator) Aggregate(stats []stat.Stat) (aggregate StatsAggregate) {
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
