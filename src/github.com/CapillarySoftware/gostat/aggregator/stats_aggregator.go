// Package aggregator aggregates statistics
package aggregator

import "github.com/CapillarySoftware/gostat/stat"

type StatsAggregator struct {
}

func (a StatsAggregator) Aggregate(stats []stat.Stat) (average, min, max float64) {
	if stats == nil {
		return
	}

	return
}
