// Package aggregator aggregates statistics
package aggregator

import "github.com/CapillarySoftware/gostat/stat"

type StatsAggregator struct {
}

func (a StatsAggregator) Aggregate(stats []stat.Stat) (average, min, max float64) {
	if stats == nil || len(stats) == 0 {
		return
	}

	min = stats[0].Value
	max = stats[0].Value
	sum := 0.0

	for i := range stats {
		v := stats[i].Value
		
		sum += v

		if v < min {
			min = v
		}

		if v > max {
			max = v
		}
	}
	average = sum / float64(len(stats))

	return
}
