package bucketer

import (
	"github.com/CapillarySoftware/gostat/stat"
	"time"
)

type StatBucketer struct {
	CurrentBucketMinTime  time.Time
	CurrentBuckets        map[string][]stat.Stat
		
	PerviousBucketMinTime time.Time
	PreviousBuckets       map[string][]stat.Stat
}

func NewStatBucketer() *StatBucketer {
	startOfCurrentMin := time.Now().UTC().Truncate(time.Minute) // "now", rounded down to the current min

	return &StatBucketer {
		CurrentBucketMinTime  : startOfCurrentMin,
		CurrentBuckets        : make(map[string][]stat.Stat),
		PerviousBucketMinTime : startOfCurrentMin.Add(time.Minute * -1), // one minute behind
		PreviousBuckets       : make(map[string][]stat.Stat),
	}
}