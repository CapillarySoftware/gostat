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
	now := time.Now().UTC()

	return &StatBucketer {
		CurrentBucketMinTime  : now,
		CurrentBuckets        : make(map[string][]stat.Stat),
		PerviousBucketMinTime : now.Add(time.Minute * -1),
		PreviousBuckets       : make(map[string][]stat.Stat),
	}
}