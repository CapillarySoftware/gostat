package bucketer

import (
	"github.com/CapillarySoftware/gostat/stat"
	"time"
)

type Bucketer struct {
	currentBucketMinTime  time.Time
	currentBuckets        map[string][]stat.Stat
		
	previousBucketMinTime time.Time
	previousBuckets       map[string][]stat.Stat

	input                 <-chan *stat.Stat
	output                chan<- []stat.Stat
	shutdown              <-chan bool
}

//cs chan string
func NewBucketer(stats <-chan *stat.Stat, bucketedStats chan<- []stat.Stat, shutdown <-chan bool) *Bucketer {
	startOfCurrentMin := time.Now().UTC().Truncate(time.Minute) // "now", rounded down to the current min

	return &Bucketer {
		currentBucketMinTime  : startOfCurrentMin,
		currentBuckets        : make(map[string][]stat.Stat),
		previousBucketMinTime : startOfCurrentMin.Add(time.Minute * -1), // one minute behind
		previousBuckets       : make(map[string][]stat.Stat),

		input                 : stats,
		output                : bucketedStats,
		shutdown              : shutdown,
	}
}