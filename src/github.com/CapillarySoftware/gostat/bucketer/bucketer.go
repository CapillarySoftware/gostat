package bucketer

import (
	"github.com/CapillarySoftware/gostat/stat"
	"time"
	log "github.com/cihub/seelog"
)

type Bucketer struct {
	currentBucketMinTime  time.Time
	currentBuckets        map[string][]*stat.Stat
		
	previousBucketMinTime time.Time
	previousBuckets       map[string][]*stat.Stat

	input                 <-chan *stat.Stat   // Stats to be bucketed are read from this channel
	output                chan<- []*stat.Stat // 'buckets' of Stats are written to this channel
	shutdown              <-chan bool         // signals a graceful shutdown
}

// NewBucketer constructs a Bucketer
func NewBucketer(stats <-chan *stat.Stat, bucketedStats chan<- []*stat.Stat, shutdown <-chan bool) *Bucketer {
	startOfCurrentMin := time.Now().UTC().Truncate(time.Minute) // "now", rounded down to the current min

	return &Bucketer {
		currentBucketMinTime  : startOfCurrentMin,
		currentBuckets        : make(map[string][]*stat.Stat),
		previousBucketMinTime : startOfCurrentMin.Add(time.Minute * -1), // one minute behind
		previousBuckets       : make(map[string][]*stat.Stat),

		input                 : stats,
		output                : bucketedStats,
		shutdown              : shutdown,
	}
}


func (b *Bucketer) Run() {
	done := false

	for !done {
		select {
		case stat := <-b.input : log.Debugf("Bucketer got %+v", *stat)
		                         b.insert(stat)
		case done =  <-b.shutdown : break
		case         <-time.After(time.Second * 1) : log.Debug("Bucketer Run() timeout ", time.Now())
		}
	}

	log.Info("Bucketer Run() exiting ", time.Now())
}


// insert places the provided stat in the appropriate current or previous bucket.
// It returns an error if the stat could not be placed in a bucket
func (b *Bucketer) insert(s *stat.Stat) error {
	var buckets map[string][]*stat.Stat

	if s == nil {
		return log.Errorf("dropping nil stat")
	}

	if s.Timestamp.After(b.currentBucketMinTime)         ||  s.Timestamp.Equal(b.currentBucketMinTime) {
		buckets = b.currentBuckets
	} else if s.Timestamp.After(b.previousBucketMinTime) || s.Timestamp.Equal(b.previousBucketMinTime) {
		buckets = b.previousBuckets
	} else {
		return log.Warnf("Bucketer: dropping stat older than %v: %+v", b.previousBucketMinTime, *s)
	}

	stats := buckets[s.Name]
	stats = append(stats, s)
	buckets[s.Name] = stats
	return nil
}

// next advances to the next interval, updating the current/previous buckets and
// their associated times
func (b *Bucketer) next() {
	b.previousBucketMinTime = b.currentBucketMinTime
	b.currentBucketMinTime  = b.currentBucketMinTime.Add(time.Duration(time.Minute))

	b.previousBuckets = b.currentBuckets
	b.currentBuckets  = make(map[string][]*stat.Stat)
}