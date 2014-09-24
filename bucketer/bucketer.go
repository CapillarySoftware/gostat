package bucketer

import (
	"github.com/CapillarySoftware/gostat/stat"
	log "github.com/cihub/seelog"
	"time"
)

const NaonsecondsPerMin time.Duration = 60000000000

type bucketMap map[string][]*stat.Stat

type Bucketer struct {
	currentBucketMinTime time.Time
	currentBuckets       bucketMap

	previousBucketMinTime time.Time
	previousBuckets       bucketMap

	futureBucketMinTime time.Time
	futureBuckets       bucketMap

	input    <-chan *stat.Stat   // Stats to be bucketed are read from this channel
	output   chan<- []*stat.Stat // 'buckets' of Stats are written to this channel
	shutdown <-chan bool         // signals a graceful shutdown
}

// NewBucketer constructs a Bucketer
func NewBucketer(stats <-chan *stat.Stat, bucketedStats chan<- []*stat.Stat, shutdown <-chan bool) *Bucketer {
	startOfCurrentMin := time.Now().UTC().Truncate(time.Minute) // "now", rounded down to the current min

	return &Bucketer{
		currentBucketMinTime: startOfCurrentMin,
		currentBuckets:       make(bucketMap),

		previousBucketMinTime: startOfCurrentMin.Add(time.Minute * -1), // one minute behind
		previousBuckets:       make(bucketMap),

		futureBucketMinTime: startOfCurrentMin.Add(time.Minute), // one minute ahead
		futureBuckets:       make(bucketMap),

		input:    stats,
		output:   bucketedStats,
		shutdown: shutdown,
	}
}

// Run is a goroutine that reads stats from the input channel, placing them into
// the appropriate bucket. Buckets are published on the output channel at the
// specified interval
func (b *Bucketer) Run(publishInterval time.Duration) {
	done := false

	publishTickChan := time.NewTicker(publishInterval)

	for !done {
		select {
		case stat := <-b.input:
			log.Debugf("Bucketer got %+v", *stat)
			b.insert(stat)
		case done = <-b.shutdown:
			log.Debug("Bucketer shutting down ", time.Now())
			// TODO: drain remaining stats
			publishTickChan.Stop()
			b.pub()
			break
		case <-publishTickChan.C:
			log.Debug("Bucketer publish interval elapsed ", time.Now())
			b.pub()
		case <-time.After(time.Second * 1):
			log.Debug("Bucketer Run() timeout ", time.Now())
		}

		if time.Now().UTC().After(b.futureBucketMinTime) {
			log.Debug("Bucketer advancing ", time.Now())
			b.next()
		}
	}

	log.Info("Bucketer Run() exiting ", time.Now())
}

// pub invokes publish() on the current and previous buckets
func (b *Bucketer) pub() {
	log.Debug("publishing current buckets")
	b.publish(b.currentBuckets)

	log.Debug("publishing previous buckets")
	b.publish(b.previousBuckets)
}

// publish sends a copy of each named stat in the specified bucketMap to the
// Bucketer's output channel
func (b *Bucketer) publish(buckets bucketMap) {
	for statName, bucket := range buckets {
		log.Debugf("publishing %d stats for bucket: %v", len(bucket), statName)

		clone := make([]*stat.Stat, len(bucket))
		for i := range bucket {
			clone[i] = bucket[i]
		}
		b.output <- clone
	}
}

// insert places the provided stat in the appropriate current, previous, or future bucket.
// It returns an error if the stat could not be placed in a bucket
func (b *Bucketer) insert(s *stat.Stat) error {
	var buckets bucketMap

	if s == nil {
		return log.Errorf("dropping nil stat")
	} else if s.Timestamp.After(b.futureBucketMinTime.Add(time.Nanosecond * (NaonsecondsPerMin - 1))) {
		// TODO: insert a "meta stat" representing a dropped future stat
		return log.Warnf("Bucketer: dropping 'future' stat that is 'after' %v: %+v", b.futureBucketMinTime.Add(time.Nanosecond*(NaonsecondsPerMin-1)), *s)
	}

	if s.Timestamp.After(b.futureBucketMinTime) || s.Timestamp.Equal(b.futureBucketMinTime) {
		buckets = b.futureBuckets
	} else if s.Timestamp.After(b.currentBucketMinTime) || s.Timestamp.Equal(b.currentBucketMinTime) {
		buckets = b.currentBuckets
	} else if s.Timestamp.After(b.previousBucketMinTime) || s.Timestamp.Equal(b.previousBucketMinTime) {
		buckets = b.previousBuckets
	} else {
		// TODO: insert a "meta stat" representing a dropped 'too old' stat
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
	b.currentBucketMinTime = b.futureBucketMinTime
	b.futureBucketMinTime = b.futureBucketMinTime.Add(time.Duration(time.Minute))

	b.previousBuckets = b.currentBuckets
	b.currentBuckets = b.futureBuckets
	b.futureBuckets = make(map[string][]*stat.Stat)
}
