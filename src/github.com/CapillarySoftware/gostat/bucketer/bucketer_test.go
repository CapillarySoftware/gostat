package bucketer

import (
  "github.com/CapillarySoftware/gostat/stat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Bucketer", func() {

	var stats         <-chan *stat.Stat
	var bucketedStats chan<- []stat.Stat
	var shutdown      <-chan bool

	JustBeforeEach(func() {
		stats         = make(chan *stat.Stat)
		bucketedStats = make(chan []stat.Stat)
		shutdown      = make(chan bool)
	})

	Describe("Construction", func() {
		It("should return a properly initialized Bucketer", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			// a newly constructed StatBucketer has nothing in it's buckets
			Expect(len(x.currentBuckets)).To(Equal(0))
			Expect(len(x.previousBuckets)).To(Equal(0))

			// the current bucket min time is rounded down to the minute boundary
			// so it should not have any 'seconds' or 'nanoseconds' part
			Expect(x.currentBucketMinTime.Second()).To(Equal(0))
			Expect(x.currentBucketMinTime.Nanosecond()).To(Equal(0))

			// the previous bucket's min time is exactly one minute less than the current bucket's min time
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))
		})
	})
})
