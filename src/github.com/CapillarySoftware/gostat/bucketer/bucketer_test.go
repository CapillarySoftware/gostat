package bucketer

import (
  "github.com/CapillarySoftware/gostat/stat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"fmt"
	"time"
)

var _ = Describe("Bucketer", func() {

	var stats         <-chan *stat.Stat
	var bucketedStats chan<- []*stat.Stat
	var shutdown      <-chan bool

	JustBeforeEach(func() {
		stats         = make(chan *stat.Stat)
		bucketedStats = make(chan []*stat.Stat)
		shutdown      = make(chan bool)
	})

	Describe("Construction", func() {
		It("should return a properly initialized Bucketer", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			// a newly constructed Bucketer has nothing in it's buckets
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

	Describe("Insert", func() {
		It("should return an error if a nil stat is inserted", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			Expect(x.insert(nil)).To(Equal(fmt.Errorf("dropping nil stat")))
		})

		It("should insert the stat in the current bucket if the Timestamp is after the current bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name][0]).To(Equal(&s))
			Expect(x.previousBuckets[s.Name]).To(BeNil())
		})

		It("should insert the stat in the current bucket if the Timestamp is the same as the current bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime, Value : 1}
			Expect(s.Timestamp).To(BeTemporally("==", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">",  x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name][0]).To(Equal(&s))
			Expect(x.previousBuckets[s.Name]).To(BeNil())
		})

		It("should insert the stat in the previous bucket if the Timestamp is less than the current bucket's min time and greater than the previous bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name][0]).To(Equal(&s))
		})

		It("should insert the stat in the previous bucket if the Timestamp is less than the current bucket's min time and equal to the previous bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.previousBucketMinTime, Value : 1}
			Expect(s.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("==", x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name][0]).To(Equal(&s))
		})

		It("should NOT insert the stat if the Timestamp is less than the previous bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.previousBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(Equal(fmt.Errorf("Bucketer: dropping stat older than %v: %+v", x.previousBucketMinTime, s)))
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
		})
	})	
})
