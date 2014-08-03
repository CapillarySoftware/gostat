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


	Describe("NewBucketer construction", func() {
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

			// verify the input channels
			Expect(x.input).NotTo(BeClosed())
			Expect(x.shutdown).NotTo(BeClosed())
		})
	})


	Describe("insert", func() {
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
			
			Expect(x.currentBuckets[s.Name]).To(ConsistOf(&s))
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
			
			Expect(x.currentBuckets[s.Name]).To(ConsistOf(&s))
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
			Expect(x.previousBuckets[s.Name]).To(ConsistOf(&s))
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
			Expect(x.previousBuckets[s.Name]).To(ConsistOf(&s))
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

		It("should insert the same stat in the current bucket TWICE we call insert() on the same stat twice and the Timestamp is after the current bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(ConsistOf(&s, &s))
			Expect(x.previousBuckets[s.Name]).To(BeNil())
		})
	})


	Describe("next", func() {
		It("should advance the current/previous bucket min times by one minute", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)
			
			// the previous bucket's min time is exactly one minute less than the current bucket's min time
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))

			t := x.currentBucketMinTime // save to compare
			x.next()
			Expect(x.currentBucketMinTime.Sub(t)).To(Equal(time.Duration(time.Minute)))			

			// the previous bucket's min time should STILL be exactly one minute less than the current bucket's min time
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))	
		})

		It("should not change the length of the current and previous buckets if they are both empty", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)
			
			// the previous bucket's min time is exactly one minute less than the current bucket's min time
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))

			t := x.currentBucketMinTime // save to compare
			x.next()
			Expect(x.currentBucketMinTime.Sub(t)).To(Equal(time.Duration(time.Minute)))			

			// the previous bucket's min time should STILL be exactly one minute less than the current bucket's min time
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))

			// both the current and previous buckets are still empty
			Expect(len(x.currentBuckets)).To(Equal(0))
			Expect(len(x.previousBuckets)).To(Equal(0))	
		})

		It("should discard the old previous stats and make the 'current' stats the 'previous' stats", func() {
			const STAT_NAME = "foo"
			x := NewBucketer(stats, bucketedStats, shutdown)
			
			// both the current and previous buckets are initially empty
			Expect(len(x.currentBuckets)).To(Equal(0))
			Expect(len(x.previousBuckets)).To(Equal(0))	

			// insert a "current" stat
			s1 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s1.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s1.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			
			Expect(x.insert(&s1)).To(BeNil())
			
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1))
			Expect(x.previousBuckets[STAT_NAME]).To(BeNil())

			// insert a "previous" stat
			s2 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 2}
			Expect(s2.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s2.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			
			Expect(x.insert(&s2)).To(BeNil())
			
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1))
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s2))

			x.next()
			
			// after advancing, the current stats are empty, and the previous stats have become current
			Expect(x.currentBuckets[STAT_NAME]).To(BeNil())
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s1))
		})
	})	
})
