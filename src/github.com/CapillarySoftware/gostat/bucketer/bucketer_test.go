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


	Describe("construction", func() {
		It("should return a properly initialized Bucketer", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			// a newly constructed Bucketer has nothing in it's buckets
			Expect(len(x.currentBuckets)).To(Equal(0))
			Expect(len(x.previousBuckets)).To(Equal(0))
			Expect(len(x.futureBuckets)).To(Equal(0))

			// the current bucket min time is rounded down to the minute boundary
			// so it should not have any 'seconds' or 'nanoseconds' part
			Expect(x.currentBucketMinTime.Second()).To(Equal(0))
			Expect(x.currentBucketMinTime.Nanosecond()).To(Equal(0))

			// the previous bucket's min time is exactly one minute less than the current bucket's min time, and the future bucket is one min ahead
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))
			Expect(x.futureBucketMinTime.Sub(x.currentBucketMinTime)).To(Equal(time.Duration(time.Minute)))

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

		It("should NOT insert the stat if the Timestamp is > the max future bucket time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.futureBucketMinTime.Add(time.Duration(time.Minute)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(Equal(fmt.Errorf("Bucketer: dropping 'future' stat that is 'after' %v: %+v", x.futureBucketMinTime.Add(time.Nanosecond * (NaonsecondsPerMin - 1)), s)))
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})

		It("should insert the stat in the future bucket if the Timestamp is > the future bucket's min time and less than the max time for future stats", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.futureBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.futureBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.futureBucketMinTime.Add(time.Duration(time.Minute))))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(ConsistOf(&s))
		})

		It("should insert the stat in the future bucket if the Timestamp is equal to the future bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.futureBucketMinTime, Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("==", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(ConsistOf(&s))
		})

		It("should insert the stat in the current bucket if the Timestamp is after the current bucket's min time and less than the future bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(ConsistOf(&s))
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})

		It("should insert the stat in the current bucket if the Timestamp is equal to the current bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime, Value : 1}
			Expect(s.Timestamp).To(BeTemporally("==", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">",  x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(ConsistOf(&s))
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})

		It("should insert the stat in the previous bucket if the Timestamp is less than the current bucket's min time and greater than the previous bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(ConsistOf(&s))
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})

		It("should insert the stat in the previous bucket if the Timestamp is equal to the previous bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.previousBucketMinTime, Value : 1}
			Expect(s.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("==", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(ConsistOf(&s))
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})

		It("should NOT insert the stat if the Timestamp is less than the previous bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.previousBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.previousBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(Equal(fmt.Errorf("Bucketer: dropping stat older than %v: %+v", x.previousBucketMinTime, s)))
			
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})

		It("should insert the same stat in the current bucket TWICE we call insert() on the same stat twice and the Timestamp is after the current bucket's min time", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)

			s := stat.Stat{Name : "foo", Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(x.currentBuckets[s.Name]).To(BeNil())
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
			
			Expect(x.insert(&s)).To(BeNil())
			Expect(x.insert(&s)).To(BeNil())
			
			Expect(x.currentBuckets[s.Name]).To(ConsistOf(&s, &s))
			Expect(x.previousBuckets[s.Name]).To(BeNil())
			Expect(x.futureBuckets[s.Name]).To(BeNil())
		})
	})


	Describe("next", func() {
		It("should advance the current/previous/future bucket min times by one minute", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)
			
			// the previous bucket's min time is exactly one minute less than the current bucket's min time, and the future is one min ahead
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))
			Expect(x.futureBucketMinTime.Sub(x.currentBucketMinTime)).To(Equal(time.Duration(time.Minute)))

			t := x.currentBucketMinTime // save to compare
			x.next()
			Expect(x.currentBucketMinTime.Sub(t)).To(Equal(time.Duration(time.Minute)))		

			// the previous bucket's min time should STILL be exactly one minute less than the current bucket's min time, and the future is STILL one min ahead
			Expect(x.currentBucketMinTime.Sub(x.previousBucketMinTime)).To(Equal(time.Duration(time.Minute)))
			Expect(x.futureBucketMinTime.Sub(x.currentBucketMinTime)).To(Equal(time.Duration(time.Minute)))	
		})

		It("should not change the length of the current/previous/future buckets if they are all empty", func() {
			x := NewBucketer(stats, bucketedStats, shutdown)
			
			x.next()

			// all the buckets are still empty
			Expect(len(x.currentBuckets)).To(Equal(0))
			Expect(len(x.previousBuckets)).To(Equal(0))
			Expect(len(x.futureBuckets)).To(Equal(0))	
		})


		It("should advance the stats to their next buckets", func() {
			const STAT_NAME = "foo"
			x := NewBucketer(stats, bucketedStats, shutdown)
			
			// both the current, previous, and future buckets are initially empty
			Expect(len(x.currentBuckets)).To(Equal(0))
			Expect(len(x.previousBuckets)).To(Equal(0))	
			Expect(len(x.futureBuckets)).To(Equal(0))	

			// insert a "current" stat
			s1 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)), Value : 1}
			Expect(s1.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s1.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s1.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			
			Expect(x.insert(&s1)).To(BeNil())
			
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1))
			Expect(x.previousBuckets[STAT_NAME]).To(BeNil())
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())

			// insert a "previous" stat
			s2 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 2}
			Expect(s2.Timestamp).To(BeTemporally("<", x.currentBucketMinTime))
			Expect(s2.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s2.Timestamp).To(BeTemporally("<", x.futureBucketMinTime))
			
			Expect(x.insert(&s2)).To(BeNil())
			
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1))
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s2))
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())

			x.next()
			
			// after advancing, the current stats are empty, and what was once current is now previous
			Expect(x.currentBuckets[STAT_NAME]).To(BeNil())
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s1))
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())

			// insert a "future" stat
			s3 := stat.Stat{Name : STAT_NAME, Timestamp : x.futureBucketMinTime.Add(time.Duration(time.Second)), Value : 3}
			Expect(s3.Timestamp).To(BeTemporally(">", x.currentBucketMinTime))
			Expect(s3.Timestamp).To(BeTemporally(">", x.previousBucketMinTime))
			Expect(s3.Timestamp).To(BeTemporally(">", x.futureBucketMinTime))

			Expect(x.insert(&s3)).To(BeNil())

			Expect(x.currentBuckets[STAT_NAME]).To(BeNil())
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s1))
			Expect(x.futureBuckets[STAT_NAME]).To(ConsistOf(&s3))

			x.next()
			
			// after advancing, the future stats are empty, the previous stats are empty, and what was once the future is now 'current'
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s3))
			Expect(x.previousBuckets[STAT_NAME]).To(BeNil())
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())
		})
	})


	Describe("publish", func() {
		It("should publish the expected stats", func(done Done) {
			const STAT_NAME = "foo"
			output := make(chan []*stat.Stat)
			x := NewBucketer(stats, output, shutdown)

			// insert two "current" stats
			s1 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)),   Value : 1}
			s2 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second*2)), Value : 2}
			x.insert(&s1)
			x.insert(&s2)
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1, &s2))

			// start a goroutine to consume and verify the output
			go func(bucketed chan []*stat.Stat) {
				bucket := <-bucketed
				Expect(bucket).To(ConsistOf(&s1, &s2))
				close(done)
			}(output)

			x.publish(x.currentBuckets)
		})
	})


	Describe("pub", func() {
		It("should publish the expected current and previous stats", func(done Done) {
			const STAT_NAME = "foo"
			output := make(chan []*stat.Stat)
			x := NewBucketer(stats, output, shutdown)

			// insert two "current" stats
			s1 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)),   Value : 1}
			s2 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second*2)), Value : 2}
			x.insert(&s1)
			x.insert(&s2)
			
			// insert two previous stats
			s3 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -2)), Value : 3}
			s4 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 4}
			x.insert(&s3)
			x.insert(&s4)
			
			// verify the stats went to the appropriate buckets
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1, &s2))
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s3, &s4))
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())

			// start a goroutine to consume and verify the output
			go func(bucketed chan []*stat.Stat) {
				// the "current" bucket is published/received first
				bucket := <-bucketed
				Expect(bucket).To(ConsistOf(&s1, &s2))
				
				// the "previous" bucket is published/received second
				bucket = <-bucketed
				Expect(bucket).To(ConsistOf(&s3, &s4))
				close(done)
			}(output)

			x.pub()
		})

		It("should publish the expected current and previous stats before and after next() is invoked", func(done Done) {
			const STAT_NAME = "foo"
			output := make(chan []*stat.Stat)
			x := NewBucketer(stats, output, shutdown)

			// insert two "current" stats
			s1 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second)),   Value : 1}
			s2 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second*2)), Value : 2}
			x.insert(&s1)
			x.insert(&s2)
			
			// insert two previous stats
			s3 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -2)), Value : 3}
			s4 := stat.Stat{Name : STAT_NAME, Timestamp : x.currentBucketMinTime.Add(time.Duration(time.Second * -1)), Value : 4}
			x.insert(&s3)
			x.insert(&s4)
			
			// verify the stats went to the appropriate buckets
			Expect(x.currentBuckets[STAT_NAME]).To(ConsistOf(&s1, &s2))
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s3, &s4))
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())

			// start a goroutine to consume and verify the output
			go func(bucketed chan []*stat.Stat) {
				// the "current" bucket is published/received first
				bucket := <-bucketed
				Expect(bucket).To(ConsistOf(&s1, &s2))
				
				// the "previous" bucket is published/received second
				bucket = <-bucketed
				Expect(bucket).To(ConsistOf(&s3, &s4))

				// after next() was invoked, there were no new "current" buckets to publish, however
				// the "previous" bucket is published/received, because it had data
				bucket = <-bucketed
				Expect(bucket).To(ConsistOf(&s1, &s2))
				close(done)
			}(output)

			x.pub()
			x.next()
			x.pub()
			// verify the stats went to the appropriate buckets
			Expect(x.currentBuckets[STAT_NAME]).To(BeNil())
			Expect(x.previousBuckets[STAT_NAME]).To(ConsistOf(&s1, &s2))
			Expect(x.futureBuckets[STAT_NAME]).To(BeNil())
		})
	})	
})
