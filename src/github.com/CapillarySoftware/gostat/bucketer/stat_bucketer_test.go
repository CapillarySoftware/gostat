package bucketer

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("StatBucketer", func() {

	Describe("Construction", func() {
		It("should return a properly initialized StatBucketer", func() {
			x := NewStatBucketer()

			// a newly constructed StatBucketer has nothing in it's buckets
			Expect(len(x.CurrentBuckets)).To(Equal(0))
			Expect(len(x.PreviousBuckets)).To(Equal(0))

			// the current bucket min time is rounded down to the minute boundary
			// so it should not have any 'seconds' or 'nanoseconds' part
			Expect(x.CurrentBucketMinTime.Second()).To(Equal(0))
			Expect(x.CurrentBucketMinTime.Nanosecond()).To(Equal(0))

			// the previous bucket's min time is exactly one minute less than the current bucket's min time
			Expect(x.CurrentBucketMinTime.Sub(x.PerviousBucketMinTime)).To(Equal(time.Duration(time.Minute)))
		})
	})
})
