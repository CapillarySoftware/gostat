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

			Expect(len(x.CurrentBuckets)).To(Equal(0))
			Expect(len(x.PreviousBuckets)).To(Equal(0))
			Expect(x.CurrentBucketMinTime.Sub(x.PerviousBucketMinTime)).To(Equal(time.Duration(time.Minute)))
		})
	})
})
