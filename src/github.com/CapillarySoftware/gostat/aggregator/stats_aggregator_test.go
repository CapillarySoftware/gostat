package aggregator

import (
	"github.com/CapillarySoftware/gostat/stat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("StatsAggregator", func() {

	var sa StatsAggregator

	BeforeEach(func() {
		sa = StatsAggregator{}
	})

	Describe("Aggregate", func() {
		It("Should return all 0 values if a nil slice is received", func() {

			average, min, max := sa.Aggregate(nil)
			Expect(average).To(Equal(0.0))
			Expect(min).To(Equal(0.0))
			Expect(max).To(Equal(0.0))
		})

		It("Should return all 0 values if an empty slice is received", func() {

			average, min, max := sa.Aggregate([]stat.Stat{})
			Expect(average).To(Equal(0.0))
			Expect(min).To(Equal(0.0))
			Expect(max).To(Equal(0.0))
		})

	})

})
