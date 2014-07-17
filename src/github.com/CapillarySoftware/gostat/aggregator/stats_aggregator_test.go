package aggregator

import (
	"github.com/CapillarySoftware/gostat/stat"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("StatsAggregator", func() {

	BeforeEach(func() {
	})

	Describe("Aggregate", func() {
		It("Should return all 0 values if a nil slice is received", func() {
			sa := StatsAggregator{}

			average, min, max := sa.Aggregate(nil)
			Expect(average).To(Equal(0.0))
			Expect(min).To(Equal(0.0))
			Expect(max).To(Equal(0.0))
		})

		It("Should return all 0 values if an empty slice is received", func() {
			sa := StatsAggregator{}

			average, min, max := sa.Aggregate([]stat.Stat{})
			Expect(average).To(Equal(0.0))
			Expect(min).To(Equal(0.0))
			Expect(max).To(Equal(0.0))
		})

	})

})
