package bucketer

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestBucketer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Bucketer Suite")
}
