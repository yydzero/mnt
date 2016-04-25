package libpq_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestLibpq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Libpq Suite")
}
