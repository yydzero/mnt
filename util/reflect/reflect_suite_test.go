package reflect_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestReflect(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Reflect Suite")
}
