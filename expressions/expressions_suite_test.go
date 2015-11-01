package expressions_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestExpressions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Expressions Suite")
}
