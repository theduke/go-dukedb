package dukedb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestGoDukedb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "GoDukedb Suite")
}
