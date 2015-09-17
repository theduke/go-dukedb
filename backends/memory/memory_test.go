package memory_test

import (
	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"

	"github.com/theduke/go-dukedb/backends/tests"
	. "github.com/theduke/go-dukedb/backends/memory"
)

var _ = Describe("Memory", func() {
	backend := New()
	tests.TestBackend(backend)
})
