package memory_test

import (
	. "github.com/theduke/go-dukedb/backends/memory"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Memory", func() {

	var _ Backend


	It("SHould ok", func() {
		Expect(1).To(Equal(1))
	})

})
