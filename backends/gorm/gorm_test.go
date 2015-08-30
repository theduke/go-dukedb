package gorm_test

import (
	. "github.com/theduke/go-dukedb/backends/gorm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gorm", func() {

	var _ Backend

	It("SHould ok", func() {
		x := 1
		Expect(x).To(Equal(1))
	})
})
