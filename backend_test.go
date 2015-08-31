package dukedb_test

import (
	. "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend", func() {

	Describe("Backend implementations", func() {
		var _ Backend

		_ = func() {
			Expect(1).To(Equal(1))
		}	

	})
})
