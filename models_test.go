package dukedb_test

import (
	. "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Models", func() {

	Describe("BaseModelIntID", func() {

		It("Should set id", func() {
			m := BaseModelIntID{}
			err := m.SetID("22")
			Expect(err).ToNot(HaveOccurred())
			Expect(m.ID).To(Equal(uint64(22)))
		})

		It("Should error on SetID() with invalid arg", func() {
			m := BaseModelIntID{}
			err := m.SetID("xxx")
			Expect(err).To(HaveOccurred())
		})

		It("Should get id", func() {
			m := BaseModelIntID{ID: uint64(22)}
			Expect(m.GetID()).To(Equal("22"))
		})

	})

})
