package dukedb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	db "github.com/theduke/go-dukedb"
)

var _ = Describe("Models", func() {

	Describe("BaseModelIntID", func() {

		It("Should .SetStrID()", func() {
			m := db.IntIDModel{}
			err := m.SetStrID("22")
			Expect(err).ToNot(HaveOccurred())
			Expect(m.ID).To(Equal(uint64(22)))
		})

		It("Should error on SetID() with invalid arg", func() {
			m := db.IntIDModel{}
			err := m.SetID("xxx")
			Expect(err).To(HaveOccurred())
		})

		It("Should .GetStrID()", func() {
			m := db.IntIDModel{ID: uint64(22)}
			Expect(m.GetStrID()).To(Equal("22"))
		})

	})

})
