package dukedb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	db "github.com/theduke/go-dukedb"
)

var _ = Describe("Models", func() {

	Describe("BaseModelIntId", func() {

		It("Should .SetStrId()", func() {
			m := db.IntIdModel{}
			err := m.SetStrId("22")
			Expect(err).ToNot(HaveOccurred())
			Expect(m.Id).To(Equal(uint64(22)))
		})

		It("Should error on SetId() with invalid arg", func() {
			m := db.IntIdModel{}
			err := m.SetId("xxx")
			Expect(err).To(HaveOccurred())
		})

		It("Should .GetStrId()", func() {
			m := db.IntIdModel{Id: uint64(22)}
			Expect(m.GetStrId()).To(Equal("22"))
		})

	})

})
