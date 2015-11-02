package dukedb_test

import (
	"reflect"

	db "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/theduke/go-dukedb/backends/tests"
)

var _ = Describe("Utils", func() {
	Describe("db.GetModelSliceFieldValues", func() {
		var modelSlice []interface{}

		BeforeEach(func() {
			modelSlice = []interface{}{&TestModel{
				ID:     1,
				StrVal: "str1",
				IntVal: 1,
			}, &TestModel{
				ID:     2,
				StrVal: "str2",
				IntVal: 2,
			}}
		})

		It("Should error on invalid field", func() {
			_, err := db.GetModelSliceFieldValues(modelSlice, "InvalidField")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("field_not_found"))
		})

		It("Should work for str field", func() {
			val := []interface{}{"str1", "str2"}
			Expect(db.GetModelSliceFieldValues(modelSlice, "StrVal")).To(Equal(val))
		})

		It("Should work for int field", func() {
			val := []interface{}{int64(1), int64(2)}
			Expect(db.GetModelSliceFieldValues(modelSlice, "IntVal")).To(Equal(val))
		})
	})

	Describe("db.SetStructModelField", func() {

		var testParent *TestParent

		BeforeEach(func() {
			p := NewTestParent(1, false)
			testParent = &p
		})

		It("Should error on non-pointer", func() {
			err := db.SetStructModelField(22, "Child", []interface{}{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("pointer_expected"))
		})

		It("Should error on pointer to non-struct", func() {
			x := 22
			err := db.SetStructModelField(&x, "Child", []interface{}{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("pointer_to_struct_expected"))
		})

		It("Should error on unknown field", func() {
			err := db.SetStructModelField(testParent, "InvalidField", []interface{}{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("unknown_field"))
		})

		It("Should error on invalid target field type", func() {
			err := db.SetStructModelField(testParent, "StrVal", []interface{}{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("unsupported_field_type"))
		})

		It("Should set struct", func() {
			child := NewTestModel(1)
			db.SetStructModelField(testParent, "Child", []interface{}{&child})
			Expect(testParent.Child).To(Equal(child))
		})

		It("Should set struct pointer", func() {
			child := NewTestModel(1)
			db.SetStructModelField(testParent, "ChildPtr", []interface{}{&child})
			Expect(testParent.ChildPtr).To(Equal(&child))
		})

		It("Should set slice", func() {
			childSlice := NewTestModelSlice(1, 2)
			ifSlice := []interface{}{childSlice[0], childSlice[1]}

			err := db.SetStructModelField(testParent, "ChildSlice", ifSlice)
			Expect(err).ToNot(HaveOccurred())

			Expect(testParent.ChildSlice).To(Equal(childSlice))
		})

		It("Should set pointer slice", func() {
			childSlice := NewTestModelPtrSlice(1, 2)
			ifSlice := []interface{}{childSlice[0], childSlice[1]}
			db.SetStructModelField(testParent, "ChildSlicePtr", ifSlice)
			Expect(testParent.ChildSlicePtr).To(Equal(childSlice))
		})
	})
})
