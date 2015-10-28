package dukedb_test

import (
	db "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ExpressionTranslater", func() {

	Describe("SQL", func() {
		t := &db.SqlTranslator{}

		AfterEach(func() {
			t.Reset()
		})

		/**
		 * Sorting.
		 */

		It("Should translate ascending SortExpression", func() {
			sql := `"myfield" ASC`
			sort := db.Sort("myfield", true)
			Expect(t.Translate(sort)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate descending SortExpression", func() {
			sql := `"myfield" DESC`
			sort := db.Sort("myfield", false)
			Expect(t.Translate(sort)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Filtering.
		 */

		It("Should translate EQ FieldValueFilter", func() {
			sql := `"col"."myfield" = ?`
			expr := db.ValFilter("col", "myfield", "eq", 33)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate LT FieldFilter with MAX FunctionExpression", func() {
			sql := `"col"."myfield" < MAX("col2"."otherfield")`
			expr := db.FF("col", "myfield", "lt", db.Func("MAX", db.ColFieldIdentifier("col2", "otherfield")))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})
	})
})
