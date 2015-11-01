package expressions_test

import (
	. "github.com/theduke/go-dukedb/expressions"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ExpressionTranslater", func() {

	Describe("SQL", func() {
		t := &SqlTranslator{}

		AfterEach(func() {
			t.Reset()
		})

		/**
		 * Generic expressions.
		 */

		It("Should translate TextExpression", func() {
			sql := `my expression`
			expr := NewTextExpr("my expression")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldTypeExpression", func() {
			sql := `varchar(255)`
			expr := NewFieldTypeExpr("varchar(255)", nil)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate ValueExpression", func() {
			sql := `?`
			expr := NewValueExpr(44)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
			Expect(t.Arguments()).To(Equal([]interface{}{44}))
		})

		It("Should translate IdentifierExpression", func() {
			sql := `"identifier"`
			expr := NewIdExpr("identifier")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CollectionFieldIdentifierExpression", func() {
			sql := `"col"."field"`
			expr := NewColFieldIdExpr("col", "field")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate NotNullConstraint", func() {
			sql := `NOT NULL`
			expr := NewConstraintExpr(CONSTRAINT_NOT_NULL)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate UniqueConstraint", func() {
			sql := `UNIQUE`
			expr := NewConstraintExpr(CONSTRAINT_UNIQUE)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/*
			It("Should translate UniqueFieldsConstraint", func() {
				sql := `UNIQUE("field1", "field3", "field5")`
				expr := &UniqueFieldsConstraint{
					Fields: []*IdentifierExpression{
						Identifier("field1"),
						Identifier("field3"),
						Identifier("field5"),
					},
				}
				Expect(t.Translate(expr)).ToNot(HaveOccurred())
				Expect(t.String()).To(Equal(sql))
			})
		*/

		It("Should translate PrimaryKeyConstraint", func() {
			sql := `PRIMARY KEY`
			expr := NewConstraintExpr(CONSTRAINT_PRIMARY_KEY)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate AutoIncrementConstraint", func() {
			sql := `AUTO_INCREMENT`
			expr := NewConstraintExpr(CONSTRAINT_AUTO_INCREMENT)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DefaultValueConstraint", func() {
			sql := `DEFAULT ABS("field2")`
			expr := NewDefaultValConstraint(NewFuncExpr("ABS", NewIdExpr("field2")))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldUpdateConstraint with CASCADE", func() {
			sql := `ON UPDATE CASCADE`
			expr := NewActionConstraint(EVENT_UPDATE, ACTION_CASCADE)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldDeleteConstraint with RESTRICT", func() {
			sql := `ON DELETE RESTRICT`
			expr := NewActionConstraint(EVENT_DELETE, ACTION_RESTRICT)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CheckConstraint", func() {
			sql := `CHECK ("field" < ?)`
			expr := NewCheckConstraint(NewFieldFilter("", "field", OPERATOR_LT, NewValueExpr(33)))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate ReferenceConstraint", func() {
			sql := `REFERENCES "col" ("field")`
			expr := NewReferenceConstraint("col", "field")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldExpression", func() {
			sql := `"field" varchar(200) NOT NULL UNIQUE ON UPDATE CASCADE`
			constraints := []Expression{
				NewConstraintExpr(CONSTRAINT_NOT_NULL),
				NewConstraintExpr(CONSTRAINT_UNIQUE),
				NewActionConstraint(EVENT_UPDATE, ACTION_CASCADE),
			}
			expr := NewFieldExpr("field", NewFieldTypeExpr("varchar(200)", nil), constraints...)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldValueExpression", func() {
			sql := `"field" = ?`
			expr := NewFieldValExpr(NewIdExpr("field"), NewValueExpr(33))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FunctionExpression", func() {
			sql := `MAX("field")`
			expr := NewFuncExpr("MAX", NewIdExpr("field"))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Logical expressions.
		 */

		It("Should translate OrExpression", func() {
			sql := `("col"."field" = ? OR "col"."field" = "col2"."field")`
			expr := NewOrExpr(
				NewFieldValFilter("col", "field", "=", 44),
				NewFieldFilter("col", "field", "=", NewColFieldIdExpr("col2", "field")),
			)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate AndExpression", func() {
			sql := `(? <= ? AND "col"."fieldx" = ?)`
			expr := NewAndExpr(
				NewFilter(NewValueExpr(22), "<=", NewValueExpr(33)),
				NewFieldValFilter("col", "fieldx", "=", 66),
			)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Filtering.
		 */

		It("Should translate EQ FieldValueFilter", func() {
			sql := `"col"."myfield" = ?`
			expr := NewFieldValFilter("col", "myfield", "=", 33)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate LT FieldFilter with MAX FunctionExpression", func() {
			sql := `"col"."myfield" < MAX("col2"."otherfield")`
			expr := NewFieldFilter("col", "myfield", "<", NewFuncExpr("MAX", NewColFieldIdExpr("col2", "otherfield")))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate NotExpression with nested OR", func() {
			sql := `NOT ("col"."field" = "field2" OR "col"."field3" >= ?)`
			expr := NewNotExpr(NewOrExpr(
				NewFieldFilter("col", "field", "=", NewIdExpr("field2")),
				NewFieldValFilter("col", "field3", ">=", 44),
			))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate GTE Filter with MAX FieldExpression and subquery clause", func() {
			sql := `ABS("myfield") < (SELECT "field" FROM "table" LIMIT 1)`

			subQ := NewSelectStmt("table")
			subQ.SetLimit(1)
			subQ.AddField(NewIdExpr("field"))

			expr := NewFilter(NewFuncExpr("ABS", NewIdExpr("myfield")), "<", subQ)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Sorting.
		 */

		It("Should translate ascending SortExpression", func() {
			sql := `"myfield" ASC`
			sort := NewSortExpr(NewIdExpr("myfield"), true)
			Expect(t.Translate(sort)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate descending SortExpression", func() {
			sql := `"myfield" DESC`
			sort := NewSortExpr(NewIdExpr("myfield"), false)
			Expect(t.Translate(sort)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Statements.
		 */

		It("Should translate CreateCollectionStatement", func() {
			sql := `CREATE TABLE IF NOT EXISTS "col" ("f1" varchar(200), "f2" varchar(200), "f3" varchar(200) NOT NULL, UNIQUE("f2", "f3"))`

			fields := []*FieldExpr{
				NewFieldExpr("f1", NewFieldTypeExpr("varchar(200)", nil)),
				NewFieldExpr("f2", NewFieldTypeExpr("varchar(200)", nil)),
				NewFieldExpr("f3", NewFieldTypeExpr("varchar(200)", nil), NewConstraintExpr(CONSTRAINT_NOT_NULL)),
			}
			constraints := []Expression{
				NewUniqueFieldsConstraint(NewIdExpr("f2"), NewIdExpr("f3")),
			}
			expr := NewCreateColStmt("col", true, fields, constraints)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate RenameCollectionStatement", func() {
			sql := `ALTER TABLE "old_name" RENAME TO "new_name"`
			expr := NewRenameColStmt("old_name", "new_name")

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DropCollectionStatement", func() {
			sql := `DROP TABLE IF EXISTS "col" CASCADE`
			expr := NewDropColStmt("col", true, true)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CreateFieldStmt", func() {
			sql := `ALTER TABLE "col" ADD COLUMN "field" varchar(200) NOT NULL`
			expr := NewCreateFieldStmt(
				"col",
				NewFieldExpr("field", NewFieldTypeExpr("varchar(200)", nil),
					NewConstraintExpr(CONSTRAINT_NOT_NULL),
				),
			)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DropFieldStmt", func() {
			sql := `ALTER TABLE "col" DROP COLUMN IF EXISTS "field" CASCADE`
			expr := NewDropFieldStmt("col", "field", true, true)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CreateIndexStatement", func() {
			sql := `CREATE UNIQUE INDEX "index" ON "col" USING btree (lower("field"), "field2")`
			expr := NewCreateIndexStmt(
				"index",
				NewIdExpr("col"),
				[]Expression{NewFuncExpr("lower", NewIdExpr("field")), NewIdExpr("field2")},
				true,
				"btree",
			)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DropIndexStatement", func() {
			sql := `DROP INDEX IF EXISTS "index" CASCADE`
			expr := NewDropIndexStmt("index", true, true)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate SelectStatement", func() {
			sql := `SELECT "field1", "field2", "field3" AS "custom_name" FROM "col" WHERE ("field1" != ? AND "field2" = ?) ORDER BY "field1" ASC, "field2" DESC LIMIT 20 OFFSET 44`

			expr := NewSelectStmt("col")
			expr.AddField(NewIdExpr("field1"))
			expr.AddField(NewIdExpr("field2"))
			expr.AddField(NameExpr("custom_name", NewIdExpr("field3")))
			expr.FilterAnd(NewFieldValFilter("", "field1", "!=", 44))
			expr.FilterAnd(NewFieldValFilter("", "field2", "=", 22))
			expr.AddSort(NewSortExpr(NewIdExpr("field1"), true))
			expr.AddSort(NewSortExpr(NewIdExpr("field2"), false))
			expr.SetLimit(20)
			expr.SetOffset(44)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

	})
})

/**
*
*
*MutationStatement
*CreateStatement
*UpdateStatement
 */
