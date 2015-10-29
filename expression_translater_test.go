package dukedb_test

import (
	. "github.com/theduke/go-dukedb"

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
			expr := TextExpr("my expression")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldTypeExpression", func() {
			sql := `varchar(255)`
			expr := FieldTypeExpr("varchar(255)", nil)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate ValueExpression", func() {
			sql := `?`
			expr := ValueExpr(44)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
			Expect(t.Arguments()).To(Equal([]interface{}{44}))
		})

		It("Should translate IdentifierExpression", func() {
			sql := `"identifier"`
			expr := IdExpr("identifier")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CollectionFieldIdentifierExpression", func() {
			sql := `"col"."field"`
			expr := ColFieldIdExpr("col", "field")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate NotNullConstraint", func() {
			sql := `NOT NULL`
			expr := Constr(CONSTRAINT_NOT_NULL)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate UniqueConstraint", func() {
			sql := `UNIQUE`
			expr := Constr(CONSTRAINT_UNIQUE)
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
			expr := Constr(CONSTRAINT_PRIMARY_KEY)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate AutoIncrementConstraint", func() {
			sql := `AUTO_INCREMENT`
			expr := Constr(CONSTRAINT_AUTO_INCREMENT)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DefaultValueConstraint", func() {
			sql := `DEFAULT ABS("field2")`
			expr := DefaultValConstr(FuncExpr("ABS", IdExpr("field2")))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldUpdateConstraint with CASCADE", func() {
			sql := `ON UPDATE CASCADE`
			expr := ActionConstr(EVENT_UPDATE, ACTION_CASCADE)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldDeleteConstraint with RESTRICT", func() {
			sql := `ON DELETE RESTRICT`
			expr := ActionConstr(EVENT_DELETE, ACTION_RESTRICT)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CheckConstraint", func() {
			sql := `CHECK ("field" < ?)`
			expr := CheckConstr(FieldFilter("", "field", OPERATOR_LT, ValueExpr(33)))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate ReferenceConstraint", func() {
			sql := `REFERENCES "col" ("field")`
			expr := ReferenceConstr("col", "field")
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldExpression", func() {
			sql := `"field" varchar(200) NOT NULL UNIQUE ON UPDATE CASCADE`
			constraints := []Expression{
				Constr(CONSTRAINT_NOT_NULL),
				Constr(CONSTRAINT_UNIQUE),
				ActionConstr(EVENT_UPDATE, ACTION_CASCADE),
			}
			expr := FieldExpr("field", FieldTypeExpr("varchar(200)", nil), constraints...)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FieldValueExpression", func() {
			sql := `"field" = ?`
			expr := FieldValExpr(IdExpr("field"), ValueExpr(33))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate FunctionExpression", func() {
			sql := `MAX("field")`
			expr := FuncExpr("MAX", IdExpr("field"))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Logical expressions.
		 */

		It("Should translate OrExpression", func() {
			sql := `("col"."field" = ? OR "col"."field" = "col2"."field")`
			expr := OrExpr(
				FieldValFilter("col", "field", "=", 44),
				FieldFilter("col", "field", "=", ColFieldIdExpr("col2", "field")),
			)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate AndExpression", func() {
			sql := `(? <= ? AND "col"."fieldx" = ?)`
			expr := AndExpr(
				FilterExpr(ValueExpr(22), "<=", ValueExpr(33)),
				FieldValFilter("col", "fieldx", "=", 66),
			)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Filtering.
		 */

		It("Should translate EQ FieldValueFilter", func() {
			sql := `"col"."myfield" = ?`
			expr := FieldValFilter("col", "myfield", "=", 33)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate LT FieldFilter with MAX FunctionExpression", func() {
			sql := `"col"."myfield" < MAX("col2"."otherfield")`
			expr := FieldFilter("col", "myfield", "<", FuncExpr("MAX", ColFieldIdExpr("col2", "otherfield")))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate NotExpression with nested OR", func() {
			sql := `NOT ("col"."field" = "field2" OR "col"."field3" >= ?)`
			expr := NotExpr(OrExpr(
				FieldFilter("col", "field", "=", IdExpr("field2")),
				FieldValFilter("col", "field3", ">=", 44),
			))
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate GTE Filter with MAX FieldExpression and subquery clause", func() {
			sql := `ABS("myfield") < (SELECT "field" FROM "table" LIMIT 1)`

			subQ := SelectStmt("table")
			subQ.SetLimit(1)
			subQ.AddField(IdExpr("field"))

			expr := FilterExpr(FuncExpr("ABS", IdExpr("myfield")), "<", subQ)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Sorting.
		 */

		It("Should translate ascending SortExpression", func() {
			sql := `"myfield" ASC`
			sort := SortExpr(IdExpr("myfield"), true)
			Expect(t.Translate(sort)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate descending SortExpression", func() {
			sql := `"myfield" DESC`
			sort := SortExpr(IdExpr("myfield"), false)
			Expect(t.Translate(sort)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		/**
		 * Statements.
		 */

		It("Should translate CreateCollectionStatement", func() {
			sql := `CREATE TABLE IF NOT EXISTS "col" ("f1" varchar(200), "f2" varchar(200), "f3" varchar(200) NOT NULL, UNIQUE("f2", "f3"))`

			fields := []FieldExpression{
				FieldExpr("f1", FieldTypeExpr("varchar(200)", nil)),
				FieldExpr("f2", FieldTypeExpr("varchar(200)", nil)),
				FieldExpr("f3", FieldTypeExpr("varchar(200)", nil), Constr(CONSTRAINT_NOT_NULL)),
			}
			constraints := []Expression{
				UniqueFieldsConstr(IdExpr("f2"), IdExpr("f3")),
			}
			expr := CreateColStmt("col", true, fields, constraints)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate RenameCollectionStatement", func() {
			sql := `ALTER TABLE "old_name" RENAME TO "new_name"`
			expr := RenameColStmt("old_name", "new_name")

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DropCollectionStatement", func() {
			sql := `DROP TABLE IF EXISTS "col" CASCADE`
			expr := DropColStmt("col", true, true)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CreateFieldStmt", func() {
			sql := `ALTER TABLE "col" ADD COLUMN "field" varchar(200) NOT NULL`
			expr := CreateFieldStmt(
				"col",
				FieldExpr("field", FieldTypeExpr("varchar(200)", nil),
					Constr(CONSTRAINT_NOT_NULL),
				),
			)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DropFieldStmt", func() {
			sql := `ALTER TABLE "col" DROP COLUMN IF EXISTS "field" CASCADE`
			expr := DropFieldStmt("col", "field", true, true)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate CreateIndexStatement", func() {
			sql := `CREATE UNIQUE INDEX "index" ON "col" USING btree (lower("field"), "field2")`
			expr := CreateIndexStmt(
				"index",
				IdExpr("col"),
				[]Expression{FuncExpr("lower", IdExpr("field")), IdExpr("field2")},
				true,
				"btree",
			)

			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate DropIndexStatement", func() {
			sql := `DROP INDEX IF EXISTS "index" CASCADE`
			expr := DropIndexStmt("index", true, true)
			Expect(t.Translate(expr)).ToNot(HaveOccurred())
			Expect(t.String()).To(Equal(sql))
		})

		It("Should translate SelectStatement", func() {
			sql := `SELECT "field1", "field2", "field3" AS "custom_name" FROM "col" WHERE ("field1" != ? AND "field2" = ?) ORDER BY "field1" ASC, "field2" DESC LIMIT 20 OFFSET 44`

			expr := SelectStmt("col")
			expr.AddField(IdExpr("field1"))
			expr.AddField(IdExpr("field2"))
			expr.AddField(NameExpr("custom_name", IdExpr("field3")))
			expr.FilterAnd(FieldValFilter("", "field1", "!=", 44))
			expr.FilterAnd(FieldValFilter("", "field2", "=", 22))
			expr.AddSort(SortExpr(IdExpr("field1"), true))
			expr.AddSort(SortExpr(IdExpr("field2"), false))
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
