package dukedb_test

import (
	//db "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"
)

var _ = Describe("Queryparsers", func() {
	/*

		It("Shold parse simple condition correctly", func() {
			json := `{
				"collection": "col",
				"filters": {"name": "testname"}
			}
			`
			q, err := db.ParseJsonQuery([]byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetCollection()).To(Equal("col"))
			Expect(q.GetFilters()[0]).To(BeEquivalentTo(Eq("name", "testname")))
		})

		It("Shold parse comparison operator condition correctly", func() {
			json := `{
				"filters": {"intField": {"$gt": 20}}
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetCollection()).To(Equal("col"))
			Expect(q.GetFilters()[0]).To(BeEquivalentTo(Gt("intField", float64(20))))
		})

		It("Shold parse multiple conditions correctly", func() {
			json := `{
				"filters": {"name": "testname", "intField": {"$gt": 20}}
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())

			first := q.GetFilters()[0].(*FieldCondition)
			second := q.GetFilters()[1].(*FieldCondition)

			// The ordering by json.Unmarshal() is random, so swap the filters
			// if the order is reversed.
			if first.Field == "intField" {
				first, second = second, first
			}

			Expect(first).To(BeEquivalentTo(Eq("name", "testname")))
			Expect(second).To(BeEquivalentTo(Gt("intField", float64(20))))
		})

		It("Shold parse top level $or correctly", func() {
			json := `{
				"filters": {
					"$or": [{"name": "testname"}, {"intField": {"$lte": 100}}]
				}
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())

			// The ordering by json.Unmarshal() is random, so checking has to be done
			// with order in mind.
			or := q.GetFilters()[0].(*OrCondition)
			first := or.Filters[0].(*FieldCondition)
			second := or.Filters[1].(*FieldCondition)

			// The ordering by json.Unmarshal() is random, so swap the filters
			// if the order is reversed.
			if first.Field == "intField" {
				first, second = second, first
			}

			Expect(first).To(BeEquivalentTo(Eq("name", "testname")))
			Expect(second).To(BeEquivalentTo(Lte("intField", float64(100))))
		})

		It("Shold parse limit correctly", func() {
			json := `{
				"limit": 20
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetLimit()).To(Equal(20))
		})

		It("Shold parse offset correctly", func() {
			json := `{
				"offset": 20
			}
			`
			q, err := ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetOffset()).To(Equal(20))
		})

		It("Shold parse fields correctly", func() {
			json := `{
				"fields": ["field1", "field2"]
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetFields()).To(Equal([]string{"field1", "field2"}))
		})

		It("Shold parse joins", func() {
			json := `{
				"joins": ["Children"]
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetJoins()[0]).To(Equal(RelQ(q, "Children")))
		})

		It("Shold parse joined fields", func() {
			json := `{
				"joins": ["Children"],
				"fields": ["Children.field1", "Children.field2"]
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.GetJoins()[0].GetFields()).To(Equal([]string{"field1", "field2"}))
		})

		It("Shold parse nested joins", func() {
			json := `{
				"joins": ["Children", "Children.Tags"]
			}
			`
			q, err := db.ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())

			Expect(len(q.GetJoins())).To(Equal(1))
			Expect(q.GetJoins()[0].GetRelationName()).To(Equal("Children"))

			Expect(len(q.GetJoins()[0].GetJoins())).To(Equal(1))
			Expect(q.GetJoins()[0].GetJoins()[0].GetRelationName()).To(Equal("Tags"))
		})
	*/
})
