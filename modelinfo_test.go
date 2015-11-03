package dukedb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/theduke/go-apperror"
	. "github.com/theduke/go-dukedb"
	//. "github.com/theduke/go-dukedb/backends/tests"
)

func buildInfo(models ...interface{}) (ModelInfos, apperror.Error) {
	infos := make(ModelInfos)

	for _, m := range models {
		info, err := BuildModelInfo(m)
		if err != nil {
			return nil, err
		}
		infos.Add(info)
	}

	if err := infos.AnalyzeRelations(); err != nil {
		return nil, err
	}

	return infos, nil
}

var _ = Describe("Modelinfo", func() {

	Describe("Relationships", func() {

		Describe("Automatic has-one", func() {
			It("Should detect has-one relationship with same-name + 'Id' automatically", func() {
				type Parent struct {
					Id uint64

					HasOne   *Parent
					HasOneId uint64
				}

				infos, err := buildInfo(&Parent{})
				Expect(err).ToNot(HaveOccurred())

				rel := infos.Get("parents").Relation("HasOne")
				Expect(rel).ToNot(BeNil())

				Expect(rel.RelationType()).To(Equal(RELATION_TYPE_HAS_ONE))
				Expect(rel.LocalField()).To(Equal("HasOneId"))
				Expect(rel.ForeignField()).To(Equal("Id"))
			})

			It("Should detect has-one relationship with same-name + 'ID' automatically", func() {
				type Child struct{ ID uint64 }
				type Parent struct {
					Id uint64

					HasOne   Child
					HasOneID uint64
				}

				infos, err := buildInfo(&Parent{}, &Child{})
				Expect(err).ToNot(HaveOccurred())

				rel := infos.Get("parents").Relation("HasOne")
				Expect(rel).ToNot(BeNil())

				Expect(rel.RelationType()).To(Equal(RELATION_TYPE_HAS_ONE))
				Expect(rel.LocalField()).To(Equal("HasOneID"))
				Expect(rel.ForeignField()).To(Equal("ID"))
			})

			It("Should error out when missing has-one key field", func() {
				type Parent struct {
					Id     uint64
					HasOne *Parent
				}

				_, err := buildInfo(&Parent{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("relationship_not_determined"))
			})
		})

		Describe("Custom has-one", func() {
			It("Should detect has-one relationship with custom tag", func() {
				type Parent struct {
					Id uint64

					OtherId uint64

					HasOne   *Parent `db:"has-one:CustomId:OtherId"`
					CustomId uint64
				}

				infos, err := buildInfo(&Parent{})
				Expect(err).ToNot(HaveOccurred())

				rel := infos.Get("parents").Relation("HasOne")
				Expect(rel).ToNot(BeNil())

				Expect(rel.RelationType()).To(Equal(RELATION_TYPE_HAS_ONE))
				Expect(rel.LocalField()).To(Equal("CustomId"))
				Expect(rel.ForeignField()).To(Equal("OtherId"))
			})

			It("Should error out on missing local field", func() {
				type Parent struct {
					Id     uint64
					HasOne *Parent `db:"has-one:Missing:OtherId"`
				}

				_, err := buildInfo(&Parent{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("invalid_relation_local_field"))
			})

			It("Should error out on missing foreign field", func() {
				type Parent struct {
					Id       uint64
					HasOne   *Parent `db:"has-one:HasOneId:Missing"`
					HasOneId uint64
				}

				_, err := buildInfo(&Parent{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("invalid_relation_foreign_field"))
			})
		})
	})
})
