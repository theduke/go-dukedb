package dukedb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/backends/tests"
)

var _ = Describe("Modelinfo", func() {
	Describe("ParseFieldTag", func() {
		It("Should parse primary_key", func() {
			info, _ := ParseFieldTag("primary-key;")
			Expect(info.PrimaryKey).To(Equal(true))

			info, _ = ParseFieldTag("primary-key")
			Expect(info.PrimaryKey).To(Equal(true))
		})

		It("Should parse ignore", func() {
			info, _ := ParseFieldTag("-")
			Expect(info.Ignore).To(Equal(true))
		})

		It("Should parse name", func() {
			info, _ := ParseFieldTag("name:the_name")
			Expect(info.BackendName).To(Equal("the_name"))
		})

		It("Should fail on invalid name", func() {
			_, err := ParseFieldTag("name")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("invalid_name"))
		})

		It("Should parse m2m", func() {
			info, _ := ParseFieldTag("m2m")
			Expect(info.M2M).To(Equal(true))
		})

		It("Should parse has-one", func() {
			info, _ := ParseFieldTag("has-one")
			Expect(info.HasOne).To(Equal(true))
		})

		It("Should parse explicit has-one", func() {
			info, _ := ParseFieldTag("has-one:field1:field2;")
			Expect(info.HasOne).To(Equal(true))
			Expect(info.HasOneField).To(Equal("field1"))
			Expect(info.HasOneForeignField).To(Equal("field2"))
		})

		It("Should fail on invalid has-one", func() {
			_, err := ParseFieldTag("has-one:field1")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("invalid_has_one"))
		})

		It("Should parse belongs-to", func() {
			info, _ := ParseFieldTag("belongs-to")
			Expect(info.BelongsTo).To(Equal(true))
		})

		It("Should parse explicit belongs-to", func() {
			info, _ := ParseFieldTag("belongs-to:field1:field2;")
			Expect(info.BelongsTo).To(Equal(true))
			Expect(info.BelongsToField).To(Equal("field1"))
			Expect(info.BelongsToForeignField).To(Equal("field2"))
		})

		It("Should fail on invalid belongs-to", func() {
			_, err := ParseFieldTag("belongs-to:field1")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("invalid_belongs_to"))
		})
	})

	Describe("ModelInfo", func() {

		Describe("CreateModelInfo", func() {

			It("Should fail on invalid tags", func() {
				type InvalidTagModel struct {
					TestModel
					InvalidField string `db:"has-one:xxx"`
				}

				_, err := CreateModelInfo(&InvalidTagModel{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("build_field_info_error"))
			})

			It("Should fail without primary key", func() {
				type NoPKModel struct {
					BaseModel
					SomeField string
				}
				_, err := CreateModelInfo(&NoPKModel{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("primary_key_not_found"))
			})

			It("Should determine ID field as primary key", func() {
				info, err := CreateModelInfo(&TestModel{})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.PkField).To(Equal("ID"))
			})

			It("Should determine explicit primary key field", func() {
				type PKModel struct {
					BaseModel
					Name string `db:"primary-key"`
				}

				info, err := CreateModelInfo(&PKModel{})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.PkField).To(Equal("Name"))
			})

			It("Should build info for test model successfully", func() {
				_, err := CreateModelInfo(&TestModel{})
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should build info for test parent model successfully", func() {
				_, err := CreateModelInfo(&TestParent{})
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Describe("ModelInfoMethods", func() {

			It("Should run .GetPkName() correctly", func() {
				type PKModel struct {
					BaseModel
					Name string `db:"primary-key;name:custom_name"`
				}

				info, err := CreateModelInfo(&PKModel{})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.FieldInfo[info.PkField].BackendName).To(Equal("custom_name"))
			})

			It("Should map field names correctly (.MapFieldName())", func() {
				type PKModel struct {
					BaseModel
					Name string `db:"primary-key;name:custom_name"`
				}

				info, err := CreateModelInfo(&PKModel{})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.MapFieldName("custom_name")).To(Equal("Name"))
			})
		})

	})

	Describe("Building of relationship info", func() {
		It("Builds relationship info without errors", func() {
			parent, _ := CreateModelInfo(&TestParent{})
			model, _ := CreateModelInfo(&TestModel{})

			modelInfo := map[string]*ModelInfo{
				"test_parents": parent,
				"test_models":  model,
			}
			Expect(BuildAllRelationInfo(modelInfo)).ToNot(HaveOccurred())
		})
	})

	Context("Correct relationship info", func() {
		var modelInfo map[string]*ModelInfo

		BeforeEach(func() {
			parent, _ := CreateModelInfo(&TestParent{})
			model, _ := CreateModelInfo(&TestModel{})

			modelInfo = map[string]*ModelInfo{
				"test_parents": parent,
				"test_models":  model,
			}
			BuildAllRelationInfo(modelInfo)
		})

		It("Finds inferred has-one", func() {
			parent := modelInfo["test_parents"]

			Expect(parent.FieldInfo["Child"].HasOne).To(Equal(true))
			Expect(parent.FieldInfo["Child"].RelationIsMany).To(Equal(false))
			Expect(parent.FieldInfo["Child"].HasOneField).To(Equal("ChildID"))
			Expect(parent.FieldInfo["Child"].HasOneForeignField).To(Equal("ID"))
		})

		It("Finds explicit has-one", func() {
			child := modelInfo["test_models"]

			Expect(child.FieldInfo["MyParent"].HasOne).To(Equal(true))
			Expect(child.FieldInfo["MyParent"].RelationIsMany).To(Equal(false))
			Expect(child.FieldInfo["MyParent"].HasOneField).To(Equal("MyParentID"))
			Expect(child.FieldInfo["MyParent"].HasOneForeignField).To(Equal("ID"))
		})

		It("Finds inferred belongs-to correctly on parent model", func() {
			BuildAllRelationInfo(modelInfo)

			parent := modelInfo["test_parents"]

			Expect(parent.FieldInfo["ChildSlice"].BelongsTo).To(Equal(true))
			Expect(parent.FieldInfo["ChildSlice"].RelationIsMany).To(Equal(true))
			Expect(parent.FieldInfo["ChildSlice"].BelongsToField).To(Equal("ID"))
			Expect(parent.FieldInfo["ChildSlice"].BelongsToForeignField).To(Equal("TestParentID"))
		})

		It("Finds explicit belongs-to correctly on parent model", func() {
			BuildAllRelationInfo(modelInfo)

			parent := modelInfo["test_parents"]

			Expect(parent.FieldInfo["ChildSlice2"].BelongsTo).To(Equal(true))
			Expect(parent.FieldInfo["ChildSlice2"].RelationIsMany).To(Equal(true))
			Expect(parent.FieldInfo["ChildSlice2"].BelongsToField).To(Equal("ID"))
			Expect(parent.FieldInfo["ChildSlice2"].BelongsToForeignField).To(Equal("MyParentID"))
		})

		It("Finds m2m on parent model", func() {
			BuildAllRelationInfo(modelInfo)

			parent := modelInfo["test_parents"]

			Expect(parent.FieldInfo["ChildSlicePtr"].M2M).To(Equal(true))
			Expect(parent.FieldInfo["ChildSlicePtr"].RelationIsMany).To(Equal(true))
		})

	})

	Context("Invalid relationship info", func() {
		// Todo: test invalid relationship info.
	})
})
