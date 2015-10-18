package tests

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
)

var _ = fmt.Printf

func TestBackend(backend db.Backend) {
	It("Should configure backend", func() {
		backend.SetDebug(true)
		backend.RegisterModel(&TestModel{})
		backend.RegisterModel(&TestParent{})
		backend.RegisterModel(&HooksModel{})
		backend.RegisterModel(&ValidationsModel{})
		backend.BuildRelationshipInfo()

		Expect(backend.GetDebug()).To(Equal(true))
	})

	It("Should drop all collections", func() {
		err := backend.DropAllCollections()
		Expect(err).ToNot(HaveOccurred())
	})

	It("Should create collections", func() {
		err := backend.CreateCollections("test_models", "test_parents", "hooks_models", "validations_models")
		Expect(err).ToNot(HaveOccurred())
	})

	It("Should count with zero entries", func() {
		Expect(backend.Q("test_models").Count()).To(Equal(0))
	})

	It("Should insert and set ID, then FindOne()", func() {
		testModel := NewTestModel(1)
		testModel.ID = 0

		err := backend.Create(&testModel)
		Expect(err).ToNot(HaveOccurred())
		Expect(testModel.ID).ToNot(Equal(0))

		m, err := backend.FindOne("test_models", testModel.ID)
		Expect(err).ToNot(HaveOccurred())
		Expect(*m.(*TestModel)).To(Equal(testModel))
	})

	It("Should count with 1 entry", func() {
		Expect(backend.Q("test_models").Count()).To(Equal(1))
	})

	It("Should update", func() {
		// Persist model to update.
		testModel := NewTestModel(2)
		err := backend.Create(&testModel)
		Expect(err).ToNot(HaveOccurred())

		testModel.IntVal = 33
		testModel.StrVal = "str33"
		err = backend.Update(&testModel)
		Expect(err).ToNot(HaveOccurred())

		m, err := backend.FindOne("test_models", testModel.ID)
		Expect(err).ToNot(HaveOccurred())
		Expect(m).To(Equal(&testModel))
	})

	It("Should delete", func() {
		// Persist model to update.
		testModel := NewTestModel(3)
		err := backend.Create(&testModel)
		Expect(err).ToNot(HaveOccurred())

		err = backend.Delete(&testModel)
		Expect(err).ToNot(HaveOccurred())

		m, err := backend.FindOne("test_models", testModel.ID)
		Expect(err).ToNot(HaveOccurred())
		Expect(m).To(BeNil())
	})

	It("Should delete many", func() {
		m1 := NewTestModel(4)
		m2 := NewTestModel(5)
		m3 := NewTestModel(6)
		Expect(backend.Create(&m1)).ToNot(HaveOccurred())
		Expect(backend.Create(&m2)).ToNot(HaveOccurred())
		Expect(backend.Create(&m3)).ToNot(HaveOccurred())

		err := backend.Q("test_models").Delete()
		Expect(err).ToNot(HaveOccurred())
		Expect(backend.Q("test_models").Count()).To(Equal(0))
	})

	It("Should should work with marshalled fields", func() {

	})

	Describe("Marshalled fields", func() {
		type MarshalledData struct {
			IntVal    int
			StringVal string
		}

		type MarshalledModel struct {
			ID uint64

			MapVal       map[string]interface{} `db:"marshal"`
			StructVal    MarshalledData         `db:"marshal"`
			StructPtrVal *MarshalledData        `db:"marshal"`
		}

		It("Should create collection with marshalled fields", func() {
			backend.RegisterModel(&MarshalledModel{})
			backend.BuildRelationshipInfo()
			Expect(backend.CreateCollection("marshalled_models")).ToNot(HaveOccurred())
		})

		It("Should persist marshalled field with MAP and unmarshal on query", func() {
			data := map[string]interface{}{"key1": float64(22), "key2": "lala"}
			m := &MarshalledModel{
				MapVal: data,
			}

			Expect(backend.Create(m)).ToNot(HaveOccurred())

			rawModel, err := backend.FindOne("marshalled_models", m.ID)
			Expect(err).ToNot(HaveOccurred())

			Expect(rawModel.(*MarshalledModel).MapVal).To(Equal(data))
		})

		It("Should persist marshalled field with STRUCT and unmarshal on query", func() {
			data := MarshalledData{
				IntVal:    22,
				StringVal: "test",
			}
			m := &MarshalledModel{
				StructVal: data,
			}

			Expect(backend.Create(m)).ToNot(HaveOccurred())

			rawModel, err := backend.FindOne("marshalled_models", m.ID)
			Expect(err).ToNot(HaveOccurred())

			Expect(rawModel.(*MarshalledModel).StructVal).To(Equal(data))
		})

		It("Should persist marshalled field with STRUCT POINTER and unmarshal on query", func() {
			data := &MarshalledData{
				IntVal:    22,
				StringVal: "test",
			}
			m := &MarshalledModel{
				StructPtrVal: data,
			}

			Expect(backend.Create(m)).ToNot(HaveOccurred())

			rawModel, err := backend.FindOne("marshalled_models", m.ID)
			Expect(err).ToNot(HaveOccurred())

			Expect(rawModel.(*MarshalledModel).StructPtrVal).To(Equal(data))
		})
	})

	Describe("Querying", func() {
		It("Should filter with field backend name", func() {
			model := NewTestModel(60)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			m, err := backend.Q("test_models").Filter("int_val", 60).First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestModel).ID).To(Equal(model.ID))
		})

		It("Should filter with struct field name", func() {
			model := NewTestModel(61)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			m, err := backend.Q("test_models").Filter("IntVal", 61).First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestModel).ID).To(Equal(model.ID))
		})

		It("Should filter with simple AND", func() {
			model := NewTestModel(63)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			m, err := backend.Q("test_models").Filter("IntVal", 63).Filter("str_val", "str63").First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestModel).ID).To(Equal(model.ID))
		})

		It("Should .Query() with target slice", func() {
			model := NewTestModel(64)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			var models []TestModel

			_, err := backend.Query(db.Q("test_models").Filter("id", model.ID), &models)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(models)).To(Equal(1))
		})

		It("Should .Query() with target pointer slice", func() {
			model := NewTestModel(65)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			var models []*TestModel

			_, err := backend.Query(db.Q("test_models").Filter("id", model.ID), &models)
			Expect(err).ToNot(HaveOccurred())
			Expect(models[0]).To(Equal(&model))
		})

		It("Should .QueryOne()", func() {
			m := NewTestModel(1)
			m2 := NewTestModel(1)

			m.IntVal = 70
			m2.IntVal = 70
			Expect(backend.Create(&m)).ToNot(HaveOccurred())
			Expect(backend.Create(&m2)).ToNot(HaveOccurred())

			res, err := backend.QueryOne(db.Q("test_models").Filter("int_val", 70))
			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal(&m))
		})

		It("Should .QueryOne() with target", func() {
			m := NewTestModel(66)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel

			_, err := backend.QueryOne(db.Q("test_models").Filter("id", m.ID), &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .QueryOne() with target pointer", func() {
			m := NewTestModel(67)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel

			_, err := backend.QueryOne(db.Q("test_models").Filter("id", m.ID), &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .Last()", func() {
			m := NewTestModel(1)
			m2 := NewTestModel(1)

			m.IntVal = 71
			m2.IntVal = 71
			Expect(backend.Create(&m)).ToNot(HaveOccurred())
			Expect(backend.Create(&m2)).ToNot(HaveOccurred())

			res, err := backend.Last(db.Q("test_models").Filter("int_val", 71))
			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal(&m2))
		})

		It("Should .Last() with target model", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel
			_, err := backend.Last(db.Q("test_models").Filter("id", m.ID), &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .Last() with target pointer", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel
			_, err := backend.Last(db.Q("test_models").Filter("id", m.ID), &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindBy()", func() {
			m := NewTestModel(1)
			m2 := NewTestModel(1)

			m.IntVal = 72
			m2.IntVal = 72
			Expect(backend.Create(&m)).ToNot(HaveOccurred())
			Expect(backend.Create(&m2)).ToNot(HaveOccurred())

			res, err := backend.FindBy("test_models", "int_val", 72)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res)).To(Equal(2))
			Expect(res[0]).To(Equal(&m))
			Expect(res[1]).To(Equal(&m2))
		})

		It("Should .FindBy() with target slice", func() {
			m := NewTestModel(1)
			m2 := NewTestModel(1)

			m.IntVal = 73
			m2.IntVal = 73
			Expect(backend.Create(&m)).ToNot(HaveOccurred())
			Expect(backend.Create(&m2)).ToNot(HaveOccurred())

			var res []TestModel

			_, err := backend.FindBy("test_models", "int_val", 73, &res)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res)).To(Equal(2))

			Expect(res[0]).To(Equal(m))
			Expect(res[1]).To(Equal(m2))
		})

		It("Should .FindBy() with target slice pointer", func() {
			m := NewTestModel(1)
			m2 := NewTestModel(1)

			m.IntVal = 74
			m2.IntVal = 74
			Expect(backend.Create(&m)).ToNot(HaveOccurred())
			Expect(backend.Create(&m2)).ToNot(HaveOccurred())

			var res []*TestModel

			_, err := backend.FindBy("test_models", "int_val", 74, &res)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(res)).To(Equal(2))

			Expect(res[0]).To(Equal(&m))
			Expect(res[1]).To(Equal(&m2))
		})

		It("Should .FindOne()", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			model, err := backend.FindOne("test_models", m.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindOne() with target model", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel
			_, err := backend.FindOne("test_models", m.ID, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .FindOne() with target model pointer", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel
			_, err := backend.FindOne("test_models", m.ID, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindOneBy()", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			model, err := backend.FindOneBy("test_models", "id", m.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindOneBy() with target model", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel
			_, err := backend.FindOneBy("test_models", "id", m.ID, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .FindOneBy() with target model pointer", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel
			_, err := backend.FindOneBy("test_models", "id", m.ID, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})
	})

	Describe("Relationship handling", func() {
		It("Should auto-persist has-one", func() {
			model := NewTestParent(1, true)
			model.ChildPtr = nil
			model.ChildSlice = nil
			model.ChildSlicePtr = nil

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			m, err := backend.FindOne("test_parents", model.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestParent).ChildID).To(Equal(model.Child.ID))
		})

		It("Should join has-one", func() {
			model := NewTestParent(2, true)
			model.ChildPtr = nil
			model.ChildSlice = nil
			model.ChildSlicePtr = nil

			model.Child.ID = 0

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			m, err := backend.Q("test_parents").Filter("id", model.ID).Join("Child").First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestParent).Child.ID).To(Equal(model.Child.ID))
		})

		It("Should auto-persist single belongs-to", func() {
			model := NewTestParent(3, true)
			model.ChildSlice = nil
			model.ChildSlicePtr = nil

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			m, err := backend.FindOne("test_models", model.ChildPtr.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestModel).TestParentID).To(Equal(model.ID))
		})

		It("Should join single belongs-to", func() {
			model := NewTestParent(4, true)
			model.ChildSlice = nil
			model.ChildSlicePtr = nil

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			m, err := backend.Q("test_parents").Filter("id", model.ID).Join("ChildPtr").First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestParent).ChildPtr.ID).To(Equal(model.ChildPtr.ID))
		})

		It("Should auto-persist mutli belongs-to", func() {
			model := NewTestParent(5, true)
			model.ChildPtr = nil
			model.ChildSlicePtr = nil

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			models, err := backend.Q("test_models").Filter("test_parent_id", model.ID).Find()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(models)).To(Equal(2))
		})

		It("Should join multi belongs-to", func() {
			model := NewTestParent(6, true)
			model.ChildPtr = nil
			model.ChildSlicePtr = nil

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			m, err := backend.Q("test_parents").Filter("id", model.ID).Join("ChildSlice").First()
			Expect(err).ToNot(HaveOccurred())

			m2 := m.(*TestParent)

			Expect(len(m2.ChildSlice)).To(Equal(2))
		})

		It("Should auto-persist m2m", func() {
			model := NewTestParent(7, true)
			model.ChildPtr = nil
			model.ChildSlice = nil
			model.ChildSlicePtr = model.ChildSlicePtr[:1]

			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			m2m, err := backend.M2M(&model, "ChildSlicePtr")
			Expect(err).ToNot(HaveOccurred())
			Expect(m2m.Count()).To(Equal(1))
			Expect(m2m.All()[0].(*TestModel).ID).To(Equal(model.ChildSlicePtr[0].ID))
		})

		It("Should join m2m", func() {
			model := NewTestParent(8, true)
			model.ChildPtr = nil
			model.ChildSlice = nil
			model.ChildSlicePtr = model.ChildSlicePtr[0:1]

			err := backend.Create(&model)
			Expect(err).ToNot(HaveOccurred())

			result, err := backend.Q("test_parents").Filter("id", model.ID).Join("ChildSlicePtr").First()
			Expect(err).ToNot(HaveOccurred())

			m := result.(*TestParent)

			Expect(len(m.ChildSlicePtr)).To(Equal(1))
			Expect(m.ChildSlicePtr[0].ID).To(Equal(model.ChildSlicePtr[0].ID))
		})
	})

	Describe("Transactions", func() {
		transactionBackend, _ := backend.(db.TransactionBackend)

		It("Should successfully commit a transaction", func() {
			if transactionBackend == nil {
				Skip("Not a transaction backend")
			}

			tx := transactionBackend.Begin()
			Expect(tx).ToNot(BeNil())

			model := NewTestModel(100)
			Expect(tx.Create(&model)).ToNot(HaveOccurred())

			Expect(tx.Commit()).ToNot(HaveOccurred())

			m, err := backend.FindOne("test_models", model.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
		})

		It("Should successfully roll back a transaction", func() {
			if transactionBackend == nil {
				Skip("Not a transaction backend")
			}

			tx := transactionBackend.Begin()
			Expect(tx).ToNot(BeNil())

			model := NewTestModel(101)
			Expect(tx.Create(&model)).ToNot(HaveOccurred())

			Expect(tx.Rollback()).ToNot(HaveOccurred())

			m, err := backend.FindOne("test_models", model.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(m).To(BeNil())
		})
	})

	Describe("Hooks", func() {
		// Hooks tests.
		It("Should call before/afterCreate + Validate hooks", func() {
			m := &HooksModel{}
			Expect(backend.Create(m)).ToNot(HaveOccurred())
			Expect(m.CalledHooks).To(Equal([]string{"before_create", "validate", "after_create"}))
		})

		It("Should stop on error in BeforeCreate()", func() {
			m := &HooksModel{HookError: true}
			Expect(backend.Create(m)).To(Equal(&apperror.Err{Code: "before_create"}))
		})

		It("Should call before/afterUpdate hooks", func() {
			m := &HooksModel{}
			Expect(backend.Create(m)).ToNot(HaveOccurred())

			m.CalledHooks = nil

			Expect(backend.Update(m)).ToNot(HaveOccurred())
			Expect(m.CalledHooks).To(Equal([]string{"before_update", "validate", "after_update"}))
		})

		It("Should stop on error in BeforeUpdate()", func() {
			m := &HooksModel{}
			Expect(backend.Create(m)).ToNot(HaveOccurred())
			m.HookError = true
			Expect(backend.Update(m)).To(Equal(&apperror.Err{Code: "before_update"}))
		})

		It("Should call before/afterDelete hooks", func() {
			m := &HooksModel{}
			Expect(backend.Create(m)).ToNot(HaveOccurred())

			m.CalledHooks = nil

			Expect(backend.Delete(m)).ToNot(HaveOccurred())
			Expect(m.CalledHooks).To(Equal([]string{"before_delete", "after_delete"}))
		})

		It("Should stop on error in BeforeDelete()", func() {
			m := &HooksModel{}
			Expect(backend.Create(m)).ToNot(HaveOccurred())
			m.HookError = true
			Expect(backend.Delete(m)).To(Equal(&apperror.Err{Code: "before_delete"}))
		})

		It("Should call AfterQuery hook", func() {
			m := &HooksModel{}
			Expect(backend.Create(m)).ToNot(HaveOccurred())
			m.CalledHooks = nil

			m2, err := backend.FindOne("hooks_models", m.ID)
			Expect(err).ToNot(HaveOccurred())
			Expect(m2.(*HooksModel).CalledHooks).To(Equal([]string{"after_query"}))
		})
	})

	Describe("Model validations", func() {

		It("Should fail on empty not-null string", func() {
			m := &ValidationsModel{
				NotNullInt:      1,
				ValidatedString: "123456",
				ValidatedInt:    6,

				NotNullString: "",
			}

			err := backend.Create(m)
			Expect(err).To(HaveOccurred())
			Expect(err.(apperror.Error).GetCode()).To(Equal("empty_required_field"))
		})

		It("Should fail on minimum restraint string", func() {
			m := &ValidationsModel{
				NotNullString: "x",
				ValidatedInt:  6,
				NotNullInt:    1,

				ValidatedString: "t",
			}

			err := backend.Create(m)
			Expect(err).To(HaveOccurred())
			Expect(err.(apperror.Error).GetCode()).To(Equal("shorter_than_min_length"))
		})

		It("Should fail on maximum restraint string", func() {
			m := &ValidationsModel{
				NotNullString: "x",
				ValidatedInt:  6,
				NotNullInt:    1,

				ValidatedString: "tttttttttttt",
			}

			err := backend.Create(m)
			Expect(err).To(HaveOccurred())
			Expect(err.(apperror.Error).GetCode()).To(Equal("longer_than_max_length"))
		})

		It("Should fail on minimum restraint int", func() {
			m := &ValidationsModel{
				NotNullString:   "x",
				NotNullInt:      1,
				ValidatedString: "tttttt",

				ValidatedInt: 1,
			}

			err := backend.Create(m)
			Expect(err).To(HaveOccurred())
			Expect(err.(apperror.Error).GetCode()).To(Equal("shorter_than_min_length"))
		})

		It("Should fail on maximum restraint int", func() {
			m := &ValidationsModel{
				NotNullString:   "x",
				NotNullInt:      1,
				ValidatedString: "tttttt",

				ValidatedInt: 11,
			}

			err := backend.Create(m)
			Expect(err).To(HaveOccurred())
			Expect(err.(apperror.Error).GetCode()).To(Equal("longer_than_max_length"))
		})

		It("Should create correctly within restraints", func() {
			m := &ValidationsModel{
				NotNullString:   "x",
				NotNullInt:      1,
				ValidatedString: "tttttttttt",
				ValidatedInt:    5,
			}

			err := backend.Create(m)
			Expect(err).ToNot(HaveOccurred())
		})
	})

}
