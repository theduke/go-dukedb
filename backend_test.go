package dukedb_test

import (
	"fmt"

	_ "github.com/lib/pq"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"

	. "github.com/theduke/go-dukedb"
	sql "github.com/theduke/go-dukedb/backends/sql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Backend", func() {

	Describe("Backend implementations", func() {
		var backend Backend
		var transactionBackend TransactionBackend
		var connectionError DbError

		BeforeEach(func() {
			transactionBackend, _ = backend.(TransactionBackend)
		})

		testBackend := func() {
			It("Shoud have connected", func() {
				Expect(connectionError).ToNot(HaveOccurred())
				Expect(backend).ToNot(BeNil())
			})

			It("Should configure backend", func() {
				backend.SetDebug(true)
				backend.RegisterModel(&TestModel{})
				backend.RegisterModel(&TestParent{})
				backend.BuildRelationshipInfo()

				Expect(backend.GetDebug()).To(Equal(true))
			})

			It("Should drop all collections", func() {
				err := backend.DropAllCollections()
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should create collections", func() {
				err := backend.CreateCollection("test_models")
				Expect(err).ToNot(HaveOccurred())

				err = backend.CreateCollection("test_parents")
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
				Expect(testModel.GetID()).ToNot(Equal("0"))

				m, err := backend.FindOne("test_models", testModel.GetID())
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

				m, err := backend.FindOne("test_models", testModel.GetID())
				Expect(err).ToNot(HaveOccurred())
				Expect(m).To(Equal(&testModel))
			})

			It("Should delete", func() {
				// Persist model to update.
				testModel := NewTestModel(2)
				err := backend.Create(&testModel)
				Expect(err).ToNot(HaveOccurred())

				err = backend.Delete(&testModel)
				Expect(err).ToNot(HaveOccurred())

				m, err := backend.FindOne("test_models", testModel.GetID())
				Expect(err).ToNot(HaveOccurred())
				Expect(m).To(BeNil())
			})

			It("Should delete many", func() {
				m1 := NewTestModel(1)
				m2 := NewTestModel(2)
				m3 := NewTestModel(3)
				Expect(backend.Create(&m1)).ToNot(HaveOccurred())
				Expect(backend.Create(&m2)).ToNot(HaveOccurred())
				Expect(backend.Create(&m3)).ToNot(HaveOccurred())

				err := backend.Q("test_models").Delete()
				Expect(err).ToNot(HaveOccurred())
				Expect(backend.Q("test_models").Count()).To(Equal(0))
			})

			It("Should filter with field backend name", func() {
				model := NewTestModel(60)
				Expect(backend.Create(&model)).ToNot(HaveOccurred())
				
				m, err := backend.Q("test_models").Filter("int_val", 60).First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.GetID()).To(Equal(model.GetID()))
			})

			It("Should filter with struct field name", func() {
				model := NewTestModel(61)
				Expect(backend.Create(&model)).ToNot(HaveOccurred())
				
				m, err := backend.Q("test_models").Filter("IntVal", 61).First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.GetID()).To(Equal(model.GetID()))
			})

			It("Should filter with simple AND", func() {
				model := NewTestModel(63)
				Expect(backend.Create(&model)).ToNot(HaveOccurred())
				
				m, err := backend.Q("test_models").Filter("IntVal", 63).Filter("str_val", "str63").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.GetID()).To(Equal(model.GetID()))
			})

			// Relationships.
			It("Should auto-persist has-one", func() {
				model := NewTestParent(1, true)
				model.ChildPtr = nil
				model.ChildSlice = nil
				model.ChildSlicePtr = nil

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				m, err := backend.FindOne("test_parents", model.GetID())
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*TestParent).ChildID).To(Equal(model.Child.ID))
			})

			It("Should join has-one", func() {
				model := NewTestParent(2, true)
				model.ChildPtr = nil
				model.ChildSlice = nil
				model.ChildSlicePtr = nil

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				m, err := backend.Q("test_parents").Filter("id", model.GetID()).Join("Child").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*TestParent).Child.ID).To(Equal(model.Child.ID))
			})

			It("Should auto-persist single belongs-to", func() {
				model := NewTestParent(1, true)
				model.ChildSlice = nil
				model.ChildSlicePtr = nil

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				m, err := backend.FindOne("test_models", model.ChildPtr.GetID())
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*TestModel).TestParentID).To(Equal(model.ID))
			})

			It("Should join single belongs-to", func() {
				model := NewTestParent(2, true)
				model.ChildSlice = nil
				model.ChildSlicePtr = nil

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				m, err := backend.Q("test_parents").Filter("id", model.GetID()).Join("ChildPtr").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*TestParent).ChildPtr.ID).To(Equal(model.ChildPtr.ID))
			})

			It("Should auto-persist mutli belongs-to", func() {
				model := NewTestParent(1, true)
				model.ChildPtr = nil
				model.ChildSlicePtr = nil

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				models, err := backend.Q("test_models").Filter("test_parent_id", model.ID).Find()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(models)).To(Equal(2))
			})

			It("Should join multi belongs-to", func() {
				model := NewTestParent(2, true)
				model.ChildPtr = nil
				model.ChildSlicePtr = nil

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				m, err := backend.Q("test_parents").Filter("id", model.GetID()).Join("ChildSlice").First()
				Expect(err).ToNot(HaveOccurred())

				m2 := m.(*TestParent)

				Expect(len(m2.ChildSlice)).To(Equal(2))
			})

			It("Should auto-persist m2m", func() {
				model := NewTestParent(2, true)
				model.ChildPtr = nil
				model.ChildSlice = nil
				model.ChildSlicePtr = model.ChildSlicePtr[0:1]

				err := backend.Create(&model)
				Expect(err).ToNot(HaveOccurred())

				m2m, err := backend.M2M(&model, "ChildSlicePtr")	
				Expect(err).ToNot(HaveOccurred())
				Expect(m2m.Count()).To(Equal(1))
				Expect(m2m.All()[0].(*TestModel).ID).To(Equal(model.ChildSlicePtr[0].ID))
			})

			It("Should join m2m", func() {
				model := NewTestParent(2, true)
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

			It("Should successfully commit a transaction", func() {
				if transactionBackend == nil {
					Skip("Not a transaction backend")
				}

				tx := transactionBackend.Begin()
				fmt.Printf("\ntx backend: %+v\n", tx.(*sql.Backend).ModelInfo)
				Expect(tx).ToNot(BeNil())

				model := NewTestModel(100)
				Expect(tx.Create(&model)).ToNot(HaveOccurred())

				Expect(tx.Commit()).ToNot(HaveOccurred())

				m, err := backend.FindOne("test_models", model.GetID())
				Expect(err).ToNot(HaveOccurred())
				Expect(m).ToNot(BeNil())
			})

			It("Should successfully roll back a transaction", func() {
				if transactionBackend == nil {
					Skip("Not a transaction backend")
				}

				tx := transactionBackend.Begin()
				Expect(tx).ToNot(BeNil())

				model := NewTestModel(100)
				Expect(tx.Create(&model)).ToNot(HaveOccurred())

				Expect(tx.Rollback()).ToNot(HaveOccurred())

				m, err := backend.FindOne("test_models", model.GetID())
				Expect(err).ToNot(HaveOccurred())
				Expect(m).To(BeNil())
			})
		}

		Context("SQL backend", func() {
			Context("PostgreSQL", func() {
				backend, connectionError = sql.New("postgres", "postgres://test:test@localhost/test?sslmode=disable")
				testBackend()
			})

			Context("MySQL", func() {
				backend, connectionError = sql.New("mysql", "test:test@/test?charset=utf8&parseTime=True&loc=Local")
				testBackend()
			})

			/*
			Context("Sqlite3", func() {
				backend, connectionError = sql.New("sqlite3", ".test.sqlite3")
				testBackend()
				os.Remove(".test.sqlite3")
			})
			*/
		})
	})
})
