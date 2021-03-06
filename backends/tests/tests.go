package tests

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
)

var _ = fmt.Printf

func TestBackend(skipFlag *bool, backendBuilder func() (db.Backend, apperror.Error)) {
	doSkip := false
	var backend db.Backend

	BeforeEach(func() {
		if *skipFlag || doSkip {
			Skip("Skipping due to previous error.")
		}

		var err apperror.Error
		backend, err = backendBuilder()
		Expect(err).ToNot(HaveOccurred())

		backend.SetDebug(true)

		backend.RegisterModel(&Tag{})
		backend.RegisterModel(&Project{})
		backend.RegisterModel(&Task{})
		backend.RegisterModel(&File{})

		backend.RegisterModel(&TestModel{})
		backend.RegisterModel(&TestParent{})
		backend.RegisterModel(&HooksModel{})
		backend.RegisterModel(&ValidationsModel{})
		backend.RegisterModel(&MarshalledModel{})
		backend.Build()
	})

	It("Should drop all collections", func() {
		doSkip = true
		err := backend.DropAllCollections()
		Expect(err).ToNot(HaveOccurred())
		doSkip = false
	})

	It("Should create collections", func() {
		doSkip = true
		err := backend.CreateCollection(
			"test_models",
			"test_parents",
			"hooks_models",
			"validations_models",
			"marshalled_models",
			"tags",
			"projects",
			"tasks",
			"files",
		)
		Expect(err).ToNot(HaveOccurred())
		doSkip = false
	})

	It("Should count with zero entries", func() {
		Expect(backend.Q("test_models").Count()).To(Equal(0))
	})

	It("Should insert and set Id, then FindOne()", func() {
		testModel := NewTestModel(1)
		testModel.Id = 0

		err := backend.Create(&testModel)
		Expect(err).ToNot(HaveOccurred())
		Expect(testModel.Id).ToNot(Equal(0))

		m, err := backend.FindOne("test_models", testModel.Id)
		Expect(err).ToNot(HaveOccurred())
		Expect(*m.(*TestModel)).To(Equal(testModel))
	})

	It("Should count with 1 entry", func() {
		Expect(backend.Create(&Project{Name: "Test"})).ToNot(HaveOccurred())
		Expect(backend.Q("projects").Count()).To(Equal(1))
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

		m, err := backend.FindOne("test_models", testModel.Id)
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

		m, err := backend.FindOne("test_models", testModel.Id)
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
		BeforeEach(func() {
			Expect(backend.Q("marshalled_models").Delete()).ToNot(HaveOccurred())
		})

		It("Should persist marshalled field with MAP and unmarshal on query", func() {
			data := map[string]interface{}{"key1": float64(22), "key2": "lala"}
			m := &MarshalledModel{
				MapVal: data,
			}

			Expect(backend.Create(m)).ToNot(HaveOccurred())

			rawModel, err := backend.FindOne("marshalled_models", m.Id)
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

			rawModel, err := backend.FindOne("marshalled_models", m.Id)
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

			rawModel, err := backend.FindOne("marshalled_models", m.Id)
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
			Expect(m.(*TestModel).Id).To(Equal(model.Id))
		})

		It("Should filter with struct field name", func() {
			model := NewTestModel(61)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			m, err := backend.Q("test_models").Filter("IntVal", 61).First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestModel).Id).To(Equal(model.Id))
		})

		It("Should filter with simple AND", func() {
			model := NewTestModel(63)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			m, err := backend.Q("test_models").Filter("IntVal", 63).Filter("str_val", "str63").First()
			Expect(err).ToNot(HaveOccurred())
			Expect(m.(*TestModel).Id).To(Equal(model.Id))
		})

		It("Should .Query() with target slice", func() {
			model := NewTestModel(64)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			var models []TestModel

			_, err := backend.Query(backend.Q("test_models").Filter("id", model.Id), &models)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(models)).To(Equal(1))
		})

		It("Should .Query() with target pointer slice", func() {
			model := NewTestModel(65)
			Expect(backend.Create(&model)).ToNot(HaveOccurred())

			var models []*TestModel

			_, err := backend.Query(backend.Q("test_models").Filter("id", model.Id), &models)
			Expect(err).ToNot(HaveOccurred())
			Expect(models[0]).To(Equal(&model))
		})

		It("Should .QueryOne()", func() {
			m := NewTestModel(1)
			m2 := NewTestModel(1)

			m.IntVal = 70
			m2.IntVal = 71
			Expect(backend.Create(&m)).ToNot(HaveOccurred())
			Expect(backend.Create(&m2)).ToNot(HaveOccurred())

			res, err := backend.QueryOne(backend.Q("test_models").Filter("int_val", 70))
			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal(&m))
		})

		It("Should .QueryOne() with target", func() {
			m := NewTestModel(66)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel

			_, err := backend.QueryOne(backend.Q("test_models").Filter("id", m.Id), &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .QueryOne() with target pointer", func() {
			m := NewTestModel(67)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel

			_, err := backend.QueryOne(backend.Q("test_models").Filter("id", m.Id), &model)
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

			res, err := backend.Last(backend.Q("test_models").Filter("int_val", 71))
			Expect(err).ToNot(HaveOccurred())
			Expect(res).To(Equal(&m2))
		})

		It("Should .Last() with target model", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel
			_, err := backend.Last(backend.Q("test_models").Filter("id", m.Id), &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .Last() with target pointer", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel
			_, err := backend.Last(backend.Q("test_models").Filter("id", m.Id), &model)
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

			model, err := backend.FindOne("test_models", m.Id)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindOne() with target model", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel
			_, err := backend.FindOne("test_models", m.Id, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .FindOne() with target model pointer", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel
			_, err := backend.FindOne("test_models", m.Id, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindOneBy()", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			model, err := backend.FindOneBy("test_models", "id", m.Id)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})

		It("Should .FindOneBy() with target model", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model TestModel
			_, err := backend.FindOneBy("test_models", "id", m.Id, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(m))
		})

		It("Should .FindOneBy() with target model pointer", func() {
			m := NewTestModel(1)
			Expect(backend.Create(&m)).ToNot(HaveOccurred())

			var model *TestModel
			_, err := backend.FindOneBy("test_models", "id", m.Id, &model)
			Expect(err).ToNot(HaveOccurred())
			Expect(model).To(Equal(&m))
		})
	})

	Describe("Relationships", func() {

		BeforeEach(func() {
			// Clear models.
			Expect(backend.Q("tags").Delete()).ToNot(HaveOccurred())
			Expect(backend.Q("projects").Delete()).ToNot(HaveOccurred())
			Expect(backend.Q("tasks").Delete()).ToNot(HaveOccurred())
			Expect(backend.Q("files").Delete()).ToNot(HaveOccurred())
			Expect(backend.Q("tasks_tags").Delete()).ToNot(HaveOccurred())

			// Rebuild relation info.
			backend.Build()
		})

		Describe("Has one", func() {
			It("Should ignore unpersisted has-one", func() {
				t := &Task{
					Name:    "P1",
					Project: Project{Name: "x"},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(backend.Q("projects").Count()).To(Equal(0))
			})

			It("Should set key for has-one relationship", func() {
				p := &Project{Name: "P1"}
				Expect(backend.Create(p)).ToNot(HaveOccurred())

				t := &Task{
					Name:    "T1",
					Project: *p,
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(t.ProjectId).To(Equal(p.Id))

				dbTask, err := backend.FindOne("tasks", t.Id)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbTask.(*Task).ProjectId).To(Equal(p.Id))
			})

			It("Should auto-persist has-one", func() {
				// Enable auto-create.
				backend.ModelInfo("tasks").Relation("Project").SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(t.Project.Id).ToNot(BeZero())

				m, err := backend.FindOne("projects", t.Project.Id)
				Expect(err).ToNot(HaveOccurred())
				Expect(m).ToNot(BeNil())

				tm, err := backend.FindOne("tasks", t.Id)
				Expect(err).ToNot(HaveOccurred())
				Expect(tm).ToNot(BeNil())
				Expect(tm.(*Task).ProjectId).To(Equal(t.Project.Id))
			})

			It("Should auto-update has-one", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("Project")
				rel.SetAutoCreate(true)
				rel.SetAutoUpdate(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				t.Project.Name = "NewName"
				Expect(backend.Update(t)).ToNot(HaveOccurred())

				m, _ := backend.FindOne("projects", t.Project.Id)
				Expect(m.(*Project).Name).To(Equal("NewName"))
			})

			It("Should auto-delete has-one", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("Project")
				rel.SetAutoCreate(true)
				rel.SetAutoDelete(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(backend.Delete(t)).ToNot(HaveOccurred())

				Expect(backend.FindOne("projects", t.Project.Id)).To(BeNil())
			})

			It("Should not delete with auto-delete disabled", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("Project")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(backend.Delete(t)).ToNot(HaveOccurred())

				Expect(backend.FindOne("projects", t.Project.Id)).ToNot(BeNil())
			})

			It("Should join has-one", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("Project")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q("tasks").Filter("id", t.Id).Join("Project").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*Task).Project.Id).To(Equal(t.Project.Id))
			})

			It("Should .Related() with model", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("Project")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q(t).Related("Project").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*Project).Id).To(Equal(t.Project.Id))
			})

			It("Should .Related() with id", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("Project")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					Project: Project{
						Name: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q("tasks", t.Id).Related("Project").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*Project).Id).To(Equal(t.Project.Id))
			})
		})

		Describe("Belongs to", func() {
			It("Should ignore unpersisted belongs-to", func() {
				t := &Task{
					Name: "P1",
					File: &File{Filename: "file.txt"},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(backend.Q("files").Count()).To(Equal(0))
			})

			It("Should set key for belongs-to relationship", func() {
				f := &File{Filename: "file.txt"}
				Expect(backend.Create(f)).ToNot(HaveOccurred())

				t := &Task{
					Name: "T1",
					File: f,
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(f.TaskId).To(Equal(t.Id))

				dbFile, err := backend.FindOne("files", f.Id)
				Expect(err).ToNot(HaveOccurred())
				Expect(dbFile.(*File).TaskId).To(Equal(t.Id))
			})

			It("Should auto-persist belongs-to", func() {
				// Enable auto-create.
				backend.ModelInfo("tasks").Relation("File").SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(t.File.Id).ToNot(BeZero())

				m, err := backend.FindOne("files", t.File.Id)
				Expect(err).ToNot(HaveOccurred())
				Expect(m).ToNot(BeNil())
				Expect(m.(*File).TaskId).To(Equal(t.Id))
			})

			It("Should auto-update belongs-to", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("File")
				rel.SetAutoCreate(true)
				rel.SetAutoUpdate(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				t.File.Filename = "NewName"
				Expect(backend.Update(t)).ToNot(HaveOccurred())

				m, _ := backend.FindOne("files", t.File.Id)
				Expect(m.(*File).Filename).To(Equal("NewName"))
			})

			It("Should auto-delete belongs-to", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("File")
				rel.SetAutoCreate(true)
				rel.SetAutoDelete(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(backend.Delete(t)).ToNot(HaveOccurred())

				Expect(backend.Q("files").Count()).To(Equal(0))
			})

			It("Should not delete with auto-delete disabled", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("File")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())
				Expect(backend.Delete(t)).ToNot(HaveOccurred())

				Expect(backend.FindOne("files", t.File.Id)).ToNot(BeNil())
			})

			It("Should join belongs-to", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("File")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q("tasks").Filter("id", t.Id).Join("File").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*Task).File).ToNot(BeNil())
				Expect(m.(*Task).File.Id).To(Equal(t.File.Id))
			})

			It("Should .Related() with model", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("File")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q(t).Related("File").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*File).Id).To(Equal(t.File.Id))
			})

			It("Should .Related() with id", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("tasks").Relation("File")
				rel.SetAutoCreate(true)

				t := &Task{
					Name: "T1",
					File: &File{
						Filename: "test",
					},
				}

				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q("tasks", t.Id).Related("File").First()
				Expect(err).ToNot(HaveOccurred())
				Expect(m.(*File).Id).To(Equal(t.File.Id))
			})
		})

		Describe("has-many", func() {
			It("Should ignore unpersisted has-many", func() {
				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())
				Expect(backend.Q("tasks").Count()).To(Equal(0))
			})

			It("Should set key for has-many relationship", func() {
				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(&p.Todos[0])).ToNot(HaveOccurred())
				Expect(backend.Create(&p.Todos[1])).ToNot(HaveOccurred())

				Expect(backend.Create(p)).ToNot(HaveOccurred())

				tasks, err := backend.Q("tasks").Find()
				Expect(err).ToNot(HaveOccurred())
				Expect(tasks).To(HaveLen(2))
				Expect(tasks[0].(*Task).ProjectId).To(Equal(p.Id))
				Expect(tasks[1].(*Task).ProjectId).To(Equal(p.Id))
			})

			It("Should auto-persist has-many", func() {
				// Enable auto-create.
				backend.ModelInfo("projects").Relation("Todos").SetAutoCreate(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())
				Expect(p.Todos[0].Id).ToNot(BeZero())
				Expect(p.Todos[1].Id).ToNot(BeZero())

				tasks, err := backend.Q("tasks").Find()
				Expect(err).ToNot(HaveOccurred())
				Expect(tasks).To(HaveLen(2))
				Expect(tasks[0].(*Task).ProjectId).To(Equal(p.Id))
				Expect(tasks[1].(*Task).ProjectId).To(Equal(p.Id))
			})

			It("Should auto-update has-many", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("projects").Relation("Todos")
				rel.SetAutoCreate(true)
				rel.SetAutoUpdate(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())

				p.Todos[0].Name = "X1"
				p.Todos[1].Name = "X2"
				Expect(backend.Update(p)).ToNot(HaveOccurred())

				tasks, err := backend.Q("tasks").Find()
				Expect(err).ToNot(HaveOccurred())
				Expect(tasks).To(HaveLen(2))
				Expect(tasks[0].(*Task).Name).To(Equal("X1"))
				Expect(tasks[1].(*Task).Name).To(Equal("X2"))
			})

			It("Should auto-delete has-many", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("projects").Relation("Todos")
				rel.SetAutoCreate(true)
				rel.SetAutoDelete(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())
				Expect(backend.Delete(p)).ToNot(HaveOccurred())

				Expect(backend.Q("tasks").Count()).To(Equal(0))
			})

			It("Should not delete with auto-delete disabled", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("projects").Relation("Todos")
				rel.SetAutoCreate(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())
				Expect(backend.Delete(p)).ToNot(HaveOccurred())

				Expect(backend.Q("tasks").Count()).To(Equal(2))
			})

			It("Should join has-many", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("projects").Relation("Todos")
				rel.SetAutoCreate(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())

				m, err := backend.Q("projects").Filter("id", p.Id).Join("Todos").First()
				Expect(err).ToNot(HaveOccurred())

				todos := m.(*Project).Todos
				Expect(todos).To(HaveLen(2))
			})

			It("Should .Related() with model", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("projects").Relation("Todos")
				rel.SetAutoCreate(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())

				m, err := backend.Q(p).Related("Todos").Find()
				Expect(err).ToNot(HaveOccurred())

				Expect(m).To(BeEquivalentTo([]interface{}{&p.Todos[0], &p.Todos[1]}))
			})

			It("Should .Related() with id", func() {
				// Enable auto-create.
				rel := backend.ModelInfo("projects").Relation("Todos")
				rel.SetAutoCreate(true)

				p := &Project{
					Name:  "P1",
					Todos: []Task{Task{Name: "T1"}, Task{Name: "T2"}},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())

				m, err := backend.Q("projects", p.Id).Related("Todos").Find()
				Expect(err).ToNot(HaveOccurred())

				Expect(m).To(BeEquivalentTo([]interface{}{&p.Todos[0], &p.Todos[1]}))
			})
		})

		Describe("M2M Collection", func() {

			It("Should error when building col for unpersisted model", func() {
				t := &Task{Name: "test"}

				_, err := backend.M2M(t, "Tags")
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("unpersisted_model"))
			})

			It("Should retrieve collection", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				_, err := backend.M2M(t, "Tags")
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should .Add()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")

				Expect(col.Add(tags[0], tags[1])).ToNot(HaveOccurred())

				Expect(col.All()).To(BeEquivalentTo([]interface{}{&tags[0], &tags[1]}))
			})

			It("Should .Remove()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")
				Expect(col.Add(tags[0], tags[1], tags[2], tags[3])).ToNot(HaveOccurred())

				Expect(col.Remove(tags[1], tags[2])).ToNot(HaveOccurred())

				Expect(col.All()).To(BeEquivalentTo([]interface{}{&tags[0], &tags[3]}))
			})

			It("Should .Clear()", func() {
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t1 := &Task{Name: "test"}
				Expect(backend.Create(t1)).ToNot(HaveOccurred())
				col1, _ := backend.M2M(t1, "Tags")
				Expect(col1.Add(tags[0], tags[1])).ToNot(HaveOccurred())

				t2 := &Task{Name: "test2"}
				Expect(backend.Create(t2)).ToNot(HaveOccurred())
				col2, _ := backend.M2M(t2, "Tags")
				Expect(col2.Add(tags[0], tags[1])).ToNot(HaveOccurred())

				Expect(col1.Clear()).ToNot(HaveOccurred())

				Expect(col1.Count()).To(Equal(0))
				Expect(col2.All()).To(BeEquivalentTo([]interface{}{&tags[0], &tags[1]}))
			})

			It("Should .Replace()", func() {
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t1 := &Task{Name: "test"}
				Expect(backend.Create(t1)).ToNot(HaveOccurred())
				col1, _ := backend.M2M(t1, "Tags")
				Expect(col1.Add(tags[0], tags[1])).ToNot(HaveOccurred())

				Expect(col1.Replace(tags[2], tags[3])).ToNot(HaveOccurred())

				Expect(col1.All()).To(BeEquivalentTo([]interface{}{&tags[2], &tags[3]}))
			})

			It("Should .Count()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")

				Expect(col.Count()).To(Equal(0))
				Expect(col.Add(tags[0])).ToNot(HaveOccurred())
				Expect(col.Count()).To(Equal(1))
				Expect(col.Add(tags[1], tags[2])).ToNot(HaveOccurred())
				Expect(col.Count()).To(Equal(3))
				Expect(col.Clear()).ToNot(HaveOccurred())
				Expect(col.Count()).To(Equal(0))
			})

			It("Should .Contains()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")

				Expect(col.Contains(tags[0])).To(BeFalse())

				Expect(col.Add(tags[0], tags[3])).ToNot(HaveOccurred())
				Expect(col.Contains(tags[0])).To(BeTrue())
				Expect(col.Contains(&tags[3])).To(BeTrue())
			})

			It("Should .ContainsId()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")

				Expect(col.ContainsId(tags[0].Id)).To(BeFalse())

				Expect(col.Add(tags[0], tags[3])).ToNot(HaveOccurred())
				Expect(col.ContainsId(tags[0].Id)).To(BeTrue())
				Expect(col.ContainsId(tags[3].Id)).To(BeTrue())
			})

			It("Should .GetById()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")

				Expect(col.ContainsId(tags[0].Id)).To(BeFalse())

				Expect(col.Add(tags[0], tags[3])).ToNot(HaveOccurred())
				Expect(col.ContainsId(tags[0].Id)).To(BeTrue())
				Expect(col.ContainsId(tags[3].Id)).To(BeTrue())
			})

			It("Should .All()", func() {
				t := &Task{Name: "test"}
				Expect(backend.Create(t)).ToNot(HaveOccurred())
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")

				Expect(col.All()).To(HaveLen(0))

				Expect(col.Add(tags[0], tags[3])).ToNot(HaveOccurred())
				Expect(col.All()).To(BeEquivalentTo([]interface{}{&tags[0], &tags[3]}))

				Expect(col.Add(tags[1], tags[2])).ToNot(HaveOccurred())
				Expect(col.All()).To(BeEquivalentTo([]interface{}{&tags[0], &tags[3], &tags[1], &tags[2]}))

				Expect(col.Clear()).ToNot(HaveOccurred())
				Expect(col.All()).To(HaveLen(0))
			})
		})

		Describe("m2m", func() {
			It("Should ignore unpersisted m2m", func() {
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")
				Expect(col.Count()).To(Equal(0))
			})

			It("Should save m2m", func() {
				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")
				Expect(col.Count()).To(Equal(2))
			})

			It("Should auto-persist m2m", func() {
				rel := backend.ModelInfo("tasks").Relation("Tags")
				rel.SetAutoCreate(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")
				Expect(col.Count()).To(Equal(2))
			})

			It("Should auto-update m2m", func() {
				rel := backend.ModelInfo("tasks").Relation("Tags")
				rel.SetAutoCreate(true)
				rel.SetAutoUpdate(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				tags[0].Tag = "X1"
				tags[1].Tag = "X2"

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				col, _ := backend.M2M(t, "Tags")
				all, err := col.All()
				Expect(err).ToNot(HaveOccurred())
				Expect(all).To(HaveLen(2))
				Expect(all[0].(*Tag).Tag).To(Equal("X1"))
				Expect(all[1].(*Tag).Tag).To(Equal("X2"))
			})

			It("Should auto-delete m2m", func() {
				rel := backend.ModelInfo("tasks").Relation("Tags")
				rel.SetAutoDelete(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				Expect(backend.Delete(t)).ToNot(HaveOccurred())

				// If a m2m collection exists, check that is was cleared properly.
				if backend.HasCollection("tasks_tags") {
					q := backend.Q("tasks_tags").Filter("tasks.id", t.Id)
					Expect(q.Count()).To(Equal(0))
				}
			})

			It("Should join m2m", func() {
				rel := backend.ModelInfo("tasks").Relation("Tags")
				rel.SetAutoDelete(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q("tasks").Filter("id", t.Id).Join("Tags").First()
				Expect(err).ToNot(HaveOccurred())

				taskTags := m.(*Task).Tags
				Expect(taskTags).To(HaveLen(2))
			})

			It("Should .Related() with model", func() {
				rel := backend.ModelInfo("tasks").Relation("Tags")
				rel.SetAutoDelete(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q(t).Related("Tags").Find()
				Expect(err).ToNot(HaveOccurred())

				Expect(m).To(BeEquivalentTo([]interface{}{&tags[0], &tags[1]}))
			})

			It("Should .Related() with id", func() {
				rel := backend.ModelInfo("tasks").Relation("Tags")
				rel.SetAutoDelete(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				t := &Task{
					Name: "test",
					Tags: []Tag{tags[0], tags[1]},
				}
				Expect(backend.Create(t)).ToNot(HaveOccurred())

				m, err := backend.Q("tasks", t.Id).Related("Tags").Find()
				Expect(err).ToNot(HaveOccurred())

				Expect(m).To(BeEquivalentTo([]interface{}{&tags[0], &tags[1]}))
			})

		})

		Describe("Complex relations", func() {
			It("Should do a nested join", func() {
				backend.ModelInfo("projects").Relation("Todos").SetAutoCreate(true)

				tags := []Tag{Tag{Tag: "T1"}, {Tag: "T2"}, {Tag: "T3"}, {Tag: "T4"}}
				Expect(backend.Create(&tags[0], &tags[1], &tags[2], &tags[3])).ToNot(HaveOccurred())

				p := &Project{
					Name: "P1",
					Todos: []Task{
						Task{Name: "Task 1", Tags: []Tag{tags[0], tags[1]}},
						Task{Name: "Task 2", Tags: []Tag{tags[2], tags[3]}},
					},
				}

				Expect(backend.Create(p)).ToNot(HaveOccurred())

				raw, err := backend.Q("projects").Filter("id", p.Id).Join("Todos.Tags").First()
				Expect(err).ToNot(HaveOccurred())
				m := raw.(*Project)
				Expect(m.Todos).To(HaveLen(2))
				Expect(m.Todos[0].Tags).To(Equal([]Tag{tags[0], tags[1]}))
				Expect(m.Todos[1].Tags).To(Equal([]Tag{tags[2], tags[3]}))
			})
		})
	})

	Describe("Transactions", func() {
		transactionBackend, _ := backend.(db.TransactionBackend)

		It("Should successfully commit a transaction", func() {
			if transactionBackend == nil {
				Skip("Not a transaction backend")
			}

			tx, err := transactionBackend.Begin()
			Expect(err).ToNot(HaveOccurred())
			Expect(tx).ToNot(BeNil())

			model := NewTestModel(100)
			Expect(tx.Create(&model)).ToNot(HaveOccurred())

			Expect(tx.Commit()).ToNot(HaveOccurred())

			m, err := backend.FindOne("test_models", model.Id)
			Expect(err).ToNot(HaveOccurred())
			Expect(m).ToNot(BeNil())
		})

		It("Should successfully roll back a transaction", func() {
			if transactionBackend == nil {
				Skip("Not a transaction backend")
			}

			tx, err := transactionBackend.Begin()
			Expect(err).ToNot(HaveOccurred())
			Expect(tx).ToNot(BeNil())

			model := NewTestModel(101)
			Expect(tx.Create(&model)).ToNot(HaveOccurred())

			Expect(tx.Rollback()).ToNot(HaveOccurred())

			m, err := backend.FindOne("test_models", model.Id)
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

			m2, err := backend.FindOne("hooks_models", m.Id)
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
