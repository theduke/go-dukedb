package dukedb_test

import (
	. "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type BaseModel struct {
}

func (i BaseModel) GetID() string { 
	return "" 
}
func (i BaseModel) SetID(x string) error {
	return nil
}
func (i BaseModel) GetCollection() string { 
	return "base_model" 
}

type TestModel struct {
	ID uint64

	// For inferred belongs-to
	TestParentID uint64

	// For explicit has-one/belongs-to
	MyParent *TestParent `db:"has-one:MyParentID:ID"`
	MyParentID uint64 

	StrVal string
	IntVal int
}

func (t TestModel) GetCollection() string {
	return "test_models"
}

func (t TestModel) GetID() string {
	return strconv.FormatUint(t.ID, 64)
}

func (t TestModel) SetID(x string) error {
	id, err := strconv.ParseUint(x, 10, 64)
	if err != nil {
		return err
	}
	t.ID = id
	return nil
}

type TestParent struct {
	TestModel

	Child TestModel
	ChildID uint64

	ChildPtr *TestModel

	ChildSlice []TestModel
	ChildSlice2 []TestModel `db:"belongs-to:ID:MyParentID"`
	ChildSlicePtr []*TestModel `db:"m2m"`
}

func (t TestParent) GetCollection() string {
	return "test_parents"
}

func NewTestModel(index int) TestModel{
	return TestModel{
		ID: uint64(index),
		StrVal: fmt.Sprintf("str%v", index),
		IntVal: 1,
	}
}

func NewTestParent(index int, withChildren bool) TestParent {
	base := NewTestModel(index)

	var child TestModel
	var childPtr *TestModel
	var childSlice []TestModel
	var childPtrSlice []*TestModel
	if withChildren {
		child = NewTestModel(index * 10 + 1)

		child2 := NewTestModel(index * 10 + 2)
		childPtr = &child2

		childSlice = NewTestModelSlice(index * 10 + 3, 2)
		childPtrSlice = NewTestModelPtrSlice(index * 10 + 5, 2)
	}

	return TestParent{
		TestModel: base,

		Child: child,
		ChildPtr: childPtr,

		ChildSlice: childSlice,
		ChildSlicePtr: childPtrSlice,
	}
}

func NewTestModelSlice(startIndex int, count int) []TestModel {
	slice := make([]TestModel, 0)
	for i := startIndex; i < startIndex + count; i++ {
		slice = append(slice, NewTestModel(i))
	}

	return slice
}

func NewTestModelPtrSlice(startIndex int, count int) []*TestModel {
	slice := make([]*TestModel, 0)
	for i := startIndex; i < startIndex + count; i++ {
		model := NewTestModel(i)
		slice = append(slice, &model)
	}

	return slice
}

func NewTestModelInterfaceSlice(startIndex int, count int) []Model {
	slice := make([]Model, 0)
	for i := startIndex; i < startIndex + count; i++ {
		model := NewTestModel(i)
		slice = append(slice, &model)
	}

	return slice
}

func NewTestParentSlice(startIndex int, count int, withChildren bool) []TestParent {
	slice := make([]TestParent, 0)
	for i := startIndex; i < startIndex + count; i++ {
		slice = append(slice, NewTestParent(i, withChildren))
	}

	return slice
}

func NewTestParentPtrSlice(startIndex int, count int, withChildren bool) []*TestParent {
	slice := make([]*TestParent, 0)
	for i := startIndex; i < startIndex + count; i++ {
		parent := NewTestParent(i, withChildren)
		slice = append(slice, &parent)
	}

	return slice
}

func NewTestParentInterfaceSlice(startIndex int, count int, withChildren bool) []Model {
	slice := make([]Model, 0)
	for i := startIndex; i < startIndex + count; i++ {
		model := NewTestParent(i, withChildren)
		slice = append(slice, &model)
	}

	return slice
}

var _ = Describe("Models", func() {

	Describe("BaseModelIntID", func() {

		It("Should set id", func() {
			m := BaseModelIntID{}
			err := m.SetID("22")
			Expect(err).ToNot(HaveOccurred())
			Expect(m.ID).To(Equal(uint64(22)))
		})

		It("Should error on SetID() with invalid arg", func() {
			m := BaseModelIntID{}
			err := m.SetID("xxx")
			Expect(err).To(HaveOccurred())
		})

		It("Should get id", func() {
			m := BaseModelIntID{ID: uint64(22)}
			Expect(m.GetID()).To(Equal("22"))
		})

	})

})
