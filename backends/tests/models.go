package tests

import (
	"fmt"
	"time"

	. "github.com/theduke/go-dukedb"

	"github.com/theduke/go-apperror"
)

type Tag struct {
	Id  uint64
	Tag string
}

type Project struct {
	Id uint64

	Name        string `db:"required"`
	Description string

	CreatedAt time.Time
	UpdatedAt *time.Time

	// has-many with struct slice.
	Todos []Task

	// has-many with struct pointer slice
	ArchviedTodos []*Task
}

type Task struct {
	Id uint64

	Name        string `db:"required"`
	Description string
	Priority    int

	// has-one with struct.
	Project   Project
	ProjectId uint64

	// has-one with struct pointer.
	Project2   *Project
	Project2ID *Project

	// belongs-to with struct.
	File *File

	// m2m with struct slice.
	Tags []Tag `db:"m2m"`

	// m2m with struct pointer slice.
	categories []*Tag `db:"m2m"`
}

type File struct {
	ID       uint64
	TaskId   uint64
	Filename string `db:"required"`
}

type TestModel struct {
	ID uint64

	// For inferred belongs-to
	TestParentID uint64 `db:"ignore-zero"`

	// For explicit has-one/belongs-to
	MyParent   *TestParent `db:"has-one:MyParentID:ID"`
	MyParentID uint64      `db:"ignore-zero"`

	StrVal string
	IntVal int64
}

type HooksModel struct {
	TestModel
	CalledHooks []string `db:"-"`
	HookError   bool     `db:"-"`
}

func (h *HooksModel) Validate() error {
	h.CalledHooks = append(h.CalledHooks, "validate")
	return nil
}

func (h *HooksModel) BeforeCreate(Backend) error {
	h.CalledHooks = append(h.CalledHooks, "before_create")
	if h.HookError {
		return apperror.New("before_create")
	}
	return nil
}

func (h *HooksModel) AfterCreate(Backend) {
	h.CalledHooks = append(h.CalledHooks, "after_create")
}

func (h *HooksModel) BeforeUpdate(Backend) error {
	h.CalledHooks = append(h.CalledHooks, "before_update")
	if h.HookError {
		return apperror.New("before_update")
	}
	return nil
}

func (h *HooksModel) AfterUpdate(Backend) {
	h.CalledHooks = append(h.CalledHooks, "after_update")
}

func (h *HooksModel) BeforeDelete(Backend) error {
	h.CalledHooks = append(h.CalledHooks, "before_delete")
	if h.HookError {
		return apperror.New("before_delete")
	}
	return nil
}

func (h *HooksModel) AfterDelete(Backend) {
	h.CalledHooks = append(h.CalledHooks, "after_delete")
}

func (h *HooksModel) AfterQuery(Backend) {
	h.CalledHooks = append(h.CalledHooks, "after_query")
}

type ValidationsModel struct {
	TestModel

	NotNullString string `db:"required"`
	NotNullInt    int    `db:"required"`

	ValidatedString string `db:"min:5;max:10"`
	ValidatedInt    int    `db:"min:5;max:10"`
}

func (m *ValidationsModel) Collection() string {
	return "validations_models"
}

type TestParent struct {
	TestModel

	Child   TestModel `db:"has-one:ChildID:ID;auto-persist;"`
	ChildID uint64

	ChildPtr *TestModel `db:"belongs-to:ID:ID;auto-persist;"`

	ChildSlice    []TestModel
	ChildSlice2   []TestModel  `db:"belongs-to:ID:MyParentID"`
	ChildSlicePtr []*TestModel `db:"m2m"`
}

func NewTestModel(index int) TestModel {
	return TestModel{
		//ID:     uint64(index),
		StrVal: fmt.Sprintf("str%v", index),
		IntVal: int64(index),
	}
}

func NewTestParent(index int, withChildren bool) TestParent {
	base := NewTestModel(index)

	var child TestModel
	var childPtr *TestModel
	var childSlice []TestModel
	var childPtrSlice []*TestModel
	if withChildren {
		child = NewTestModel(index*10 + 1)

		child2 := NewTestModel(index*10 + 2)
		childPtr = &child2

		childSlice = NewTestModelSlice(index*10+3, 2)
		childPtrSlice = NewTestModelPtrSlice(index*10+5, 2)
	}

	return TestParent{
		TestModel: base,

		Child:    child,
		ChildPtr: childPtr,

		ChildSlice:    childSlice,
		ChildSlicePtr: childPtrSlice,
	}
}

func NewTestModelSlice(startIndex int, count int) []TestModel {
	slice := make([]TestModel, 0)
	for i := startIndex; i < startIndex+count; i++ {
		slice = append(slice, NewTestModel(i))
	}

	return slice
}

func NewTestModelPtrSlice(startIndex int, count int) []*TestModel {
	slice := make([]*TestModel, 0)
	for i := startIndex; i < startIndex+count; i++ {
		model := NewTestModel(i)
		slice = append(slice, &model)
	}

	return slice
}

func NewTestModelInterfaceSlice(startIndex int, count int) []interface{} {
	slice := make([]interface{}, 0)
	for i := startIndex; i < startIndex+count; i++ {
		model := NewTestModel(i)
		slice = append(slice, &model)
	}

	return slice
}

func NewTestParentSlice(startIndex int, count int, withChildren bool) []TestParent {
	slice := make([]TestParent, 0)
	for i := startIndex; i < startIndex+count; i++ {
		slice = append(slice, NewTestParent(i, withChildren))
	}

	return slice
}

func NewTestParentPtrSlice(startIndex int, count int, withChildren bool) []*TestParent {
	slice := make([]*TestParent, 0)
	for i := startIndex; i < startIndex+count; i++ {
		parent := NewTestParent(i, withChildren)
		slice = append(slice, &parent)
	}

	return slice
}

func NewTestParentInterfaceSlice(startIndex int, count int, withChildren bool) []interface{} {
	slice := make([]interface{}, 0)
	for i := startIndex; i < startIndex+count; i++ {
		model := NewTestParent(i, withChildren)
		slice = append(slice, &model)
	}

	return slice
}
