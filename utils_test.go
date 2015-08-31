package dukedb_test

import (
	"reflect"
	"fmt"

	. "github.com/theduke/go-dukedb"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Utils", func() {

	Describe("CamelCaseToUnderscore", func() {
		It("Should be empty", func() {
			Expect(CamelCaseToUnderscore("")).To(Equal(""))
		})

		It("Should be single lower case letter from upper case", func() {
			Expect(CamelCaseToUnderscore("A")).To(Equal("a"))
		})

		It("Should be single lower case letter from lower case", func() {
			Expect(CamelCaseToUnderscore("a")).To(Equal("a"))
		})

		It("Should be two lower case letters", func() {
			Expect(CamelCaseToUnderscore("AA")).To(Equal("aa"))
		})

		It("Should be two parts with underscore", func() {
			Expect(CamelCaseToUnderscore("AaBb")).To(Equal("aa_bb"))
		})

		It("Should be three parts with underscore", func() {
			Expect(CamelCaseToUnderscore("AaBbCc")).To(Equal("aa_bb_cc"))
		})

		It("Should end with lower case", func() {
			Expect(CamelCaseToUnderscore("AaBbC")).To(Equal("aa_bb_c"))
		})
	})

	Describe("GetStructFieldValue", func() {
		type TestStruct struct {
			Val string
			ValEmpty string
			IntVal int
			StructVal *TestStruct
		}

		var testStruct TestStruct

		BeforeEach(func() {
			testStruct = TestStruct{
				Val: "test", 
				IntVal: 33,  
				StructVal: &TestStruct{Val: "test2"},
			}
		})

		Context("With nil struct argument", func() {
			It("Should fail on nil", func() {
				_, err := GetStructFieldValue(nil, "test")
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("pointer_or_struct_expected"))
			})
		})

		Context("With pointer to non-struct", func() {
			It("Should fail on pointer to non-struct", func() {
				x := 22
				_, err := GetStructFieldValue(&x, "test")
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("struct_expected"))
			})
		})

		Context("With non-struct arugment", func() {
			It("Should fail on non-struct argument", func() {
				_, err := GetStructFieldValue(22, "test")
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("struct_expected"))
			})
		})

		Context("With valid fields", func() {
			It("Should be valid string", func() {
				Expect(GetStructFieldValue(&testStruct, "Val")).To(Equal("test"))
			})

			It("Should be valid int", func() {
				Expect(GetStructFieldValue(&testStruct, "IntVal")).To(Equal(33))
			})

			It("Should be pointer to struct", func() {
				Expect(GetStructFieldValue(&testStruct, "StructVal")).To(Equal(&TestStruct{Val: "test2"}))
			})
		})

		Context("With invalid fields", func() {
			It("Should fail on non-existant field", func() {
				_, err := GetStructFieldValue(&testStruct, "DoesNotExist")
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("field_not_found"))
			})
		})
	})

	Describe("CompareValues", func() {
		It("Should eq with two strings", func() {
			a := interface{}("test")
			b := interface{}("test")
			Expect(CompareValues("eq", a, b)).To(BeTrue())
		})

		It("Should eq with two numbers", func() {
			a := interface{}(1)
			b := interface{}(uint32(1))
			Expect(CompareValues("eq", a, b)).To(BeTrue())
		})
	})

	Describe("CompareStringValues", func() {
		It("Should eq", func() {
			a := interface{}("test")
			b := interface{}("test")
			Expect(CompareStringValues("eq", a, b)).To(BeTrue())
		})
	})

	Describe("CompareNumericValues", func() {
		It("Should lt with ints", func() {
			a := interface{}(1)
			b := interface{}(2)
			Expect(CompareNumericValues("lt", a, b)).To(BeTrue())
		})

		It("Should gt with int64 and uint8", func() {
			a := interface{}(int64(1))
			b := interface{}(uint8(5))
			Expect(CompareNumericValues("lt", a, b)).To(BeTrue())
		})
	})

	Describe("SortStructSlice", func() {
		type Sortable struct {
			IntVal int
			FloatVal float32
			StrVal string
		}

		var sortables []interface{}

		BeforeEach(func() {
			sortables = []interface{}{
				Sortable{5, 5.1, "5"},
				Sortable{3, 3.1, "3"},
				Sortable{1, 1.1, "1"},
				Sortable{2, 2.1, "2"},
				Sortable{4, 4.1, "4"},
			}
		})

		It("Should sort asc by int field", func() {
			SortStructSlice(sortables, "IntVal", true)
			Expect((sortables[0]).(Sortable).IntVal).To(Equal(1))	
			Expect(sortables[4].(Sortable).IntVal).To(Equal(5))	
		})

		It("Should sort desc by int field", func() {
			SortStructSlice(sortables, "IntVal", false)
			Expect((sortables[0]).(Sortable).IntVal).To(Equal(5))	
			Expect(sortables[4].(Sortable).IntVal).To(Equal(1))	
		})

		It("Should sort asc by string field", func() {
			SortStructSlice(sortables, "StrVal", true)
			Expect((sortables[0]).(Sortable).StrVal).To(Equal("1"))	
			Expect(sortables[4].(Sortable).StrVal).To(Equal("5"))	
		})
	})
	
	Describe("ConvertToType", func() {
		It("Should convert int", func() {
			Expect(ConvertToType("-22", reflect.Int)).To(Equal(-22))
		})	

		It("Should convert int64", func() {
			Expect(ConvertToType("-22", reflect.Int64)).To(Equal(int64(-22)))
		})

		It("Should convert uint", func() {
			Expect(ConvertToType("22", reflect.Uint)).To(Equal(uint(22)))
		})

		It("Should convert uint64", func() {
			Expect(ConvertToType("22", reflect.Uint64)).To(Equal(uint64(22)))
		})

		It("Should convert string", func() {
			Expect(ConvertToType("test", reflect.String)).To(Equal("test"))
		})

		It("Should error on invalid int", func() {
			_, err := ConvertToType("test", reflect.Int)
			Expect(err).To(HaveOccurred())
		})

		It("Should error on unsupported type", func() {
			_, err := ConvertToType("22", reflect.Ptr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("cannot_convert_to_ptr"))
		})
	})

	Describe("SetStructFieldValueFromString", func() {
		type TestStruct struct {
			Val string
			IntVal int
		}

		var testStruct TestStruct

		BeforeEach(func() {
			testStruct = TestStruct{}
		})

		It("Should error on non-pointer", func() {
			err := SetStructFieldValueFromString(22, "Val", "22")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("pointer_expected"))
		})

		It("Should error on non-struct pointer", func() {
			x := 22
			err := SetStructFieldValueFromString(&x, "Val", "22")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("pointer_to_struct_expected"))
		})

		It("Should error on inexistant field", func() {
			err := SetStructFieldValueFromString(&testStruct, "InvalidField", "22")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("unknown_field"))
		})

		It("Should error on unconvertable type", func() {
			err := SetStructFieldValueFromString(&testStruct, "IntVal", "xxx")
			Expect(err).To(HaveOccurred())
		})

		It("Should work with valid args for string field", func() {
			Expect(SetStructFieldValueFromString(&testStruct, "Val", "xxx")).ToNot(HaveOccurred())
			Expect(testStruct.Val).To(Equal("xxx"))
		})

		It("Should work with valid args for int field", func() {
			Expect(SetStructFieldValueFromString(&testStruct, "IntVal", "22")).ToNot(HaveOccurred())
			Expect(testStruct.IntVal).To(Equal(22))
		})

	})

	Describe("GetModelSliceFieldValues", func() {
		var modelSlice []Model	

		BeforeEach(func() {
			modelSlice = []Model{&TestModel{
				ID: 1,
				StrVal: "str1",
				IntVal: 1,
			}, &TestModel{
				ID: 2,
				StrVal: "str2",
				IntVal: 2,
			}}
		})

		It("Should error on invalid field", func() {
			_, err := GetModelSliceFieldValues(modelSlice, "InvalidField")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("field_not_found"))
		})

		It("Should work for str field", func() {
			val := []interface{}{"str1", "str2"}
			Expect(GetModelSliceFieldValues(modelSlice, "StrVal")).To(Equal(val))
		})

		It("Should work for int field", func() {
			val := []interface{}{1, 2}
			Expect(GetModelSliceFieldValues(modelSlice, "IntVal")).To(Equal(val))
		})
	})

	Describe("FilterToSqlCondition", func() {

		It("Should convert eq", func() {
			Expect(FilterToSqlCondition("eq")).To(Equal("="))
		})

		It("Should convert neq", func() {
			Expect(FilterToSqlCondition("neq")).To(Equal("!="))
		})

		It("Should convert lt", func() {
			Expect(FilterToSqlCondition("lt")).To(Equal("<"))
		})

		It("Should convert lte", func() {
			Expect(FilterToSqlCondition("lte")).To(Equal("<="))
		})

		It("Should convert gt", func() {
			Expect(FilterToSqlCondition("gt")).To(Equal(">"))
		})

		It("Should convert gte", func() {
			Expect(FilterToSqlCondition("gte")).To(Equal(">="))
		})

		It("Should convert like", func() {
			Expect(FilterToSqlCondition("like")).To(Equal("LIKE"))
		})

		It("Should convert eq", func() {
			Expect(FilterToSqlCondition("eq")).To(Equal("="))
		})

		It("Should convert in", func() {
			Expect(FilterToSqlCondition("in")).To(Equal("IN"))
		})

		It("Should error on invalid filter", func() {
			_, err := FilterToSqlCondition("XXX")
			Expect(err).To(HaveOccurred())
			Expect(err.GetCode()).To(Equal("unknown_filter"))
		})
	})

	Describe("InterfaceToModelSlice", func() {
		var slice []interface{}

		var modelSlice []Model

		BeforeEach(func() {
			modelSlice = NewTestModelInterfaceSlice(1, 2)
			slice = []interface{}{
				modelSlice[0],
				modelSlice[1],
			}
		})

		It("Should error on non-slice/non pointer slice argument", func() {
			_, err := InterfaceToModelSlice(22)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("slice_expected"))
		})

		It("Should fail on non-model slice", func() {
			_, err := InterfaceToModelSlice([]int{1,2})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("slice_values_do_not_implement_model_if"))
		})

		It("Should work with pointer to model slice", func() {
			Expect(InterfaceToModelSlice(&slice)).To(Equal(modelSlice))
		})

		It("Should work with model slice", func() {
			Expect(InterfaceToModelSlice(slice)).To(Equal(modelSlice))
		})
	})

	Describe("ModelToInterfaceSlice", func() {
		var modelSlice []Model

		BeforeEach(func() {
			modelSlice = NewTestModelInterfaceSlice(1, 2)
		})

		It("Converts correctly", func() {
			ifSlice := []interface{}{modelSlice[0], modelSlice[1]}
			Expect(ModelToInterfaceSlice(modelSlice)).To(Equal(ifSlice))
		})
	})

	Describe("NewStruct", func() {

		It("Should error on non-struct", func() {
			_, err := NewStruct(22)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("struct_expected"))
		})

		It("Should error on ptr non-struct", func() {
			x := 22
			_, err := NewStruct(&x)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("struct_expected"))
		})

		It("Should build struct from pointer", func() {
			s, _ := NewStruct(TestModel{})
			Expect(s).To(Equal(&TestModel{}))
		})

	})

	Describe("NewSlice", func() {

		It("Should build int slice", func() {
			Expect(NewSlice(22)).To(Equal([]int{}))
		})

		It("Should build pointer to model slice", func() {
			Expect(NewSlice(&TestModel{})).To(Equal([]*TestModel{}))
		})
	})

	Describe("SetStructModelField", func() {

		var testParent *TestParent

		BeforeEach(func() {
			p := NewTestParent(1, false)
			testParent = &p
		})

		It("Should error on non-pointer", func() {
			err := SetStructModelField(22, "Child", []Model{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("pointer_expected"))
		})

		It("Should error on pointer to non-struct", func() {
			x := 22
			err := SetStructModelField(&x, "Child", []Model{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("pointer_to_struct_expected"))
		})

		It("Should error on unknown field", func() {
			err := SetStructModelField(testParent, "InvalidField", []Model{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("unknown_field"))
		})

		It("Should error on invalid target field type", func() {
			err := SetStructModelField(testParent, "StrVal", []Model{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("unsupported_field_type"))
		})

		It("Should set struct", func() {
			child := NewTestModel(1)
			SetStructModelField(testParent, "Child", []Model{&child})
			Expect(testParent.Child).To(Equal(child))
		})

		It("Should set struct pointer", func() {
			child := NewTestModel(1)
			SetStructModelField(testParent, "ChildPtr", []Model{&child})
			Expect(testParent.ChildPtr).To(Equal(&child))
		})

		It("Should set slice", func() {
			childSlice := NewTestModelSlice(1, 2)
			modelSlice, _ := InterfaceToModelSlice(childSlice)
			SetStructModelField(testParent, "ChildSlice", modelSlice)
			Expect(testParent.ChildSlice).To(Equal(childSlice))
		})

		It("Should set pointer slice", func() {
			childSlice := NewTestModelPtrSlice(1, 2)
			modelSlice, _ := InterfaceToModelSlice(childSlice)
			SetStructModelField(testParent, "ChildSlicePtr", modelSlice)
			Expect(testParent.ChildSlicePtr).To(Equal(childSlice))
		})

	})

	Describe("ParseFieldTag", func() {
		It("Should parse primary_key", func() {
			info, _ := ParseFieldTag("primary_key;")
			Expect(info.PrimaryKey).To(Equal(true))

			info, _ = ParseFieldTag("primary_key")
			Expect(info.PrimaryKey).To(Equal(true))
		})

		It("Should parse ignore", func() {
			info, _ := ParseFieldTag("-")
			Expect(info.Ignore).To(Equal(true))
		})

		It("Should parse name", func() {
			info, _ := ParseFieldTag("name:the_name")
			Expect(info.Name).To(Equal("the_name"))
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

		Describe("NewModelInfo", func() {

			It("Should fail on invalid tags", func() {
				type InvalidTagModel struct {
					TestModel
					InvalidField string `db:"has-one:xxx"` 
				}

				_, err := NewModelInfo(&InvalidTagModel{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("build_field_info_failed"))
			})

			It("Should fail without primary key", func() {
				type NoPKModel struct {
					BaseModel
					SomeField string
				}
				_, err := NewModelInfo(&NoPKModel{})
				Expect(err).To(HaveOccurred())
				Expect(err.GetCode()).To(Equal("primary_key_not_found"))
			})	

			It("Should determine ID field as primary key", func() {
				info, err := NewModelInfo(&TestModel{})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.PkField).To(Equal("ID"))
			})

			It("Should determine explicit primary key field", func() {
				type PKModel struct {
					BaseModel
					Name string `db:"primary_key"`
				}

				info, err := NewModelInfo(&PKModel{})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.PkField).To(Equal("Name"))
			})

			It("Should build info for test model successfully", func() {
				_, err := NewModelInfo(&TestModel{})
				Expect(err).ToNot(HaveOccurred())
			})

			It("Should build info for test parent model successfully", func() {
				_, err := NewModelInfo(&TestParent{})
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Describe("ModelInfoMethods", func() {


			It("Should run .GetPkName() correctly", func() {
				type PKModel struct {
					BaseModel
					Name string `db:"primary_key;name:custom_name"`
				}

				info, _ := NewModelInfo(&PKModel{})
				Expect(info.GetPkName()).To(Equal("custom_name"))
			})

			It("Should map field names correctly (.MapFieldName())", func() {
					type PKModel struct {
						BaseModel
						Name string `db:"primary_key;name:custom_name"`
					}

					info, _ := NewModelInfo(&PKModel{})
					Expect(info.MapFieldName("custom_name")).To(Equal("Name"))
				})
		})

	})

	Describe("Building of relationship info", func() {
		It("Builds relationship info without errors", func() {
			parent, _ := NewModelInfo(&TestParent{})	
			model, _ := NewModelInfo(&TestModel{})

			modelInfo := map[string]*ModelInfo{
				"test_parents": parent, 
				"test_models": model,
			}
			Expect(BuildAllRelationInfo(modelInfo)).ToNot(HaveOccurred())
		})
	})

	Context("Correct relationship info", func() {
		var modelInfo map[string]*ModelInfo

		BeforeEach(func() {
			parent, _ := NewModelInfo(&TestParent{})	
			model, _ := NewModelInfo(&TestModel{})

			modelInfo = map[string]*ModelInfo{
				"test_parents": parent, 
				"test_models": model,
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

	Describe("Query parsers", func() {

		It("Shold parse simple condition correctly", func() {
			json := `{
				"filters": {"name": "testname"}
			}
			`
			q, err := ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.Model).To(Equal("col"))
			Expect(q.Filters[0]).To(BeEquivalentTo(Eq("name", "testname")))
		})

		It("Shold parse comparison operator condition correctly", func() {
			json := `{
				"filters": {"intField": {"$gt": 20}}
			}
			`
			q, err := ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())
			Expect(q.Model).To(Equal("col"))
			Expect(q.Filters[0]).To(BeEquivalentTo(Gt("intField", float64(20))))
		})

		It("Shold parse multiple conditions correctly", func() {
			json := `{
				"filters": {"name": "testname", "intField": {"$gt": 20}}
			}
			`
			q, err := ParseJsonQuery("col", []byte(json))

			Expect(err).ToNot(HaveOccurred())

			first := q.Filters[0].(*FieldCondition)
			second := q.Filters[1].(*FieldCondition)

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
			q, err := ParseJsonQuery("col", []byte(json))
			fmt.Printf("\nquery: %+v\n", q)

			Expect(err).ToNot(HaveOccurred())

			// The ordering by json.Unmarshal() is random, so checking has to be done 
			// with order in mind.
			or := q.Filters[0].(*OrCondition)
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
	})
})
