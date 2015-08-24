package main

import(
	"log"
	"reflect"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/jinzhu/gorm"

	db "github.com/theduke/dukedb"
	dbgorm "github.com/theduke/dukedb/backends/gorm"
)

type Test struct {
	ID int
	Name string
	Intval int
	Tag TestTag
	Tags []*TestTag
}

func (t Test) GetCollection() string {
	return "tests"
}

func (t Test) GetID() string {
	return "1"
}

func (t Test) SetID(x string) {

}

type TestTag struct {
	Name string
	TestID int `db:"primary_key"`
}

func (t TestTag) GetCollection() string {
	return "test_tags"
}

func (t TestTag) GetID() string {
	return "1"
}

func (t TestTag) SetID(x string) {

}

func main() {
	//name, typ, err := db.ModelFindPrimaryKey(&Test{})
	//log.Printf("name: %v\ntype: %v\nerr: %v\n", name, typ, err)
	//info, err := db.NewModelInfo(&Test{})
	//log.Printf("err: %v\n", err)
	//log.Printf("%+v\n", info)

	fmt.Sprintf("type: %v\n", reflect.TypeOf(db.Query{}).Name())

	// connect to db
	url := "postgres://theduke:theduke@localhost/docduke?sslmode=disable"
	gormDb, err := gorm.Open("postgres", url)
	if err != nil {
		log.Printf("db connect failed")
	}

	backend := dbgorm.New(&gormDb)
	backend.RegisterModel(&Test{})	
	backend.RegisterModel(&TestTag{})
	backend.SetDebug(true)

	/*
	err = backend.Create(&Test{
		Pk: "101",
		Name: "Name 101",
		Intval: 101,
	})
	log.Printf("err: %v\n", err)
	*/
	/*
	q := db.And(
		db.Or(db.Eq("name", "name1"), db.Eq("intval", 3333)),
		db.Not(
			db.Or(db.Eq("name", "Name 101"), db.Eq("intval", 101))))
	*/
	q := backend.Q("tests")

	result, err := q.Join("test_tags", "Tags").Find()
	//m := result.(*Test)
	//result, err := q.Limit(2).Offset(1).Order("name", true).Find()

	m := result[1].(*Test)

	log.Printf("err: %v\nLen: %v\nRes: %+v\n", err, len(result), m.Tags[2])
}
