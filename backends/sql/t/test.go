package main

import (
	"fmt"

	_ "github.com/lib/pq"
	db "github.com/theduke/go-dukedb"
	"github.com/theduke/go-dukedb/backends/sql"
)

type Project struct {
	db.IntIdModel

	Name string `db:"required"`

	Todos []*Todo `db:"auto-create"`
}

type Todo struct {
	db.IntIdModel

	Name        string
	Description string
	Priority    int

	ProjectId uint64
	Project   *Project
}

func mustNot(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	b, err := sql.New("postgres", "postgres://theduke:theduke@localhost/test")
	mustNot(err)

	b.SetDebug(true)
	b.RegisterModel(&Todo{})
	b.RegisterModel(&Project{})
	b.EnableProfiling()
	b.Build()
	//b.EnableSqlProfiling()

	fmt.Printf("rels: %+v\n", b.ModelInfo("projects").Relations())

	mustNot(b.DropCollection("todos", true, false))
	mustNot(b.CreateCollection("todos"))
	mustNot(b.CreateCollection("projects"))

	t1 := &Todo{
		Name:        "T1",
		Description: "T1 descr",
		Priority:    55,
	}
	p := &Project{
		Name:  "Proj1",
		Todos: []*Todo{t1},
	}

	mustNot(b.Create(p))

	var t *Todo
	_, err = b.Q("todos").Name("todo_q1").Filter("id", 1).First(&t)
	mustNot(err)
	fmt.Printf("res: %+v\n", t1)
}
