package main

import (
	"fmt"

	_ "github.com/lib/pq"
	db "github.com/theduke/go-dukedb"
	"github.com/theduke/go-dukedb/backends/sql"
)

type Todo struct {
	db.IntIdModel

	Name        string
	Description string
	Priority    int
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

	mustNot(b.DropCollection("todos", true, false))
	mustNot(b.CreateCollection("todos"))

	t := &Todo{
		Name:        "T1",
		Description: "T1 descr",
		Priority:    55,
	}
	mustNot(b.Create(t))

	res, err := b.Q("todos").Filter("id", 1).First()
	mustNot(err)
	fmt.Printf("res: %+v\n", res)
}
