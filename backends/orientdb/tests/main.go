package main

import (
	"fmt"
	"github.com/theduke/go-dukedb/backends/orientdb"
	"gopkg.in/istreamdata/orientgo.v2"
	"reflect"
)

func main() {
	b, err := orientdb.New("localhost:2424", "test", "root", "root")
	if err != nil {
		panic(err)
	}

	res := b.SqlExec("SELECT FROM tasks")
	//res := b.SqlExec("INSERT INTO tasks(name) VALUES(?)", "NewTask 2")
	//res := b.SqlExec("CREATE CLASS tags")
	var rawData interface{}
	if err := res.All(&rawData); err != nil {
		panic(err)
	}

	allData, ok := rawData.([]orient.OIdentifiable)
	if ok {
		fmt.Printf("data: %v - %+v\n", reflect.TypeOf(allData[0]), allData[0])
	}
}
