package mysql_test

import (
	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"


	"github.com/theduke/go-dukedb/backends/sql"
	"github.com/theduke/go-dukedb/backends/tests"
)

var _ = Describe("Mysql", func() {
	backend, _ := sql.New("mysql", "root@tcp(127.0.0.1:10002)/test?charset=utf8&parseTime=True&loc=Local")
	tests.TestBackend(backend)
})
