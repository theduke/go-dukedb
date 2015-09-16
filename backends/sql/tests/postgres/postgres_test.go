package postgres_test

import (
	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"

	_ "github.com/lib/pq"

	"github.com/theduke/go-dukedb/backends/sql"
	"github.com/theduke/go-dukedb/backends/tests"
)

var _ = Describe("Postgres", func() {
	backend, _ := sql.New("postgres", "postgres://@localhost:10001/test?sslmode=disable")
	tests.TestBackend(backend)
})
