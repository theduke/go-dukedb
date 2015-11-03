package postgres_test

import (
	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"

	_ "github.com/lib/pq"

	"github.com/theduke/go-apperror"
	db "github.com/theduke/go-dukedb"
	"github.com/theduke/go-dukedb/backends/sql"
	"github.com/theduke/go-dukedb/backends/tests"
)

func builder() (db.Backend, apperror.Error) {
	return sql.New("postgres", "postgres://@localhost:10001/test?sslmode=disable")
}

var _ = Describe("Postgres", func() {
	tests.TestBackend(&setupFailed, builder)
})
