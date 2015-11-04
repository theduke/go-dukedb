package memory_test

import (
	. "github.com/onsi/ginkgo"
	//. "github.com/onsi/gomega"

	"github.com/theduke/go-apperror"
	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/backends/memory"
	"github.com/theduke/go-dukedb/backends/tests"
)

var _ = Describe("Memory", func() {
	var skip = false
	tests.TestBackend(&skip, func() (db.Backend, apperror.Error) {
		return New(), nil
	})
})
