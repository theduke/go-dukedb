package postgres_test

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/theduke/go-dukedb/backends/sql"
)

func TestPostgres(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Suite")
}

var serverCmd *exec.Cmd
var tmpDir string
var finishedChannel chan bool

var _ = BeforeSuite(func() {
	tmpDir = path.Join(os.TempDir(), "dukedb_backend_postgres_test")
	// Ensure that tmp dir is deleted.
	os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, 0700)
	Expect(err).ToNot(HaveOccurred())

	// Run initdb.
	fmt.Printf("Running postgres initdb\n")
	_, err = exec.Command("initdb", "-D", tmpDir).Output()
	Expect(err).ToNot(HaveOccurred())

	args := []string{
		"-D",
		tmpDir,

		"-p",
		"10001",

		"-c",
		"unix_socket_directories=" + tmpDir,
	}
	fmt.Printf("Starting postgres server\n")
	serverCmd = exec.Command("postgres", args...)

	err = serverCmd.Start()
	Expect(err).NotTo(HaveOccurred())

	// Give the server some time to start.
	time.Sleep(time.Second * 3)

	fmt.Printf("Connecting to postgres database\n")
	backend, err := sql.New("postgres", "postgres://@localhost:10001/postgres?sslmode=disable")
	Expect(err).ToNot(HaveOccurred())
	Expect(backend).ToNot(BeNil())

	_, err = backend.SqlExec("CREATE DATABASE test")
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	serverCmd.Process.Kill()
	os.RemoveAll(tmpDir)
})
