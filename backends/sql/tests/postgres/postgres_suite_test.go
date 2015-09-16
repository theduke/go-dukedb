package postgres_test

import (
	"testing"
	"os"
	"os/exec"
	"path"
	"time"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/theduke/go-dukedb/backends/sql"
	db "github.com/theduke/go-dukedb"
)

func TestPostgres(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Postgres Suite")
}

var serverCmd *exec.Cmd
var tmpDir string
var finishedChannel chan bool

var backend db.Backend

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
	serverCmd := exec.Command("postgres", args...) 

  err = serverCmd.Start()
  Expect(err).NotTo(HaveOccurred())

  // Give the server some time to start.
  time.Sleep(time.Second * 3)

  // Create a database.
  fmt.Printf("Creating test database\n")
 	_, err = exec.Command("bash", "-c", "echo \"create database test;\" | psql -h localhost -p 10001 -d postgres").Output()
 	Expect(err).ToNot(HaveOccurred()) 

 	fmt.Printf("Connecting to test database\n")
 	backend, err = sql.New("postgres", "postgres://test:@localhost/test?sslmode=disable")
 	Expect(err).ToNot(HaveOccurred())
 	Expect(backend).ToNot(BeNil())
})

var _ = AfterSuite(func() {
	os.RemoveAll(tmpDir)  
})
