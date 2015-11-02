package mysql_test

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	_ "github.com/go-sql-driver/mysql"

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
	tmpDir = path.Join(os.TempDir(), "dukedb_backend_mysql_test")
	// Ensure that tmp dir is deleted.
	os.RemoveAll(tmpDir)
	err := os.MkdirAll(tmpDir, 0700)
	Expect(err).ToNot(HaveOccurred())

	user, _ := user.Current()

	// Run initdb.
	fmt.Printf("Running mysql_install_db\n")
	_, err = exec.Command("mysql_install_db", "--datadir="+tmpDir, "--user="+user.Username).Output()
	Expect(err).ToNot(HaveOccurred())

	args := []string{
		"--datadir=" + tmpDir,
		"--port=10002",
		"--pid-file=" + path.Join(tmpDir, "mysql.pid"),
		"--socket=" + path.Join(tmpDir, "mysql.sock"),
	}
	fmt.Printf("Starting mysql server\n")
	serverCmd = exec.Command("mysqld", args...)

	err = serverCmd.Start()
	Expect(err).NotTo(HaveOccurred())

	// Give the server some time to start.
	time.Sleep(time.Second * 5)

	fmt.Printf("Connecting to test database\n")
	backend, err := sql.New("mysql", "root@tcp(127.0.0.1:10002)/?charset=utf8&parseTime=True&loc=Local")
	Expect(err).ToNot(HaveOccurred())
	Expect(backend).ToNot(BeNil())

	_, err = backend.SqlExec("CREATE DATABASE test")
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	serverCmd.Process.Kill()
	os.RemoveAll(tmpDir)
})
