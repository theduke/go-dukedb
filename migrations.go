package dukedb

import (
	"log"
	"strconv"
	"time"
	"fmt"
)

/**
 * MigrationHandler object that keeps track of all the migrations
 * and allows to run them.
 */

type MigrationHandler struct {
	migrations []*Migration
	Backend MigrationBackend
}

func NewMigrationHandler(backend Backend) *MigrationHandler {
	m := MigrationHandler{}
	m.migrations = make([]*Migration, 0)
	m.Backend = backend.(MigrationBackend)

	return &m
}

func (m *MigrationHandler) HasMigration(version int) bool {
	return len(m.migrations) >= version
}

func (m *MigrationHandler) Add(migration Migration) {
	migration.Version = len(m.migrations) + 1
	m.migrations = append(m.migrations, &migration)
}

func (m *MigrationHandler) Get(version int) *Migration {
	return m.migrations[version-1]
}

func (m *MigrationHandler) Migrate(force bool) DbError {
	return m.MigrateTo(len(m.migrations), force)
}

func (m *MigrationHandler) MigrateTo(targetVersion int, force bool) DbError {
	// Ensure that migrations are set up.
	if err := m.Backend.MigrationsSetup(); err != nil {
		return err
	}

	// Determine if the database is locked.
	isLocked, err := m.Backend.IsMigrationLocked()
	if err != nil {
		return err
	}

	if isLocked {
		// Last attempt was aborted. DB is locked.
		return Error{
			Code: "migrations_locked",
			Message: "Can not migrate database: Last migration was aborted. DB is locked.",
		}
	}

	// Determine current version of the database.
	curVersion, err := m.Backend.DetermineMigrationVersion()
	if err != nil {
		return err
	}

	if curVersion < targetVersion {
		for nextVersion := curVersion + 1; nextVersion <= targetVersion; nextVersion++ {
			migration := m.Get(nextVersion)
			if migration == nil {
				return Error{
					Code: "unknown_migration",
					Message: fmt.Sprintf("Unknown migration version: %v", nextVersion),
				}
			}

			if err := m.RunMigration(migration); err != nil {
				// Migration failed! Abort.
				return err
			}
		}
	} else {
		log.Println("MIGRATE: DB is already at newest schema version: " + strconv.Itoa(targetVersion))
	}

	return nil
}

func (handler *MigrationHandler) RunMigration(m *Migration) DbError {
	log.Println("MIGRATE: Migrating to db version " + strconv.Itoa(m.Version) + ": " + m.Name)

	backend := handler.Backend

	var tx Transaction
	txCapableBackend, hasTransactions := handler.Backend.(TransactionBackend)
	if hasTransactions {
		tx = txCapableBackend.BeginTransaction()
		backend = tx.(MigrationBackend)
	}

	attempt := backend.NewMigrationAttempt()
	attempt.SetVersion(m.Version)
	attempt.SetStartedAt(time.Now())
	attempt.SetComplete(false)

	if err := backend.Create(attempt); err != nil {
		return err
	}

	if err := m.Up(backend); err != nil {
		if hasTransactions {
			tx.Rollback()
		} else {
			// No transaction, so update the attempt to reflect 
			// finished state but fail.
			attempt.SetFinishedAt(time.Now())
			backend.Update(attempt)
		}

		return Error{
			Code: "migration_failed",
			Message: fmt.Sprintf("Migration to %v (version %v) failed: %v", 
				m.Name, m.Version, err),
		}
	}

	// All went fine.
	attempt.SetFinishedAt(time.Now())
	attempt.SetComplete(true)
	if err := backend.Update(attempt); err != nil {
		if hasTransactions {
			tx.Rollback()
		}

		return Error{
			Code: "attempt_update_fail",
			Message: "Migration succeded, but could not update the attempt in the database: " + err.Error(),
		}
	}

	if hasTransactions {
		tx.Commit()
	}

	return nil
}

/**
 * Individual migration template.
 */

type Migration struct {
	Version 	  int
	Name        string
	Description string
	SkipOnNew   bool // Determines if this migration can be skipped if setting up a new database.
	Up          func(MigrationBackend) error
	Down        func(MigrationBackend) error
}

