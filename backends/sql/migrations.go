package sql

import (
	"time"

	db "github.com/theduke/go-dukedb"
)

/**
 * Implement migration related interfaces.
 */
func (b Backend) GetMigrationHandler() *db.MigrationHandler {
	return b.MigrationHandler
}

func (b Backend) MigrationsSetup() db.apperror.Error {
	count, err := b.Count(b.Q("migration_attempts"))
	// Todo. determine right error string.
	if err != nil {
		count = -1
	}

	initialRun := count == -1

	if initialRun {
		tx := b.Begin()

		if err := tx.CreateCollection("migration_attempts"); err != nil {
			return db.Error{
				Code:    "migration_setup_failed",
				Message: "Could not create migrations table: " + err.Error(),
				Data:    err,
			}

			tx.Rollback()
			return db.Error{
				Code:    "migration_setup_failed",
				Message: err.Error(),
			}
		}

		migration := MigrationAttempt{}
		migration.Version = 0
		migration.StartedAt = time.Now()
		migration.FinishedAt = time.Now()
		migration.Complete = true

		if err := tx.Create(&migration); err != nil {
			return db.Error{
				Code:    "migration_setup_failed",
				Message: "Could not create migrations table: " + err.Error(),
				Data:    err,
			}
		}

		tx.Commit()
	}

	return nil
}

func (b Backend) IsMigrationLocked() (bool, db.apperror.Error) {
	var lastAttempt *MigrationAttempt
	if model, err := b.Q("migration_attempts").Last(); err != nil {
		return true, db.Error{
			Code:    "db_error",
			Message: err.Error(),
		}
	} else {
		lastAttempt = model.(*MigrationAttempt)
	}

	if lastAttempt.ID != 0 && lastAttempt.FinishedAt.IsZero() {
		// Last attempt was aborted. DB is locked.
		return true, nil
	}
	return false, nil
}

func (b Backend) DetermineMigrationVersion() (int, db.apperror.Error) {
	var lastAttempt *MigrationAttempt
	if model, err := b.Q("migration_attempts").Filter("complete", true).Last(); err != nil {
		return -1, db.Error{
			Code:    "db_error",
			Message: err.Error(),
		}
	} else {
		lastAttempt = model.(*MigrationAttempt)
	}

	return lastAttempt.Version, nil
}

type MigrationAttempt struct {
	db.BaseMigrationAttemptIntID
}

func (b Backend) NewMigrationAttempt() db.MigrationAttempt {
	return &MigrationAttempt{}
}
