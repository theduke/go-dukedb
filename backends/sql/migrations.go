package sql

import (
	"time"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
)

/**
 * Implement migration related interfaces.
 */
func (b *Backend) GetMigrationHandler() *db.MigrationHandler {
	return b.migrationHandler
}

func (b Backend) MigrationsSetup() apperror.Error {
	count, err := b.Count(b.Q("migration_attempts"))
	// Todo. determine right error string.
	if err != nil {
		count = -1
	}

	initialRun := count == -1

	if initialRun {
		tx, err := b.Begin()
		if err != nil {
			return err
		}

		if err := tx.CreateCollection("migration_attempts"); err != nil {
			tx.Rollback()
			return apperror.Wrap(err, "migration_setup_failed", "Could not create migrations table")
		}

		migration := MigrationAttempt{}
		migration.Version = 0
		migration.StartedAt = time.Now()
		migration.FinishedAt = time.Now()
		migration.Complete = true

		if err := tx.Create(&migration); err != nil {
			return apperror.Wrap(err, "migration_setup_failed", "Could not create initial migration")
		}

		tx.Commit()
	}

	return nil
}

func (b Backend) IsMigrationLocked() (bool, apperror.Error) {
	var lastAttempt *MigrationAttempt
	if model, err := b.Q("migration_attempts").Last(); err != nil {
		return true, apperror.Wrap(err, "db_error")
	} else {
		lastAttempt = model.(*MigrationAttempt)
	}

	if lastAttempt.Id != 0 && lastAttempt.FinishedAt.IsZero() {
		// Last attempt was aborted. DB is locked.
		return true, nil
	}
	return false, nil
}

func (b Backend) DetermineMigrationVersion() (int, apperror.Error) {
	var lastAttempt *MigrationAttempt
	if model, err := b.Q("migration_attempts").Filter("complete", true).Last(); err != nil {
		return -1, apperror.Wrap(err, "db_error")
	} else {
		lastAttempt = model.(*MigrationAttempt)
	}

	return lastAttempt.Version, nil
}

type MigrationAttempt struct {
	db.BaseMigrationAttemptIntId
}

func (b Backend) NewMigrationAttempt() db.MigrationAttempt {
	return &MigrationAttempt{}
}
