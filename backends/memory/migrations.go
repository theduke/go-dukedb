package memory

import (
	db "github.com/theduke/go-dukedb"
)

/**
 * Implement migration related interfaces.
 */

func (b Backend) GetMigrationHandler() *db.MigrationHandler {
	return b.MigrationHandler
}

func (b Backend) MigrationsSetup() db.DbError {
	return nil
}

func (b Backend) IsMigrationLocked() (bool, db.DbError) {
	return false, nil
}

func (b Backend) DetermineMigrationVersion() (int, db.DbError) {
	return b.MigrationVersion, nil
}

type MigrationAttempt struct {
	db.BaseMigrationAttemptIntID
}

func (b Backend) NewMigrationAttempt() db.MigrationAttempt {
	return &MigrationAttempt{}
}
