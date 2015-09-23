package memory

import (
	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
)

/**
 * Implement migration related interfaces.
 */

func (b Backend) GetMigrationHandler() *db.MigrationHandler {
	return b.MigrationHandler
}

func (b Backend) MigrationsSetup() apperror.Error {
	return nil
}

func (b Backend) IsMigrationLocked() (bool, apperror.Error) {
	return false, nil
}

func (b Backend) DetermineMigrationVersion() (int, apperror.Error) {
	return b.MigrationVersion, nil
}

type MigrationAttempt struct {
	db.BaseMigrationAttemptIntID
}

func (b Backend) NewMigrationAttempt() db.MigrationAttempt {
	return &MigrationAttempt{}
}
