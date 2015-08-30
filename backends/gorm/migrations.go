package gorm

import (
	"time"
	"log"

	db "github.com/theduke/go-dukedb"
)

/**
 * Implement migration related interfaces.
 */
func (b Backend) GetMigrationHandler() *db.MigrationHandler {
	return b.MigrationHandler
}

func (b Backend) MigrationsSetup() db.DbError {
	count := -1
	b.Db.Model(MigrationAttempt{}).Count(&count)
	initalRun := count == -1

	if initalRun {
		log.Println("MIGRATE: Building migration tables.")
		tx := b.Db.Begin()

		if err := tx.CreateTable(MigrationAttempt{}).Error; err != nil {
			return db.Error{
				Code: "migration_setup_failed",
				Message: "Could not create migrations table: " + err.Error(),
				Data: err,
			}

			tx.Rollback()
			return db.Error{
				Code: "migration_setup_failed",
				Message: err.Error(),
			}
		}

		migration := MigrationAttempt{}
		migration.Version = 0
		migration.StartedAt = time.Now()
		migration.FinishedAt = time.Now()
		migration.Complete = true
		
		if err := tx.Create(&migration).Error; err != nil {
			return db.Error{
				Code: "migration_setup_failed",
				Message: "Could not create migrations table: " + err.Error(),
				Data: err,
			}
		}

		tx.Commit()
		log.Println("MIGRATE: Migrations table created.")
	}

	return nil
}


func (b Backend) IsMigrationLocked() (bool, db.DbError) {
	var lastAttempt MigrationAttempt
	if err := b.Db.Last(&lastAttempt).Error; err != nil {
		return true, db.Error{
			Code: "db_error",
			Message: err.Error(),
		}
	}

	if lastAttempt.ID != 0 && lastAttempt.FinishedAt.IsZero() {
		// Last attempt was aborted. DB is locked.
		return true, nil
	}
	return false, nil
}

func (b Backend) DetermineMigrationVersion() (int, db.DbError) {
	var lastAttempt MigrationAttempt
	if err := b.Db.Where("complete = ?", true).Last(&lastAttempt).Error; err != nil {
		return -1, db.Error{
			Code: "db_error",
			Message: err.Error(),
		}
	}
	
	return lastAttempt.Version, nil
}

type MigrationAttempt struct {
	db.BaseMigrationAttemptIntID
}

func (b Backend) NewMigrationAttempt() db.MigrationAttempt {
	return &MigrationAttempt{}
}