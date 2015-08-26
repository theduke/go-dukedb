package gorm

import (
	"strconv"
	"time"
	"log"

	db "github.com/theduke/dukedb"
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

		migration := MigrationAttempt{
			Version:    0,
			StartedAt:  time.Now(),
			FinishedAt: time.Now(),
			Complete:  true,
		}
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

func (b Backend) NewMigrationAttempt() db.MigrationAttempt {
	return &MigrationAttempt{}
}

type MigrationAttempt struct {
	ID uint64
	Version int
	StartedAt time.Time
	FinishedAt time.Time
	Complete bool
}

func (m MigrationAttempt) GetCollection() string {
	return "migration_attempts"
}

func(a *MigrationAttempt) GetID() string {
	return strconv.FormatUint(a.ID, 10)
}

func(a *MigrationAttempt) SetID(x string) error {
	id, err := strconv.ParseUint(x, 10, 64)
	if err != nil {
		return err
	}
	a.ID = id
	return nil
}

func(a *MigrationAttempt) GetVersion() int {
	return a.Version
}

func(a *MigrationAttempt) SetVersion(x int) {
	a.Version = x
}

func(a *MigrationAttempt) GetStartedAt() time.Time {
	return a.StartedAt
}

func(a *MigrationAttempt) SetStartedAt(x time.Time) {
	a.StartedAt = x
}

func(a *MigrationAttempt) GetFinishedAt() time.Time {
	return a.FinishedAt
}

func(a *MigrationAttempt) SetFinishedAt(x time.Time) {
	a.FinishedAt = x
}

func(a *MigrationAttempt) GetComplete() bool {
	return a.Complete
}

func(a *MigrationAttempt) SetComplete(x bool) {
	a.Complete = x
}

/*
	// Check if the migration attempts table exists.
	// Otherwise, create it and run the additional migration.


	// Determine if the database is locked.


	// Determine current version of the database.
	

	if curVersion < targetVersion {
		for nextVersion := curVersion + 1; nextVersion <= targetVersion; nextVersion++ {
			migration := m.Get(nextVersion)
			if err := migration.Run(db); err != nil {
				// Migration failed! Abort.
				return err
			}
		}
	} else {
		log.Println("DB is already at newest schema version: " + strconv.Itoa(targetVersion))
	}
*/