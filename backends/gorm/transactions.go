package gorm

import (
	db "github.com/theduke/dukedb"
)

func (b Backend) BeginTransaction() db.Transaction {
	copied :=  b.Copy()
	backendCopy := copied.(*Backend)
	backendCopy.Db = b.Db.Begin()

	return backendCopy
}

func (b *Backend) Rollback() db.DbError {
	if err := b.Db.Commit().Error; err != nil {
		return db.Error{
			Code: "transaction_commit_failed",
			Message: err.Error(),
			Data: err,
		}
	}
	return nil
}

func (b *Backend)	Commit() db.DbError {
	if err := b.Db.Commit().Error; err != nil {
		return db.Error{
			Code: "transaction_commit_failed",
			Message: err.Error(),
			Data: err,
		}
	}
	return nil
}
