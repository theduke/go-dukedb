package sql

import (
	db "github.com/theduke/go-dukedb"
)

func (b Backend) Begin() db.Transaction {
	copied := b.Copy()
	backendCopy := copied.(*Backend)
	tx, err := b.Db.Begin()
	if err != nil {
		return nil
	}

	backendCopy.Tx = tx
	backendCopy.Db = nil

	return backendCopy
}

func (b *Backend) Rollback() db.apperror.Error {
	if err := b.Tx.Rollback(); err != nil {
		return db.Error{
			Code:    "transaction_rollback_failed",
			Message: err.Error(),
			Data:    err,
		}
	}
	return nil
}

func (b *Backend) Commit() db.apperror.Error {
	if err := b.Tx.Commit(); err != nil {
		return db.Error{
			Code:    "transaction_commit_failed",
			Message: err.Error(),
			Data:    err,
		}
	}
	return nil
}
