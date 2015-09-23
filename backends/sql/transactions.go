package sql

import (
	"github.com/theduke/go-apperror"

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

func (b *Backend) Rollback() apperror.Error {
	if err := b.Tx.Rollback(); err != nil {
		return apperror.Wrap(err, "transaction_rollback_failed")
	}
	return nil
}

func (b *Backend) Commit() apperror.Error {
	if err := b.Tx.Commit(); err != nil {
		return apperror.Wrap(err, "transaction_commit_failed")
	}
	return nil
}
