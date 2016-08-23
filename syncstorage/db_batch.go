package syncstorage

import (
	"database/sql"

	"github.com/pkg/errors"
)

var (
	ErrBatchNotFound = errors.New("Batch Not Found")
)

type BatchRecord struct {
	Id           int
	CollectionId int
	BSOS         string
	Modified     int
}

// BatchCreate creates a new batch
func (d *DB) BatchCreate(cId int, data string) (int, error) {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return 0, errors.Wrap(err, "BatchCreate: Failed creating transaction")
	}

	results, err := tx.Exec("INSERT INTO Batches(CollectionId, Modified,BSOS) VALUES (?, ?, ?)",
		cId,
		Now(),
		data,
	)

	var batchId64 int64
	if err == nil {
		batchId64, err = results.LastInsertId()
	}

	if err != nil {
		tx.Rollback()
		return 0, errors.Wrap(err, "Could not create new batch")
	}

	tx.Commit()
	return int(batchId64), nil
}

// BatchAppend adds data to the BSOS column for a batch id
func (d *DB) BatchAppend(id, cId int, data string) (err error) {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()

	if err != nil {
		return errors.Wrap(err, "BatchAppend: Failed creating transaction")
	}

	result, err := tx.Exec("UPDATE Batches SET Modified=?, BSOS=BSOS || ? WHERE Id=? AND CollectionId=?",
		Now(),
		data,
		id,
		cId,
	)

	if err != nil {
		tx.Rollback()
		return errors.Wrap(err, "Could not append to batch")
	}

	if affected, _ := result.RowsAffected(); affected == 0 {
		tx.Rollback()
		return ErrBatchNotFound
	}

	tx.Commit()
	return
}

// BatchExists checks if a batch exists without loading all the data from disk
func (d *DB) BatchExists(id, cId int) (bool, error) {
	d.Lock()
	defer d.Unlock()

	var foundId int
	err := d.db.QueryRow("SELECT Id FROM Batches WHERE Id=? AND CollectionId=?", id, cId).Scan(&foundId)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, errors.Wrap(err, "BatchExists failed")
	}

	return true, nil
}

func (d *DB) BatchLoad(id, cId int) (*BatchRecord, error) {
	d.Lock()
	defer d.Unlock()

	r := &BatchRecord{Id: id}

	err := d.db.QueryRow("SELECT CollectionId, Modified, BSOS FROM Batches WHERE Id=? AND CollectionId=?", id, cId).Scan(&r.CollectionId, &r.Modified, &r.BSOS)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrBatchNotFound
		}

		return nil, errors.Wrap(err, "Failed to SELECT Batch")
	}

	return r, nil
}

func (d *DB) BatchRemove(id int) error {
	d.Lock()
	defer d.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	if _, err := tx.Exec("DELETE FROM Batches WHERE Id=?", id); err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()
	return nil
}

func (d *DB) BatchPurge(TTL int) (int, error) {

	d.Lock()
	defer d.Unlock()

	r, err := d.db.Exec("DELETE FROM Batches WHERE (? - Modified) >= ?", Now(), TTL)
	if err != nil {
		return 0, err
	}

	purged, err := r.RowsAffected()
	return int(purged), err
}
