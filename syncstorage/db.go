package syncstorage

import (
	"database/sql"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	. "github.com/tj/go-debug"
)

var dbDebug = Debug("syncstorage:db")

type DB struct {
	sync.Mutex

	UserId string

	// sqlite database path
	Path string

	db *sql.DB
}

func (d *DB) Open() (err error) {
	d.db, err = sql.Open("sqlite3", d.Path)

	if err != nil {
		return
	}

	// initialize and or update tables if required

	// Initialize Schema 0 if it doesn't exist
	sqlCheck := "SELECT name from sqlite_master WHERE type='table' AND name=?"
	var name string
	if err := d.db.QueryRow(sqlCheck, "KeyValues").Scan(&name); err == sql.ErrNoRows {

		tx, err := d.db.Begin()
		if err != nil {
			return err
		}

		if _, err := tx.Exec(SCHEMA_0); err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				return rollbackErr
			} else {
				return err
			}
		} else {
			dbDebug("Initialized new database at for UserId: %s at %s", d.UserId, d.Path)
			if err := tx.Commit(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (d *DB) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

func (d *DB) GetCollection(name string) (id int, err error) {

	err = d.db.QueryRow("SELECT Id FROM Collections where Name=?", name).Scan(&id)

	return
}

func (d *DB) PutObject(collectionId int, b *BSO) error {
	d.Lock()
	defer d.Unlock()

	dbDebug("Putting BSO %s into collection: %d", b.Id, collectionId)
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`INSERT INTO BSO (
			Id, CollectionId, SortIndex,
			PayLoad, PayLoadSize,
			Modified, TTL)
			VALUES (
				?,?,?,
				?,?,
				?,?
			)`,
		b.Id, collectionId, b.SortIndex,
		b.Payload, len(b.Payload),
		Now(), b.TTL)

	if err == nil {
		dbDebug("Success, COMMIT put BSO %s", b.Id)
		return tx.Commit()
	} else {
		dbDebug("Fail, ROLLBACK putting BSO %s", b.Id)
		tx.Rollback()
		return err
	}
}

func (d *DB) StorageUsed() (count, storage uint, err error) {
	d.Lock()
	defer d.Unlock()

	dql := "SELECT count(1) totalBSO, sum(PayloadSize) totalSize FROM BSO"
	err = d.db.QueryRow(dql).Scan(&count, &storage)

	if err == sql.ErrNoRows {
		err = nil
	}

	// with SQLite the sum(PayloadSize) on no records comes back as a nil
	// which .Scan(...) can't convert correctly in go so it errors. This
	// turns the error into something useful
	if count == 0 {
		return 0, 0, nil
	}

	return
}

func Now() float64 {
	return float64(time.Now().UnixNano()) / 1000 / 1000
}

func NewDB(path, userId string) (*DB, error) {
	/* TODO: I'd be good to do some input validation */

	d := &DB{UserId: userId, Path: path}

	if err := d.Open(); err != nil {
		return nil, err
	}

	return d, nil
}
