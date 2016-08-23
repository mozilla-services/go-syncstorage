package syncstorage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBatchCreate(t *testing.T) {
	assert := assert.New(t)
	data := "some data\n"

	db, err := getTestDB()
	if !assert.NoError(err) {
		return
	}
	cId := 1
	batchId, err := db.BatchCreate(cId, data)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(1, batchId)

	batch, err := db.BatchLoad(batchId, cId)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(batchId, batch.Id)
	assert.Equal(data, batch.BSOS)
	assert.True(batch.Modified > 0)
}

func TestBatchUpdate(t *testing.T) {
	assert := assert.New(t)
	data0 := "data0\n"
	data1 := "data1\n"
	cId := 1

	db, _ := getTestDB()
	batchId, err := db.BatchCreate(cId, data0)
	if !assert.NoError(err) {
		return
	}

	batchOrig, err := db.BatchLoad(batchId, cId)

	if !assert.NoError(err) {
		return
	}
	assert.Equal(data0, batchOrig.BSOS)

	// make sure we have a different timestamp
	time.Sleep(10 * time.Millisecond)

	err = db.BatchAppend(batchId, cId, data1)
	if !assert.NoError(err) {
		return
	}

	batchUpdated, err := db.BatchLoad(batchId, cId)
	if !assert.NoError(err) {
		return
	}

	assert.Equal(data0+data1, batchUpdated.BSOS)
	assert.NotEqual(batchOrig.Modified, batchUpdated.Modified)
}

func TestBatchUpdateInvalidId(t *testing.T) {
	assert := assert.New(t)
	db, _ := getTestDB()
	cId := 1

	err := db.BatchAppend(22, cId, "test")
	assert.Equal(ErrBatchNotFound, err)

	batchId, err := db.BatchCreate(cId, "test")
	if !assert.NoError(err) {
		return
	}

	// correct batch id, wrong collection
	err = db.BatchAppend(batchId, cId+1, "test")
	assert.Equal(ErrBatchNotFound, err)
}

func TestBatchRemove(t *testing.T) {
	assert := assert.New(t)
	data := "some data\n"

	db, err := getTestDB()
	if !assert.NoError(err) {
		return
	}
	cId := 1
	batchId, err := db.BatchCreate(cId, data)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(1, batchId)

	{
		err := db.BatchRemove(batchId)
		if !assert.NoError(err) {
			return
		}
	}

	{
		_, err := db.BatchLoad(batchId, cId)
		assert.Equal(ErrBatchNotFound, err)
	}
}

func TestBatchPurge(t *testing.T) {
	assert := assert.New(t)
	data := "some data\n"

	db, err := getTestDB()
	if !assert.NoError(err) {
		return
	}
	cId := 1

	batchId, err := db.BatchCreate(cId, data)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(1, batchId)

	{
		time.Sleep(time.Millisecond * 10)
		numPurged, err := db.BatchPurge(1) // purge everything older than a ms
		if !assert.NoError(err) {
			return
		}
		assert.Equal(1, numPurged)

		_, err = db.BatchLoad(batchId, cId)
		assert.Equal(ErrBatchNotFound, err)
	}

}

func TestBatchExists(t *testing.T) {

	assert := assert.New(t)

	db, _ := getTestDB()
	batchId, err := db.BatchCreate(1, "hello")
	if !assert.NoError(err) {
		return
	}

	exists, err := db.BatchExists(batchId, 1)
	assert.True(exists)
	assert.NoError(err)

	notExists, err := db.BatchExists(2, 1)
	assert.False(notExists)
	assert.NoError(err)
}
