package syncstorage

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTempBase() string {
	dir, _ := ioutil.TempDir(os.TempDir(), "pool_test")
	return dir
}

func TestPoolPathAndFile(t *testing.T) {
	assert := assert.New(t)

	T_basepath := getTempBase()
	T_sep := string(os.PathSeparator)

	p, _ := NewPool(T_basepath, TwoLevelPath)

	path, file := p.PathAndFile("uid1234")
	assert.Equal("uid1234.db", file)
	assert.Equal(T_basepath+T_sep+"4"+T_sep+"3", path)
}

func TestPoolgetDB(t *testing.T) {

	assert := assert.New(t)
	_ = assert

	uid := "abc123"
	p, _ := NewPool(getTempBase(), TwoLevelPath)
	db, err := p.getDB(uid)

	assert.NoError(err)
	assert.NotNil(db)

	// make sure we get the same value of of the DB
	db2, err := p.getDB(uid)
	assert.NoError(err)
	assert.NotNil(db2)
	assert.Equal(db, db2)
}

func TestPoolPutGetBSO(t *testing.T) {
	assert := assert.New(t)

	uid := "abc123"
	cId := 1
	bId := "bso1"
	sortIndex := Int(12)
	payload := String("this is a big load")

	p, _ := NewPool(getTempBase(), TwoLevelPath)

	_, err := p.PutBSO(uid, cId, bId, payload, sortIndex, nil)
	assert.NoError(err)

	b, err := p.GetBSO(uid, cId, bId)
	if assert.NoError(err) {
		assert.Equal(bId, b.Id)
		assert.Equal(*payload, b.Payload)
		assert.Equal(*sortIndex, b.SortIndex)
	}
}
