package syncstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDispatchIndex(t *testing.T) {
	assert := assert.New(t)

	d, _ := NewDispatch(4, getTempBase(), TwoLevelPath, 5)

	// brute forced some uids to get index 0, 1, 2, 3 for the
	// dispatch ring of pools
	//uids := []string{"0", "10", "4", "1"}

	assert.Equal(uint16(0), d.Index("0"))
	assert.Equal(uint16(1), d.Index("10"))
	assert.Equal(uint16(2), d.Index("4"))
	assert.Equal(uint16(3), d.Index("1"))
}

func TestDispatchWorks(t *testing.T) {
	assert := assert.New(t)
	d, err := NewDispatch(4, getTempBase(), TwoLevelPath, 5)

	if !assert.NoError(err) {
		return
	}

	uids := []string{"0", "10", "4", "1"}
	cId := 1
	payload := String("hello")
	for _, uid := range uids {
		bId := "bso_4_" + uid

		_, err = d.PutBSO(uid, cId, bId, payload, nil, nil)
		if !assert.NoError(err) {
			return
		}

		b, err := d.GetBSO(uid, cId, bId)
		assert.NoError(err)

		if assert.NotNil(b) {
			assert.Equal(bId, b.Id)
			assert.Equal(*payload, b.Payload)
		}
	}
}

func BenchmarkDispatchIndex(b *testing.B) {
	d, _ := NewDispatch(4, getTempBase(), TwoLevelPath, 5)

	for i := 0; i < b.N; i++ {
		d.Index("a uid goes here")
	}
}

// Use dispatchwrap to test that the abstracted interface for SyncApi
// works all the way through
func TestDispatchSyncApiGetCollectionId(t *testing.T) {
	t.Parallel()
	testApiGetCollectionId(newDispatchwrap(), t)
}
func TestDispatchSyncApiGetCollectionModified(t *testing.T) {
	t.Parallel()
	testApiGetCollectionModified(newDispatchwrap(), t)
}
func TestDispatchSyncApiCreateCollection(t *testing.T) {
	t.Parallel()
	testApiCreateCollection(newDispatchwrap(), t)
}

func TestDispatchSyncApiDeleteCollection(t *testing.T) {
	t.Parallel()
	testApiDeleteCollection(newDispatchwrap(), t)
}

func TestDispatchSyncApiTouchCollection(t *testing.T) {
	t.Parallel()
	testApiTouchCollection(newDispatchwrap(), t)
}

func TestDispatchSyncApiInfoCollections(t *testing.T) {
	t.Parallel()
	testApiInfoCollections(newDispatchwrap(), t)
}

func TestDispatchSyncApiInfoCollectionUsage(t *testing.T) {
	t.Parallel()
	testApiInfoCollectionUsage(newDispatchwrap(), t)
}

func TestDispatchSyncApiInfoCollectionCounts(t *testing.T) {
	t.Parallel()
	testApiInfoCollectionCounts(newDispatchwrap(), t)
}

func TestDispatchSyncApiPublicPostBSOs(t *testing.T) {
	t.Parallel()
	testApiPostBSOs(newDispatchwrap(), t)
}

func TestDispatchSyncApiPublicPutBSO(t *testing.T) {
	t.Parallel()
	testApiPutBSO(newDispatchwrap(), t)
}

func TestDispatchSyncApiPublicGetBSO(t *testing.T) {
	t.Parallel()
	testApiGetBSO(newDispatchwrap(), t)
}

func TestDispatchSyncApiPublicGetBSOs(t *testing.T) {
	t.Parallel()
	testApiGetBSOs(newDispatchwrap(), t)
}

func TestDispatchSyncApiPublicGetBSOModified(t *testing.T) {
	t.Parallel()
	testApiGetBSOModified(newDispatchwrap(), t)
}

func TestDispatchSyncApiDeleteBSO(t *testing.T) {
	t.Parallel()
	testApiDeleteBSO(newDispatchwrap(), t)
}
func TestDispatchSyncApiDeleteBSOs(t *testing.T) {
	t.Parallel()
	testApiDeleteBSOs(newDispatchwrap(), t)
}

func TestDispatchSyncApiPurgeExpired(t *testing.T) {
	t.Parallel()
	testApiPurgeExpired(newDispatchwrap(), t)
}

func TestDispatchSyncApiUsageStats(t *testing.T) {
	t.Parallel()
	testApiUsageStats(newDispatchwrap(), t)
}

func TestDispatchSyncApiOptimize(t *testing.T) {
	t.Parallel()
	testApiOptimize(newDispatchwrap(), t)
}

func TestDispatchSyncApiDeleteEverything(t *testing.T) {
	t.Parallel()
	testApiDeleteEverything(newDispatchwrap(), t)
}
