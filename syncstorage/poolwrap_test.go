package syncstorage

func newPoolwrap() *poolwrap { return newPoolwrapUID("1234567890") }
func newPoolwrapUID(uid string) *poolwrap {

	p, err := NewPool(getTempBase(), TwoLevelPath)
	if err != nil {
		panic(err)
	}

	return &poolwrap{uid: uid, pool: p}
}
