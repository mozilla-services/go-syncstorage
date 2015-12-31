package syncstorage

func newDispatchwrap() *dispatchwrap {
	return newDispatchwrapUID("1234567890")
}
func newDispatchwrapUID(uid string) *dispatchwrap {

	d, err := NewDispatch(4, getTempBase(), TwoLevelPath, 5)
	if err != nil {
		panic(err)
	}

	return &dispatchwrap{uid: uid, dispatch: d}
}
