package syncstorage

import "time"

func newDispatchwrap() *dispatchwrap {
	return newDispatchwrapUID("1234567890")
}
func newDispatchwrapUID(uid string) *dispatchwrap {

	d, err := NewDispatch(4, getTempBase(), time.Millisecond*10)
	if err != nil {
		panic(err)
	}

	return &dispatchwrap{uid: uid, dispatch: d}
}
