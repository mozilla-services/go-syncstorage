package syncstorage

import (
	"strconv"
	"time"
)

func newDispatchwrap() *dispatchwrap {
	uid := strconv.FormatInt(time.Now().UnixNano(), 36)
	return newDispatchwrapUID(uid)
}
func newDispatchwrapUID(uid string) *dispatchwrap {

	d, err := NewDispatch(4, ":memory:", time.Minute)
	if err != nil {
		panic(err)
	}

	return &dispatchwrap{uid: uid, dispatch: d}
}
