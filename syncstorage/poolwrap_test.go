package syncstorage

import (
	"strconv"
	"time"
)

func newPoolwrap() *poolwrap {
	uid := strconv.FormatInt(time.Now().UnixNano(), 36)
	return newPoolwrapUID(uid)
}

func newPoolwrapUID(uid string) *poolwrap {

	p, err := NewPool(getTempBase())
	if err != nil {
		panic(err)
	}

	return &poolwrap{uid: uid, pool: p}
}
