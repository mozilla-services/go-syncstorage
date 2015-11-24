package syncstorage

type Dispatch struct {
	writers map[string]DB
}

func NewDispatch() *Dispatch {

	return &Dispatch{
		writers: make(map[string]DB),
	}
}
