package web

import (
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTwoLevelPath(t *testing.T) {
	assert := assert.New(t)

	{
		paths := TwoLevelPath("1234567")
		if assert.Len(paths, 2) {
			assert.Equal("76", paths[0])
			assert.Equal("54", paths[1])
		}
	}

	{ // too short, expects it to use bath path
		paths := TwoLevelPath("1")
		assert.Len(paths, 0)
	}
}

func TestHandlerPoolPathAndFile(t *testing.T) {
	assert := assert.New(t)
	handler := newHandlerPool("/tmp", 1, nil, nil)

	{
		path, filename := (handler.PathAndFile("12345"))
		assert.Equal("/tmp/54/32", path)
		assert.Equal("12345.db", filename)
	}

	{
		path, filename := (handler.PathAndFile("123"))
		assert.Equal("/tmp/32", path)
		assert.Equal("123.db", filename)
	}
}

func TestHandlerPoolGetElement(t *testing.T) {
	assert := assert.New(t)

	tmpdir, err := ioutil.TempDir("", "")
	if !assert.NoError(err) {
		return
	}

	handler := newHandlerPool(tmpdir, 1, nil, nil)
	el, created, err := handler.getElement("123456")
	if assert.NoError(err) {
		assert.NotEmpty(el)
		assert.True(created)
	}
}

func TestHandlerPoolGetElementParallel(t *testing.T) {
	t.Parallel()

	assert := assert.New(t)
	handler := newHandlerPool(":memory:", 2, nil, nil)

	stop := make(chan struct{})
	errChan := make(chan error)
	errorList := []error{}

	// consume and fill errorList
	go func() {
		for {
			select {
			case err := <-errChan:
				fmt.Println(err)
				errorList = append(errorList, err)
			case <-stop:
				return
			}
		}
	}()

	// run a bunch of concurrent getters, there should be no errors
	var wg sync.WaitGroup
	uids := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	wg.Add(len(uids))
	for _, uid := range uids {
		go func(uid string) {
			for i := 0; i < 200; i++ {
				element, _, err := handler.getElement(uid)

				if err != nil {
					// when an element is in the pool but it is in the process
					// of stopping this error occurs. Since this is expected behaviour
					// sleep for a bit to give the element time to shutdown
					if err == errElementStopped {
						time.Sleep(time.Microsecond)
					} else {
						errChan <- err
					}
				} else {
					if !assert.Equal(uid, element.uid) {
						errChan <- fmt.Errorf("Expected match, got %s expected %s", element.uid, uid)
					}
				}
			}
			wg.Done()
		}(uid)
	}

	wg.Wait()
	stop <- struct{}{} // stop the error filler

	if !assert.Len(errorList, 0) {
		for _, err := range errorList {
			fmt.Println(err.Error())
		}
	}

}
