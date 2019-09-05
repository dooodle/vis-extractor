package main

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestSubSets(t *testing.T) {
	buf := bytes.Buffer{}
	keys := []string{"a","b","c","d"}
	var f = func(w io.Writer, entity string, k1 string, k2 string) {
		str := fmt.Sprintf("%s%s ",k1,k2)
		w.Write([]byte(str))
	}
	subsetsForCompound(&buf,nil,"test_entity",keys,f)
	want := "ab ac ad bc bd cd "
	if buf.String() != want {
		t.Errorf("wanted %s got %s",want,buf.String())
	}
}
