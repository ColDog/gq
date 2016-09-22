package bolt

import "testing"

func TestOpen(t *testing.T) {
	s := NewBoltStore("test")
	s.Open()
	s.Close()
	s.Drop()
}
