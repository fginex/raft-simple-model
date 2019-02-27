package main

import (
	"log"

	"github.com/hashicorp/go-msgpack/codec"
	"github.com/peerstreaminc/raft"
)

// MockSnapshot is a simple snapshot implementation
type MockSnapshot struct {
	Data []string
}

// Persist - raft.FSMSnapshot interface
func (m *MockSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Println("MockSnapshot Persist()")

	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(sink, &hd)
	if err := enc.Encode(m.Data[:len(m.Data)]); err != nil {
		sink.Cancel()
		return err
	}
	sink.Close()

	return nil
}

// Release - raft.FSMSnapshot interface
func (m *MockSnapshot) Release() {
	log.Println("MockSnapshot Release()")
}
