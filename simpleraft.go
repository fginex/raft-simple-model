package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// SimpleRaft A simple raft Impl
type SimpleRaft struct {
	server    raft.Server
	transport *raft.InmemTransport
	ra        *raft.Raft
	Data      sync.Map
}

// NewSimpleInMemRaftNode creates a new simple raft node using the inmem transport.
func NewSimpleInMemRaftNode(nodeID string) (*SimpleRaft, error) {

	addr, transport := raft.NewInmemTransport(raft.NewInmemAddr())
	raftDir := fmt.Sprintf("./tmp/%s", nodeID)

	os.RemoveAll(path.Join(raftDir, "/")) //start fresh
	log.Printf("%s deleted\n", raftDir)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.CommitTimeout = time.Second * 2

	config.HeartbeatTimeout = time.Second * 4
	config.ElectionTimeout = time.Second * 4 // must be >= heartbeat timeout
	config.LeaderLeaseTimeout = config.ElectionTimeout / 2

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		log.Printf("file snapshot store: %s\n", err)
		return nil, err
	}

	// Create the log store and stable store.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		log.Printf("new bolt store: %s\n", err)
		return nil, err
	}

	// Instantiate the Raft systems.
	sr := SimpleRaft{}
	ra, err := raft.NewRaft(config, (*SimpleRaft)(&sr), logStore, logStore, snapshots, transport)
	if err != nil {
		log.Printf("new raft: %s\n", err)
		return nil, err
	}

	sr.ra = ra
	sr.server.ID = config.LocalID
	sr.server.Address = addr
	sr.transport = transport

	return &sr, nil
}

// PrintData prints the stored data.
func (sr *SimpleRaft) PrintData() {
	log.Println("------------------")
	sr.Data.Range(func(key interface{}, val interface{}) bool {
		log.Printf("%s [%d, %s]\n", sr.server.ID, key, val)
		return true
	})
	log.Println("------------------")
}

// Apply raft FSM Interface Impl
func (sr *SimpleRaft) Apply(g *raft.Log) interface{} {
	sdata := strings.TrimSpace(string(g.Data))
	log.Printf("%s APPLY New Data:%s\n", sr.server.ID, sdata)
	sr.Data.Store(g.Index, sdata)
	return g
}

// Snapshot raft FSM Interface Impl
func (sr *SimpleRaft) Snapshot() (raft.FSMSnapshot, error) {
	log.Printf("%s SNAPSHOT\n", sr.server.ID)
	return nil, nil
}

// Restore raft FSM Interface Impl
func (sr *SimpleRaft) Restore(io.ReadCloser) error {
	log.Printf("%s RESTORE\n", sr.server.ID)
	return nil
}

// Persist FSMSnapshot Interface Impl
func (sr *SimpleRaft) Persist(sink raft.SnapshotSink) error {
	log.Printf("%s FSMSnapshot Persist\n", sr.server.ID)
	return nil
}

// Release FSMSnapshot Interface Ipl
func (sr *SimpleRaft) Release() {
	log.Printf("%s FSMSnapshot Release\n", sr.server.ID)
}
