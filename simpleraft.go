package main

import (
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/peerstreaminc/raft"
)

// SimpleRaft A simple raft Impl
type SimpleRaft struct {
	server    raft.Server
	transport *raft.InmemTransport
	ra        *raft.Raft
	logStore  raft.LogStore // Shared - replicated data
	snapshot  raft.SnapshotStore

	// Local data is stored here. The leader will replicate its
	// data to the other nodes. In this example followers will have additional data
	// not yet replicated store here. Since they are attempting to apply their local
	// data the indexes will still be preserved. All the nodes will share the leader's
	// data in their logStore.
	Data sync.Map
}

// NewSimpleInMemRaftNode creates a new simple raft node using the inmem transport.
func NewSimpleInMemRaftNode(nodeID string) (*SimpleRaft, error) {

	addr, transport := raft.NewInmemTransport(raft.NewInmemAddr())

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)
	config.CommitTimeout = time.Second * 2

	config.HeartbeatTimeout = time.Second * 4
	config.ElectionTimeout = time.Second * 4 // must be >= heartbeat timeout
	config.LeaderLeaseTimeout = config.ElectionTimeout / 2

	// INMEM SNAPSHOTS & LOGSTORE
	// Create a in-memory snapshot store. Note using Inmem will avoid having
	// to write out files and hence elimintes the need for a directory to store them.
	snapshot := raft.NewInmemSnapshotStore()

	// Create the log store. We are told the Inmem store is only to be used for testing
	// however if in some cases we might want to rule out file stores for security reasons.
	logStore := raft.NewInmemStore()

	//////////////////////////////////////////////////////////////
	// ALTERNATIVE TO INMEM SNAPSHOTS AND LOGSTORE ...
	//
	// // Create a tmp directory for the snapshot and logstore files
	// raftDir := fmt.Sprintf("./tmp/%s", nodeID)
	// os.RemoveAll(path.Join(raftDir, "/")) //start fresh
	// fmt.Printf("%s deleted\n", raftDir)
	//
	// snapshot, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	// if err != nil {
	// 	fmt.Printf("file snapshot store: %s\n", err)
	// 	return nil, err
	// }
	//
	// logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	// if err != nil {
	// 	fmt.Printf("new bolt store: %s\n", err)
	// 	return nil, err
	// }
	//////////////////////////////////////////////////////////////

	// Instantiate the Raft systems.
	sr := SimpleRaft{}
	ra, err := raft.NewRaft(config, (*SimpleRaft)(&sr), logStore, logStore, snapshot, transport)
	if err != nil {
		log.Printf("new raft: %s\n", err)
		return nil, err
	}

	sr.ra = ra
	sr.server.ID = config.LocalID
	sr.server.Address = addr
	sr.transport = transport
	sr.logStore = logStore
	sr.snapshot = snapshot

	return &sr, nil
}

// PrintData prints the stored data.
func (sr *SimpleRaft) PrintData() {
	log.Println("------------------")
	log.Printf("%s\n", sr.server.ID)
	log.Println("------------------")

	log.Println("LOCAL DATA")
	var g raft.Log

	sr.Data.Range(func(key interface{}, val interface{}) bool {

		lval := ""
		err := sr.logStore.GetLog(key.(uint64), &g)
		if err != raft.ErrLogNotFound {
			lval = strings.TrimSpace(string(g.Data))
		}

		log.Printf("%s [%d, %s] logStore:%s\n", sr.server.ID, key, val, lval)

		return true
	})

	firstndx, err := sr.logStore.FirstIndex()
	if err != nil {
		log.Printf("Error getting logs first index while printing data: %s\n", err)
		return
	}
	lastndx, err := sr.logStore.LastIndex()
	if err != nil {
		log.Printf("Error getting logs last index while printing data: %s\n", err)
		return
	}

	log.Println("------------------")
	log.Printf("LOG STORE DATA indexes:(%d, %d)\n", firstndx, lastndx)

	for i := firstndx; i <= lastndx; i++ {
		err = sr.logStore.GetLog(i, &g)
		if err != raft.ErrLogNotFound {
			log.Printf("%s LOG STORE ENTRY: %s\n", sr.server.ID, strings.TrimSpace(string(g.Data)))
		}
	}

	log.Println("------------------")
	snaps, err := sr.snapshot.List()
	if err != nil {
		log.Printf("unable to list the snapshots %s\n", err)
	} else {
		log.Printf("%d SNAPSHOTS\n", len(snaps))
	}
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

	vData := []string{}

	sr.Data.Range(func(key interface{}, val interface{}) bool {
		vData = append(vData, val.(string))
		return true
	})

	return &MockSnapshot{Data: vData}, nil
}

// Restore raft FSM Interface Impl
func (sr *SimpleRaft) Restore(io.ReadCloser) error {
	log.Printf("%s RESTORE\n", sr.server.ID)
	return nil
}
