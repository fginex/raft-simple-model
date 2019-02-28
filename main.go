package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/hashicorp/raft"
)

const numberOfNodes = 3 // The number of nodes in this simulation. Should be at least 3 to allow leader election.

//--
func main() {

	allNodes := []*SimpleRaft{}
	serverList := []raft.Server{}

	for i := 0; i < numberOfNodes; i++ {
		node, err := NewSimpleInMemRaftNode(fmt.Sprintf("node%d", i))
		if err != nil {
			log.Fatalf("unable to create raft node %s", err)
		}
		allNodes = append(allNodes, node)

		serverList = append(serverList, raft.Server{ID: node.server.ID, Address: node.server.Address})
	}

	configuration := raft.Configuration{Servers: serverList}

	for _, n := range allNodes {
		n.ra.BootstrapCluster(configuration)
	}

	for _, n := range allNodes {
		for _, peer := range allNodes {
			if n != peer {
				n.transport.Connect(peer.server.Address, peer.transport)
			}
		}
	}

	go simulationLoop(allNodes)

	//--
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
}

// simulationLoop simulates activity that might be a use case for leadership consensus
// and replication.
func simulationLoop(nodes []*SimpleRaft) {

	rnData := rand.New(rand.NewSource(time.Now().UnixNano()))
	time.Sleep(time.Millisecond * 125)
	rnNode := rand.New(rand.NewSource(time.Now().UnixNano()))

	tkInjectData := time.NewTicker(time.Second * 1)
	tkStats := time.NewTicker(time.Second * 5)
	tkPurge := time.NewTicker(time.Hour * 30)

	for {
		select {
		case <-tkInjectData.C:
			// Pick a random node and inject data into it. Notice how Apply will only be
			// replicated to the other nodes when it is invoked by the leader. Invoking it
			// on a non-leader node returns an error.
			randomNode := nodes[rnNode.Intn(len(nodes))]
			if randomNode == nil {
				continue
			}
			smsg := fmt.Sprintf("{%s.%d}", randomNode.server.ID, rnData.Intn(5000))
			bmsg := []byte(smsg)

			log.Printf("%s RECEIVED NEW DATA:%s Applying...\n", randomNode.server.ID, smsg)
			ft := randomNode.ra.Apply(bmsg, time.Second*5)
			err := ft.Error()
			if err != nil {
				log.Printf("%s error applying data %s", randomNode.server.ID, err)
			}

		case <-tkStats.C:
			leader := findLeader(nodes)
			if leader == nil {
				log.Println("NO LEADER SELECTED")
			} else {
				log.Printf("LEADER IS %s\n", leader.server.ID)
			}

			for _, n := range nodes {
				n.PrintData()
			}

		case <-tkPurge.C:
			// Purge the log stores periodically, we might want to force a snapshot before doing so.

			for _, n := range nodes {

				log.Printf("%s TAKING SNAPSHOT\n", n.server.ID)
				sf := n.ra.Snapshot()
				err := sf.Error()
				if err != nil {
					log.Printf("%s snapshot failed: %s\n", n.server.ID, err)
					continue
				}

				firstndx, err := n.logStore.FirstIndex()
				if err != nil {
					log.Printf("Error getting logs first index while printing data: %s\n", err)
					return
				}
				lastndx, err := n.logStore.LastIndex()
				if err != nil {
					log.Printf("Error getting logs last index while printing data: %s\n", err)
					return
				}
				n.logStore.DeleteRange(firstndx, lastndx-1)
				log.Printf("%s LOG STORE PURGED\n", n.server.ID)
			}
		}
	}
}

func findLeader(nodes []*SimpleRaft) (leader *SimpleRaft) {

	for _, n := range nodes {
		if n.ra.State() == raft.Leader {
			leader = n
			break
		}
	}

	return leader
}

func rpcCommandToString(command interface{}) string {
	return fmt.Sprintf("%T", command)
}
