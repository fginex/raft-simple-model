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

const numberOfNodes = 3

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

	go func(nodes []*SimpleRaft) {
		rnData := rand.New(rand.NewSource(time.Now().UnixNano()))
		time.Sleep(time.Millisecond * 125)
		rnNode := rand.New(rand.NewSource(time.Now().UnixNano()))

		var leader *SimpleRaft

		for {
			time.Sleep(time.Second * 5)

			leader = nil

			for _, n := range nodes {
				if n.ra.State() == raft.Leader {
					leader = n
					break
				}
			}

			if leader == nil {
				log.Println("NO LEADER ELECTED")
				continue
			}

			log.Printf("LEADER IS %s\n", leader.server.ID)

			// Pick a random node and have it Apply data to the other nodes
			smsg := fmt.Sprintf("%d\n", rnData.Intn(5000))
			bmsg := []byte(smsg)
			nodes[rnNode.Intn(len(nodes)-1)].ra.Apply(bmsg, time.Second*5)

			for _, n := range nodes {
				n.PrintData()
			}
		}

	}(allNodes)

	//--
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
}

func rpcCommandToString(command interface{}) string {
	return fmt.Sprintf("%T", command)
}
