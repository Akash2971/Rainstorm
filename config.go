package main

import (
	"strconv"
	"time"
)

type ProtocolType string

type SuspicionType string

var mergeInterval = 10 * time.Second
var garbageCollectorInterval = 45 * time.Second
var heartbeatInterval = 100 * time.Millisecond
var suspicionCheckTimeout = 1 * time.Second
var suspicionTimeout = 2 * time.Second
var deadTimeout = 2 * time.Second
var cleanUpTimeout = 10 * time.Second
var gossipOrSwimPingInterval = 100 * time.Millisecond
var PingFanout = 1
var GossipFanout = 1
var InitialProtocol = GossipProtocol
var InitialSuspicion = NoSuspect
var InitialMessageDropRate = 0.0
var AckTimeout = 1 * time.Second
var ReadTimeout = 1 * time.Second
var ReplicationFactor = 3

// Use gossipOrSwimPingInterval and GossipFanout in conjunction

const (
	GossipProtocol  ProtocolType = "gossip"
	PingAckProtocol ProtocolType = "ping"
)

const (
	Suspect   SuspicionType = "suspect"
	NoSuspect SuspicionType = "nosuspect"
)

var ProtocolMap = map[string]ProtocolType{
	string(GossipProtocol):  GossipProtocol,
	string(PingAckProtocol): PingAckProtocol,
}

var SuspicionMap = map[string]SuspicionType{
	string(Suspect):   Suspect,
	string(NoSuspect): NoSuspect,
}

var NodeMap = map[string]string{
	"vm1":  "fa25-cs425-3701.cs.illinois.edu:8080",
	"vm2":  "fa25-cs425-3702.cs.illinois.edu:8080",
	"vm3":  "fa25-cs425-3703.cs.illinois.edu:8080",
	"vm4":  "fa25-cs425-3704.cs.illinois.edu:8080",
	"vm5":  "fa25-cs425-3705.cs.illinois.edu:8080",
	"vm6":  "fa25-cs425-3706.cs.illinois.edu:8080",
	"vm7":  "fa25-cs425-3707.cs.illinois.edu:8080",
	"vm8":  "fa25-cs425-3708.cs.illinois.edu:8080",
	"vm9":  "fa25-cs425-3709.cs.illinois.edu:8080",
	"vm10": "fa25-cs425-3710.cs.illinois.edu:8080",
}

// AddressToVMName maps address to VM name (reverse of NodeMap)
var AddressToVMName = func() map[string]string {
	reverseMap := make(map[string]string)
	for vmName, address := range NodeMap {
		reverseMap[address] = vmName
	}
	return reverseMap
}()

var Config = struct {
	IntroducerAddr    string
	Protocol          ProtocolType
	Fanout            int
	AllNodes          []string
	Suspicion         SuspicionType
	MessageDropRate   float64 // Percentage of messages to drop (0.0 to 1.0)
	HashBits          int     // Number of bits for hash function
	ReplicationFactor int     // Number of replicas for HyDFS files
}{
	IntroducerAddr:    "fa25-cs425-3701.cs.illinois.edu:8080",
	Protocol:          InitialProtocol,
	Fanout:            PingFanout,             // Number of random nodes to gossip to
	AllNodes:          []string{},             // Will be populated dynamically
	Suspicion:         InitialSuspicion,       // Enable suspicion mechanism by default
	MessageDropRate:   InitialMessageDropRate, // No message drop by default
	HashBits:          8,                      // Default to 8 bits for hash function
	ReplicationFactor: ReplicationFactor,      // Default to 3 replicas (including primary)
}

// SwitchProtocol and ToggleSuspicion allows dynamic protocol and suspicion type switching at runtime
func SwitchProtocol(newProtocol ProtocolType, newSuspicionType SuspicionType) {
	Config.Protocol = newProtocol
	Config.Suspicion = newSuspicionType
	if newProtocol == PingAckProtocol {
		Config.Fanout = PingFanout
	} else {
		Config.Fanout = GossipFanout
	}
	LogInfo(true, "New Configs: {%s, %s}", newProtocol, newSuspicionType)
}

// SetMessageDropRate sets the message drop rate for testing network failures
func SetMessageDropRate(percentage string) {
	dropRate, err := strconv.ParseFloat(percentage, 64)
	if err != nil {
		ConsolePrintf("Invalid percentage: %s\n", percentage)
		return
	}

	if dropRate < 0.0 {
		dropRate = 0.0
	}
	if dropRate > 1.0 {
		dropRate = 1.0
	}
	Config.MessageDropRate = dropRate
	ConsolePrintf("Message drop rate set to: %.2f%%\n", dropRate*100)
}
