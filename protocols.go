package main

import (
	"math/rand"
	"time"
)

// Global variables for round-robin node selection
var (
	nodeOrder       []string // Ordered list of node addresses
	currentPointer  int      // Current position in the order
	lastMemberCount int      // To detect membership changes
)

// selectNextKNodes selects the next k nodes in round-robin fashion
func selectNextKNodes(k int, self string, membershipSnapshot map[string]Member) []string {
	// Build candidates list
	var candidates []string
	for nodeId, member := range membershipSnapshot {
		if nodeId != self && member.Status != StatusDead {
			candidates = append(candidates, member.ID())
		}
	}

	if len(candidates) == 0 {
		return []string{}
	}

	// Rebuild order if membership changed
	if len(candidates) != lastMemberCount || len(nodeOrder) == 0 {
		nodeOrder = candidates
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(nodeOrder), func(i, j int) {
			nodeOrder[i], nodeOrder[j] = nodeOrder[j], nodeOrder[i]
		})
		currentPointer = 0
		lastMemberCount = len(candidates)
	}

	// Select next k nodes
	if k > len(nodeOrder) {
		k = len(nodeOrder)
	}

	result := make([]string, k)
	for i := 0; i < k; i++ {
		result[i] = nodeOrder[currentPointer]
		currentPointer = (currentPointer + 1) % len(nodeOrder)
	}

	return result
}

// GOSSIP PROTOCOL
// gossipSend sends gossip messages to random nodes
func (s *Server) gossipSend(nodeCount int) {

	snapshot := s.Members.Snapshot()
	nodes := selectNextKNodes(nodeCount, s.ID(), snapshot)

	if len(nodes) == 0 {
		return // No nodes to gossip to
	}

	// Create optimized membership list with only necessary fields
	membersList := []Member{}
	for _, m := range snapshot {
		// Create minimal member with only essential fields
		optimizedMember := Member{
			Address:               m.Address,               // Keep for ID generation
			NodeCreationTimestamp: m.NodeCreationTimestamp, // Keep for ID generation
			Status:                m.Status,                // Essential for merge logic
			Heartbeat:             m.Heartbeat,             // Used in gossip protocol
			Incarnation:           m.Incarnation,           // Essential for merge logic
			Hash:                  m.Hash,                  // Include hash for distributed hashing
			// Skip LastUpdated - gets set to time.Now() during merge
		}
		membersList = append(membersList, optimizedMember)
	}

	for _, node := range nodes {
		go func(nodeId string) {
			resp, err := CallGossip(GetAddressFromID(nodeId), s.ID(), membersList, s)
			if err != nil {
				LogError(true, "Failed to send gossip to %s: %v", nodeId, err)
			} else {
				LogInfo(false, "Sent gossip to %s, success: %v", nodeId, resp.Success)
			}
		}(node)
	}
}

// SWIM PROTOCOL (Ping-Ack)
// pingSend sends ping messages to random nodes (SWIM protocol)
func (s *Server) pingSend(nodeCount int) {

	snapshot := s.Members.Snapshot()
	nodes := selectNextKNodes(nodeCount, s.ID(), snapshot)

	if len(nodes) == 0 {
		return // No nodes to ping
	}

	// Create optimized membership list with only necessary fields for PingAck
	membersList := []Member{}
	for _, m := range snapshot {
		// Create minimal member with only essential fields for PingAck protocol
		optimizedMember := Member{
			Address:               m.Address,               // Keep for ID generation
			NodeCreationTimestamp: m.NodeCreationTimestamp, // Keep for ID generation
			Status:                m.Status,                // Essential for merge logic
			Incarnation:           m.Incarnation,           // Essential for merge logic
			Hash:                  m.Hash,                  // Include hash for distributed hashing
			// Skip Heartbeat - not used in PingAck protocol
			// Skip LastUpdated - gets set to time.Now() during merge
		}
		membersList = append(membersList, optimizedMember)
	}

	for _, node := range nodes {
		go func(nodeId string) {
			resp, err := CallPing(GetAddressFromID(nodeId), s.ID(), membersList, s)
			if err != nil {
				LogError(true, "Failed to send ping to %s: %v", nodeId, err)
			} else {
				LogInfo(false, "PingAck: Received ACK from %s", nodeId)
				mergeMembership(s, resp.MembershipList, nodeId)
			}
		}(node)
	}
}
