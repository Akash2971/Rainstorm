package main

import (
	"time"
)

// mergeMembership merges the received membership list with local membership
func mergeMembership(server *Server, receivedMembers []Member, senderId string) {
	localSnapshot := server.Members.Snapshot()

	for _, receivedMember := range receivedMembers {

		if Config.Protocol == GossipProtocol && receivedMember.Status == StatusDead {
			LogInfo(false, "FAILURE_COMMUNICATED: Received dead node status from %s: %s during merge", senderId, receivedMember.ID())
			continue
		}

		// Skip processing if local member is already dead and received member is also dead
		_, exists := localSnapshot[receivedMember.ID()]

		// Track join: newly seen, not dead
		if !exists && receivedMember.Status != StatusDead {
			receivedMember.LastUpdated = time.Now()
			server.Members.AddOrUpdate(receivedMember)
			LogInfo(true, "MEMBER_JOIN: Added new member: %s with hash: %s during merge", receivedMember.ID(), receivedMember.Hash)
			handleNewlyJoinedNode(server, receivedMember)
			continue
		} else if !exists && receivedMember.Status == StatusDead {
			continue // Ignore dead node join edge-case SWIM
		}

		localMember := localSnapshot[receivedMember.ID()]

		// Track failure: present and alive in local, now seen as dead/left
		if (localMember.Status == StatusAlive || localMember.Status == StatusSuspect) && (receivedMember.Status == StatusDead || receivedMember.Status == StatusVoluntaryLeave) {
			handleDeadNode(server, receivedMember)
		}

		// Special case: Handle self-node with suspicion enabled
		if receivedMember.ID() == server.ID() {
			if receivedMember.Status == StatusSuspect && localMember.Status == StatusAlive &&
				receivedMember.Incarnation >= localMember.Incarnation {
				// We are alive but others think we are suspect - increment incarnation
				server.IncarnationNumber++
				localMember.Incarnation = server.IncarnationNumber
				localMember.LastUpdated = time.Now()
				server.Members.AddOrUpdate(localMember)
				LogInfo(true, "Incremented incarnation to %d - I'm alive but was marked as suspect", server.IncarnationNumber)
				continue
			}
			if receivedMember.Status == StatusDead {
				// If others think we are dead, do nothing (we know we are alive)
				continue
			}
		}

		var updatedMember Member

		switch Config.Suspicion {
		case Suspect:
			updatedMember = handleSuspicionMerge(server, localMember, receivedMember, senderId)
		case NoSuspect:
			updatedMember = handleNoSuspicionMerge(server, localMember, receivedMember, senderId)
		}

		server.Members.AddOrUpdate(updatedMember)

	}

}

// handleSuspicionMerge handles merge logic with suspicion enabled
// Priority: Dead/VoluntaryLeave > Incarnation > Suspect > Alive > Heartbeat (Gossip only)
func handleSuspicionMerge(server *Server, localMember Member, receivedMember Member, receiverId string) Member {
	// Rule 1: Dead/VoluntaryLeave always wins (overrides everything, even incarnation number)
	if receivedMember.Status == StatusDead || receivedMember.Status == StatusVoluntaryLeave {
		handleDeadNode(server, receivedMember)
		if localMember.Status != receivedMember.Status {
			LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed to %s (from %s) during merge", receivedMember.ID(), receivedMember.Status, localMember.Status)
		}
		return receivedMember
	}
	if localMember.Status == StatusDead || localMember.Status == StatusVoluntaryLeave {
		return localMember // Keep local final status
	}

	// Rule 2: Higher incarnation number always takes priority
	if receivedMember.Incarnation > localMember.Incarnation {
		receivedMember.LastUpdated = time.Now()
		return receivedMember
	}
	if localMember.Incarnation > receivedMember.Incarnation {
		return localMember // Keep local (higher incarnation)
	}

	// Rule 3: Same incarnation number - status priority (Suspect > Alive)
	if receivedMember.Incarnation == localMember.Incarnation {
		// If both have same status and same incarnation
		if receivedMember.Status == localMember.Status {
			// For same status and incarnation, protocol-specific logic
			switch Config.Protocol {
			case GossipProtocol:
				// For gossip: higher heartbeat wins
				if receivedMember.Heartbeat > localMember.Heartbeat {
					receivedMember.LastUpdated = time.Now()
					return receivedMember
				}
				return localMember
			case PingAckProtocol:
				if receivedMember.ID() == receiverId {
					receivedMember.LastUpdated = time.Now()
					return receivedMember
				}
			}
		} else {
			// Different status, same incarnation: Suspect > Alive
			if compareStatus(receivedMember.Status, localMember.Status) {
				LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed from %s to %s during merge", receivedMember.ID(), localMember.Status, receivedMember.Status)
				receivedMember.LastUpdated = time.Now()
				return receivedMember
			}
			return localMember
		}
	}

	// Default: keep local
	return localMember
}

// handleNoSuspicionMerge handles merge logic without suspicion
func handleNoSuspicionMerge(server *Server, localMember Member, receivedMember Member, receiverId string) Member {
	if receivedMember.Status == StatusDead || receivedMember.Status == StatusVoluntaryLeave {
		if localMember.Status != receivedMember.Status {
			LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed to %s (from %s) during merge", receivedMember.ID(), receivedMember.Status, localMember.Status)
		}
		return receivedMember
	}
	if localMember.Status == StatusDead || localMember.Status == StatusVoluntaryLeave {
		return localMember // Keep local final status
	}
	if receivedMember.Status == localMember.Status {
		switch Config.Protocol {
		case GossipProtocol:
			// For gossip without suspicion: only return if higher heartbeat
			if receivedMember.Heartbeat > localMember.Heartbeat {
				receivedMember.LastUpdated = time.Now()

				return receivedMember
			}
		case PingAckProtocol:
			if receivedMember.ID() == receiverId {
				receivedMember.LastUpdated = time.Now()
				return receivedMember
			}
		}
	} else {
		// Different status, same incarnation: Suspect > Alive
		if compareStatus(receivedMember.Status, localMember.Status) {
			LogInfo(true, "MEMBER_STATUS_CHANGE: Member %s status changed from %s to %s during merge", receivedMember.ID(), localMember.Status, receivedMember.Status)
			receivedMember.LastUpdated = time.Now()
			return receivedMember
		}
		return localMember
	}
	return localMember
}

// compareStatus returns true if received should replace local
func compareStatus(received, local Status) bool {
	return getStatusPriority(received) > getStatusPriority(local)
}

// getStatusPriority: Dead = VoluntaryLeave > Suspect > Alive
func getStatusPriority(status Status) int {
	switch status {
	case StatusDead, StatusVoluntaryLeave:
		return 3 // Both dead and voluntary leave have highest priority
	case StatusSuspect:
		return 2
	case StatusAlive:
		return 1
	default:
		return 0
	}
}

func handleDeadNode(server *Server, m Member) {
	if server.FailedPendingStabilization[m.ID()] == FailedNode {
		return
	}
	server.FailedPendingStabilization[m.ID()] = FailedNode
	go func() {
		server.handleReplicationWindowChange(m.ID())
	}()
}

func handleNewlyJoinedNode(server *Server, m Member) {
	if server.NewlyJoinedPendingStabilization[m.ID()] == NewlyJoined {
		return
	}
	server.NewlyJoinedPendingStabilization[m.ID()] = NewlyJoined
	go func() {
		server.handleReplicationWindowChange(m.ID())
	}()
}
