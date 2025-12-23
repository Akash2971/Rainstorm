package main

import (
	"sort"
	"sync"
	"time"
)

type MembershipList struct {
	mu    sync.Mutex
	nodes []Member // Sorted slice by hash value
}

func NewMembershipList() *MembershipList {
	return &MembershipList{nodes: make([]Member, 0)}
}

func (ml *MembershipList) AddOrUpdate(member Member) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	// Find existing member by ID
	for i, existingMember := range ml.nodes {
		if existingMember.ID() == member.ID() {
			// Update existing member
			ml.nodes[i] = member
			// Sort after update
			ml.sortNodes()
			return
		}
	}

	// Add new member
	ml.nodes = append(ml.nodes, member)
	// Sort after addition
	ml.sortNodes()
}

// sortNodes sorts the nodes by hash value in ascending order
func (ml *MembershipList) sortNodes() {
	if len(ml.nodes) <= 1 {
		return // No need to sort single element or empty list
	}

	sort.Slice(ml.nodes, func(i, j int) bool {
		// Compare directly using Member.Hash which is already big.Int
		return ml.nodes[i].Hash.Cmp(&ml.nodes[j].Hash) < 0
	})
}

func (ml *MembershipList) Remove(nodeID string) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	// Find and remove the member
	for i, member := range ml.nodes {
		if member.ID() == nodeID {
			// Remove by slicing (maintains order)
			ml.nodes = append(ml.nodes[:i], ml.nodes[i+1:]...)
			return
		}
	}
}

func (ml *MembershipList) MarkSuspectIfNeeded(suspicionTimeout, deadTimeout, cleanUpTimeout time.Duration) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	changed := false
	now := time.Now()

	// Collect changes to avoid modifying slice during iteration
	var toUpdate []Member
	var toRemoveIndices []int

	for i, m := range ml.nodes {

		// Skip self check for suspicion if protocol is SWIM
		if Config.Protocol == PingAckProtocol {
			if m.Address == globalServer.Addr {
				continue
			}
		}

		elapsed := now.Sub(m.LastUpdated)

		if m.Status == StatusAlive && elapsed > suspicionTimeout {
			switch Config.Suspicion {
			case Suspect:
				m.MarkSuspect()
				toUpdate = append(toUpdate, m)
				LogInfo(true, "SUSPECT_DETECTED: Marked member as SUSPECT: %s", m.ID())
				ConsolePrintf("SUSPECT_DETECTED: Marked member as SUSPECT: %s\n", m.ID())
				changed = true
			case NoSuspect:
				// In no-suspicion mode, only mark as dead if it crosses dead timeout
				if elapsed > deadTimeout {
					m.MarkDead()
					handleDeadNode(globalServer, m)
					toUpdate = append(toUpdate, m)
					LogInfo(true, "FAILURE_DETECTED: Marked member as DEAD: %s", m.ID())
					ConsolePrintf("FAILURE_DETECTED: Marked member as DEAD: %s\n", m.ID())
					changed = true
				}
			}
		} else if m.Status == StatusSuspect && elapsed > deadTimeout {
			m.MarkDead()
			toUpdate = append(toUpdate, m)
			handleDeadNode(globalServer, m)
			LogInfo(true, "FAILURE_CONFIRMED: Marked suspected member as DEAD: %s", m.ID())
			ConsolePrintf("FAILURE_CONFIRMED: Marked suspected member as DEAD: %s\n", m.ID())
			changed = true
		} else if m.Status == StatusDead && elapsed > cleanUpTimeout {
			toRemoveIndices = append(toRemoveIndices, i)
			LogInfo(true, "MEMBER_CLEANUP: Removed dead member from list: %s", m.ID())
			ConsolePrintf("MEMBER_CLEANUP: Removed dead member from list: %s\n", m.ID())
			changed = true
		}
	}

	// Apply updates after iteration
	for _, member := range toUpdate {
		// Find and update the member in the slice
		for i, existingMember := range ml.nodes {
			if existingMember.ID() == member.ID() {
				ml.nodes[i] = member
				break
			}
		}
	}

	// Apply removals after iteration (in reverse order to maintain indices)
	for i := len(toRemoveIndices) - 1; i >= 0; i-- {
		idx := toRemoveIndices[i]
		ml.nodes = append(ml.nodes[:idx], ml.nodes[idx+1:]...)
	}

	// Sort after all updates and removals
	if changed {
		ml.sortNodes()
	}

	return changed
}

func (ml *MembershipList) Snapshot() map[string]Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	cp := make(map[string]Member)
	for _, member := range ml.nodes {
		cp[member.ID()] = member
	}
	// returns Nodes Key - ID, Nodes Value - Member
	return cp
}

func (ml *MembershipList) GetAll() []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	// Return a copy of the slice (already sorted)
	members := make([]Member, len(ml.nodes))
	copy(members, ml.nodes)
	return members
}

func (ml *MembershipList) Print(printOnConsole bool) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if printOnConsole {
		ConsolePrintln("---- Membership List ----")
	}

	// Print members (already sorted by hash)
	for _, member := range ml.nodes {
		if Config.Protocol == GossipProtocol {
			if printOnConsole {
				ConsolePrintf("VMName: %s | Member: %s | Hash: %s | Status:  %s| Heartbeat: %d| Incarnation: %d\n", AddressToVMName[member.Address], member.ID(), member.Hash.String(), member.Status, member.Heartbeat, member.Incarnation)
			}
			LogInfo(true, "VMName: %s | Member: %s | Hash: %s| Status:  %s| Heartbeat: %d| Incarnation: %d\n", AddressToVMName[member.Address], member.ID(), member.Hash.String(), member.Status, member.Heartbeat, member.Incarnation)
		}
		if Config.Protocol == PingAckProtocol {
			if printOnConsole {
				ConsolePrintf("VMName: %s | Member: %s | Hash: %s | Status:  %s| Incarnation: %d \n", AddressToVMName[member.Address], member.ID(), member.Hash.String(), member.Status, member.Incarnation)
			}
			LogInfo(true, "VMName: %s | Member: %s | Hash: %s | Status:  %s| Incarnation: %d \n", AddressToVMName[member.Address], member.ID(), member.Hash.String(), member.Status, member.Incarnation)
		}
	}
	if printOnConsole {
		ConsolePrintln("-------------------------")
	}
}

func (ml *MembershipList) PrintSuspectedNodes() {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	ConsolePrintln("---- Membership List ----")
	for _, member := range ml.nodes {
		if member.Status == StatusSuspect {
			if Config.Protocol == GossipProtocol {
				ConsolePrintf("Suspected Member: %s | Status:  %s| Heartbeat: %d| Incarnation: %d| Hash: %s| LastUpdated: %s\n", member.ID(), member.Status, member.Heartbeat, member.Incarnation, member.Hash.String(), member.LastUpdated.Format(time.RFC3339))
				LogInfo(true, "Suspected Member: %s | Status:  %s| Heartbeat: %d| Incarnation: %d| Hash: %s| LastUpdated: %s\n", member.ID(), member.Status, member.Heartbeat, member.Incarnation, member.Hash.String(), member.LastUpdated.Format(time.RFC3339))
			}
			if Config.Protocol == PingAckProtocol {
				ConsolePrintf("Suspected Member: %s | Status:  %s| Incarnation: %d| Hash: %s\n", member.ID(), member.Status, member.Incarnation, member.Hash.String())
				LogInfo(true, "Suspected Member: %s | Status:  %s| Incarnation: %d| Hash: %s\n", member.ID(), member.Status, member.Incarnation, member.Hash.String())
			}
		}
	}
	ConsolePrintln("-------------------------")

}

// GetSuspectedNodes returns all suspected nodes from the membership list
func (ml *MembershipList) GetSuspectedNodes() []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	var suspectedNodes []Member
	for _, member := range ml.nodes {
		if member.Status == StatusSuspect {
			suspectedNodes = append(suspectedNodes, member)
		}
	}
	return suspectedNodes
}

// GetAliveMembers returns all alive members from the membership list
// If excludeAddress is not empty, it excludes the member with that address
func (ml *MembershipList) GetAliveMembers(excludeAddress string) []Member {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	var aliveMembers []Member
	for _, member := range ml.nodes {
		if member.IsAlive() && member.Address != excludeAddress {
			aliveMembers = append(aliveMembers, member)
		}
	}
	return aliveMembers
}
