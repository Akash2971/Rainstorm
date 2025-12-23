package main

import (
	"fmt"
	"math/big"
	"time"
)

type Status string

const (
	StatusAlive          Status = "alive"
	StatusSuspect        Status = "suspect"
	StatusDead           Status = "dead"
	StatusVoluntaryLeave Status = "voluntary_leave"
)

// TO GET A MEMBER, ALWAYS USE ID
type Member struct {
	Address               string
	NodeCreationTimestamp time.Time
	Status                Status
	Heartbeat             uint64
	Incarnation           uint64
	LastUpdated           time.Time
	Hash                  big.Int
}

func (m *Member) ID() string {
	return fmt.Sprintf("%s-%d", m.Address, m.NodeCreationTimestamp.Unix())
}

func (m *Member) MarkSuspect() {
	m.Status = StatusSuspect
	m.LastUpdated = time.Now()
}

func (m *Member) MarkDead() {
	m.Status = StatusDead
	m.LastUpdated = time.Now()
}

func (m *Member) MarkVoluntaryLeave() {
	m.Status = StatusVoluntaryLeave
	m.LastUpdated = time.Now()
}

func (m *Member) IsAlive() bool {
	return m.Status == StatusAlive
}
