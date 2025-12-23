package main

import "sync"

type BandwidthStats struct {
	BytesSent     uint64
	BytesReceived uint64
	mu            sync.RWMutex
}

func (bs *BandwidthStats) AddSent(bytes uint64) {
	bs.mu.Lock()
	bs.BytesSent += bytes
	bs.mu.Unlock()
}

func (bs *BandwidthStats) AddReceived(bytes uint64) {
	bs.mu.Lock()
	bs.BytesReceived += bytes
	bs.mu.Unlock()
}

func (bs *BandwidthStats) GetAndReset() (sent, received uint64) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	sent = bs.BytesSent
	received = bs.BytesReceived
	bs.BytesSent = 0
	bs.BytesReceived = 0
	return
}
