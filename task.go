package main

import (
	"sync"
	"time"
)

// Tuple represents a data tuple flowing through the streaming topology
type Tuple struct {
	ID        string    // Unique tuple identifier
	Key       string    // Tuple key
	Value     string    // Tuple value
	Timestamp time.Time // Timestamp when tuple was created
	SenderID  string    // Upstream task ID that sent this tuple
	TaskID    string    // Target task ID for routing (where this tuple should be sent)
	EOF       *bool     `json:"EOF,omitempty"` // Optional EOF marker
}

// TaskStatus represents the current status of a task (as integer enum)
type TaskStatus int

const (
	TaskRunning TaskStatus = iota
	TaskFailed
	TaskStopped
)

// Task represents a single task in the streaming topology
type Task struct {
	TaskID        string            `json:"task_id"`        // Unique task identifier
	Stage         int               `json:"stage"`          // Stage number this task belongs to
	NodeID        string            `json:"node_id"`        // Node hosting this task
	Address       string            `json:"address"`        // Full address (host:port) where this task listens
	InputTasks    map[string]string `json:"input_tasks"`    // Upstream taskID -> address (for ACKs)
	OutputTargets map[string]string `json:"output_targets"` // Downstream taskID -> address
	HydfsLogPath  string            `json:"hydfs_log_path"` // Path to HyDFS log file
	ExactlyOnce   bool              `json:"exactly_once"`   // Enable exactly-once semantics
	AutoscaleMode bool              `json:"autoscale_mode"` // Enable autoscaling for this task
	Mutex         sync.Mutex        `json:"-"`              // Mutex for thread-safe access (not serialized)
}

// NewTask creates a new task instance
func NewTask(taskID string, stage int) *Task {
	return &Task{
		TaskID:        taskID,
		Stage:         stage,
		InputTasks:    make(map[string]string),
		OutputTargets: make(map[string]string),
		ExactlyOnce:   false,
		AutoscaleMode: false,
	}
}

// AddInputTask adds an upstream task with its address (for ACKs)
func (t *Task) AddInputTask(taskID string, address string) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.InputTasks[taskID] = address
}

// AddOutputTarget adds a downstream task target with its address
func (t *Task) AddOutputTarget(taskID string, address string) {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	t.OutputTargets[taskID] = address
}
