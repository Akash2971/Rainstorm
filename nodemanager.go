package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const metricsCollectionInterval = 2 * time.Second
const outputFlushInterval = 5 * time.Second

// TaskMetrics represents metrics for a task
type TaskMetrics struct {
	TaskID          string
	TuplesProcessed int64
	TuplesSent      int64
	TuplesReceived  int64
	Throughput      float64 // Tuples per second
}

// metricsHistory holds previous metrics snapshot data used to compute throughput.
type metricsHistory struct {
	LastProcessed int64
	LastReceived  int64
	LastReported  time.Time
}

// TaskInfo represents task information needed by NodeManager for management
type TaskInfo struct {
	TaskID         string              // Task identifier
	Task           *Task               // Reference to the task
	PID            int                 // Process ID of the command process
	Process        *os.Process         // Process handle
	Stdin          io.WriteCloser      // Stdin pipe writer for sending tuples to the process
	Stdout         io.ReadCloser       // Stdout pipe reader for reading tuples from the process
	ExactlyOnceMgr *ExactlyOnceManager // Exactly-once semantics manager
}

// NodeManager manages all tasks running on a single node
type NodeManager struct {
	NodeID         string                    // Node identifier
	TaskInfos      map[string]*TaskInfo      // Map of task ID to TaskInfo
	TaskStatus     map[string]TaskStatus     // Map of task ID to TaskStatus
	Metrics        map[string]*TaskMetrics   // Map of task ID to TaskMetrics
	WALFiles       map[string]*os.File       // Map of task ID to WAL file handle (for exactly-once)
	server         *Server                   // Reference to the server
	currentJobSpec *JobSpec                  // Current job spec (set during InitializeTasks)
	inputRate      int                       // Input rate (tuples per second)
	leaderAddress  string                    // Leader address for sending metrics
	metricsHistory map[string]metricsHistory // Internal history for throughput calculations
	mu             sync.RWMutex              // Mutex for thread-safe access

	// Buffered HyDFS output for final-stage tasks. Multiple final-stage tasks on
	// a node append their JSON-line output into outputBuffer, which is flushed
	// periodically to HyDFS via AppendOrCreateFromBuffer. Protected by
	// outputBufferMu for safe concurrent access from task goroutines and the
	// background flush loop.
	outputBuffer   []byte
	outputBufferMu sync.Mutex
}

// incrementTaskMetrics safely increments per-task counters (by 1) based on boolean flags, lazily creating TaskMetrics if needed.
func (nm *NodeManager) incrementTaskMetrics(taskID string, incProcessed, incSent, incReceived bool) {
	if taskID == "" {
		return
	}

	nm.mu.Lock()
	defer nm.mu.Unlock()

	metrics, exists := nm.Metrics[taskID]
	if !exists {
		metrics = &TaskMetrics{TaskID: taskID}
		nm.Metrics[taskID] = metrics
	}

	if incProcessed {
		metrics.TuplesProcessed++
	}

	if incSent {
		metrics.TuplesSent++
	}

	if incReceived {
		metrics.TuplesReceived++
	}
}

// NewNodeManager creates a new node manager instance
// Sets NodeID to address:port+2000 (where address:port is the node address passed in)
func NewNodeManager(nodeAddr string, server *Server) *NodeManager {
	// Calculate NodeID as address:port+2000 from the node address
	calculatedNodeID := nodeAddr
	host, portStr, err := net.SplitHostPort(nodeAddr)
	if err == nil {
		if port, err := strconv.Atoi(portStr); err == nil {
			calculatedNodeID = fmt.Sprintf("%s:%d", host, port+2000)
		}
	}

	nm := &NodeManager{
		NodeID:         calculatedNodeID,
		TaskInfos:      make(map[string]*TaskInfo),
		TaskStatus:     make(map[string]TaskStatus),
		Metrics:        make(map[string]*TaskMetrics),
		WALFiles:       make(map[string]*os.File),
		server:         server,
		metricsHistory: make(map[string]metricsHistory),
		outputBuffer:   make([]byte, 0),
	}
	// Start monitor goroutine
	go nm.startMonitor()
	// Start background flush loop for buffered HyDFS output.
	go nm.startOutputFlushLoop()
	return nm
}

// InitializeTasks initializes tasks on this node based on topology and assignments (RPC entry point)
func (nm *NodeManager) InitializeTasks(topology *Topology, assignments map[string]string, jobSpec *JobSpec, sourceDir string, inputRate int) error {
	// Update leader address from jobSpec if available
	if nm.leaderAddress == "" && jobSpec != nil && jobSpec.SourceAddress != "" {
		// Extract leader address from SourceAddress (remove port+2000 if present, use base port)

		host, portStr, _ := net.SplitHostPort(jobSpec.SourceAddress)
		port, _ := strconv.Atoi(portStr)
		// Leader control plane is on :5000
		nm.leaderAddress = fmt.Sprintf("%s:%d", host, port-2000)
		LogInfo(true, "NodeManager: Set leader address to %s from job spec", nm.leaderAddress)
	} else if nm.leaderAddress == "" && nm.server != nil && nm.server.IsIntroducer {
		nm.leaderAddress = nm.server.Addr
		LogInfo(true, "NodeManager: Set leader address to %s (this node is introducer)", nm.leaderAddress)
	}

	LogInfo(true, "NodeManager: Initializing tasks from topology on node %s", nm.NodeID)

	// Find all tasks whose address matches this node's NodeID (address:port+2000)
	for taskID, task := range topology.Tasks {
		// Check if task's address matches this node's NodeID
		if task.Address == nm.NodeID {
			// Use InitializeTask to initialize each task (reuses common logic)
			err := nm.InitializeTask(task, jobSpec, sourceDir, inputRate)
			if err != nil {
				return fmt.Errorf("failed to initialize task %s: %w", taskID, err)
			}
		}
	}

	return nil
}

// InitializeTask initializes a single task on this node (public method for RPC)
// This is used when adding a new task to an existing job or when initializing multiple tasks
func (nm *NodeManager) InitializeTask(task *Task, jobSpec *JobSpec, sourceDir string, inputRate int) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	LogInfo(true, "NodeManager: Initializing task %s on node %s", task.TaskID, nm.NodeID)

	// Check if task's address matches this node's NodeID
	if task.Address != nm.NodeID {
		return fmt.Errorf("task address %s does not match this node's NodeID %s", task.Address, nm.NodeID)
	}

	// Update currentJobSpec if provided
	if jobSpec != nil {
		nm.currentJobSpec = jobSpec
		nm.inputRate = inputRate
	}

	// Check if task already exists
	if _, exists := nm.TaskInfos[task.TaskID]; exists {
		return fmt.Errorf("task %s already exists on this node", task.TaskID)
	}

	// All tasks are processing stages (stages 1 to NStages)
	// Get StageOp for the task's stage
	// StageOps[0] maps to stage 1, StageOps[1] maps to stage 2, etc.
	var stageOp *StageOp
	if nm.currentJobSpec != nil {
		stageOpIndex := task.Stage - 1 // Map stage 1 -> StageOps[0], stage 2 -> StageOps[1], etc.
		if stageOpIndex >= 0 && stageOpIndex < len(nm.currentJobSpec.StageOps) {
			stageOp = &nm.currentJobSpec.StageOps[stageOpIndex]
		}
	}

	if stageOp == nil {
		return fmt.Errorf("failed to get StageOp for task %s (stage %d) - JobSpec may not be set or stage out of bounds", task.TaskID, task.Stage)
	}

	// Start command process (executable from StageOp)
	process, pid, stdinPipe, stdoutPipe, err := nm.startCommandProcess(task, stageOp, sourceDir, inputRate)
	if err != nil {
		return fmt.Errorf("failed to start command process for task %s: %w", task.TaskID, err)
	}

	// Determine if this is final stage
	isFinalStage := nm.currentJobSpec != nil && task.Stage == nm.currentJobSpec.NStages

	// Initialize ExactlyOnceManager only if exactly-once or autoscale is enabled.
	var exactlyOnceMgr *ExactlyOnceManager = nil
	if task.ExactlyOnce || task.AutoscaleMode {
		// Pass NodeManager and task to ExactlyOnceManager so it can send ACKs
		// upstream from its background flush loop once WAL is durable.
		exactlyOnceMgr = NewExactlyOnceManager(false, isFinalStage, nm, task)
	}
	taskInfo := &TaskInfo{
		TaskID:         task.TaskID,
		Task:           task,
		PID:            pid,
		Process:        process,
		Stdin:          stdinPipe,
		Stdout:         stdoutPipe,
		ExactlyOnceMgr: exactlyOnceMgr,
	}

	// Start goroutine to read stdout and forward tuples (only for processing stages)
	// Start reading output from the command process
	go nm.readTaskOutput(task, taskInfo)

	// Register the task
	nm.TaskInfos[task.TaskID] = taskInfo
	nm.TaskStatus[task.TaskID] = TaskRunning

	LogInfo(true, "NodeManager: Successfully initialized task %s (stage: %d, PID: %d, Address: %s, exe: %s, args: %v)", task.TaskID, task.Stage, pid, task.Address, stageOp.Exe, stageOp.Args)

	return nil
}

// UpdateTopology updates the topology on this node
// This updates existing tasks' InputTasks and OutputTargets, and adds new tasks if needed
func (nm *NodeManager) UpdateTopology(topology *Topology) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	LogInfo(true, "NodeManager: Updating topology for job %s on node %s", topology.JobID, nm.NodeID)

	// Update tasks that belong to this node
	for taskID, updatedTask := range topology.Tasks {
		// Only update tasks that belong to this node
		if updatedTask.Address != nm.NodeID {
			continue
		}

		// Check if task already exists
		if taskInfo, exists := nm.TaskInfos[taskID]; exists {
			// Task exists - update its InputTasks and OutputTargets
			taskInfo.Task.Mutex.Lock()
			taskInfo.Task.InputTasks = make(map[string]string)
			taskInfo.Task.OutputTargets = make(map[string]string)

			// Copy updated InputTasks
			for inputTaskID, inputAddr := range updatedTask.InputTasks {
				taskInfo.Task.InputTasks[inputTaskID] = inputAddr
			}

			// Copy updated OutputTargets
			for outputTaskID, outputAddr := range updatedTask.OutputTargets {
				taskInfo.Task.OutputTargets[outputTaskID] = outputAddr
			}

			taskInfo.Task.Mutex.Unlock()

			LogInfo(true, "NodeManager: Updated task %s InputTasks=%d, OutputTargets=%d", taskID, len(taskInfo.Task.InputTasks), len(taskInfo.Task.OutputTargets))
		} else {
			// Task doesn't exist - this is a new task that should be initialized
			// But we don't initialize it here - that should be done via AddTask RPC
			LogInfo(true, "NodeManager: Task %s in topology but not initialized on this node (use AddTask to initialize)", taskID)
		}
	}

	LogInfo(true, "NodeManager: Topology update completed for job %s", topology.JobID)
	return nil
}

// KillTaskProcess kills the process associated with a task by taskID and
// removes its in-memory state (TaskInfos, Metrics, TaskStatus, WALFiles).
func (nm *NodeManager) KillTaskProcess(taskID string) error {
	nm.mu.Lock()
	taskInfo, exists := nm.TaskInfos[taskID]

	if !exists {
		nm.mu.Unlock()
		return fmt.Errorf("task %s not found", taskID)
	}

	if taskInfo.Process == nil {
		nm.mu.Unlock()
		return fmt.Errorf("task %s has no associated process", taskID)
	}

	LogInfo(true, "NodeManager: Killing process for task %s (PID: %d)", taskID, taskInfo.PID)

	// Stop exactly-once background goroutines (retry + ACK flush) for this task,
	// if enabled, so that they don't continue running after the task is killed.
	if taskInfo.ExactlyOnceMgr != nil {
		taskInfo.ExactlyOnceMgr.Stop()
	}

	// Kill the process
	err := taskInfo.Process.Kill()
	if err != nil {
		nm.mu.Unlock()
		return fmt.Errorf("failed to kill process for task %s (PID: %d): %w", taskID, taskInfo.PID, err)
	}

	// Clean up in-memory state for this task.
	delete(nm.TaskInfos, taskID)
	delete(nm.Metrics, taskID)
	nm.TaskStatus[taskID] = TaskStopped
	if wal, ok := nm.WALFiles[taskID]; ok && wal != nil {
		if cerr := wal.Close(); cerr != nil {
			LogError(true, "NodeManager: Failed to close WAL file for task %s: %v", taskID, cerr)
		}
		delete(nm.WALFiles, taskID)
	}
	nm.mu.Unlock()

	LogInfo(true, "NodeManager: Successfully killed process for task %s (PID: %d)", taskID, taskInfo.PID)
	return nil
}

// startMonitor starts a goroutine that periodically collects metrics and task status
// and sends them to the leader
func (nm *NodeManager) startMonitor() {
	// Monitor interval: collect and send metrics every metricsCollectionInterval
	ticker := time.NewTicker(metricsCollectionInterval)
	defer ticker.Stop()

	for range ticker.C {
		nm.collectAndSendMetrics()
	}
}

// startOutputFlushLoop periodically flushes any buffered final-stage output
// tuples from outputBuffer to HyDFS using AppendOrCreateFromBuffer. This allows
// tasks to write into an in-memory buffer while a single background goroutine
// performs batched, replicated appends, similar to how WAL data is handled.
func (nm *NodeManager) startOutputFlushLoop() {
	ticker := time.NewTicker(outputFlushInterval)
	defer ticker.Stop()

	for range ticker.C {
		// If server or job spec is not initialized yet, skip this cycle.
		if nm.server == nil {
			continue
		}

		nm.mu.RLock()
		job := nm.currentJobSpec
		nm.mu.RUnlock()
		if job == nil || job.DestFile == "" {
			continue
		}

		// Snapshot buffer under lock.
		nm.outputBufferMu.Lock()
		if len(nm.outputBuffer) == 0 {
			nm.outputBufferMu.Unlock()
			continue
		}
		bufCopy := append([]byte(nil), nm.outputBuffer...)
		nm.outputBuffer = nm.outputBuffer[:0]
		nm.outputBufferMu.Unlock()

		// Compute current HyDFS output filename based on the active job spec.
		destName := job.DestFile
		hydfsOutputFile := fmt.Sprintf("out-%s", filepath.Base(destName))

		// Perform the actual HyDFS append outside the lock.
		nm.server.AppendOrCreateFromBuffer(hydfsOutputFile, bufCopy)
	}
}

// isProcessAlive checks if a process is still alive by sending signal 0
// Signal 0 is a no-op signal used to check if process exists without affecting it
func (nm *NodeManager) isProcessAlive(process *os.Process) bool {
	if process == nil {
		return false
	}

	// On Windows, os.Process.Signal is not supported; assume alive here and rely on other signals/logs
	if runtime.GOOS == "windows" {
		return true
	}

	// Send signal 0 to check if process exists
	// If process is dead, this will return an error
	err := process.Signal(syscall.Signal(0))
	if err != nil {
		// Process is likely dead (os.ErrProcessDone or similar)
		return false
	}
	return true
}

// collectAndSendMetrics collects current metrics and task status, then sends to leader
func (nm *NodeManager) collectAndSendMetrics() {
	if nm.leaderAddress == "" {
		// Leader address not set yet, skip this cycle
		return
	}

	nm.mu.Lock()
	now := time.Now()

	// Check process status for all tasks and update TaskStatus accordingly
	for taskID, taskInfo := range nm.TaskInfos {
		alive := nm.isProcessAlive(taskInfo.Process)
		if !alive && nm.TaskStatus[taskID] == TaskRunning {
			// Process died but status was still running - update to failed
			nm.TaskStatus[taskID] = TaskFailed
			LogInfo(true, "NodeManager: Task %s process (PID: %d) is dead, updating status to Failed", taskID, taskInfo.PID)
		}
	}

	// Update throughput for each task based on TuplesReceived delta over time
	for taskID, metrics := range nm.Metrics {
		history := nm.metricsHistory[taskID]
		intervalSeconds := metricsCollectionInterval.Seconds()
		var delta int64

		if history.LastReported.IsZero() {
			// First report: treat the cumulative received count as occurring over the default interval
			delta = metrics.TuplesReceived
		} else {
			if elapsed := now.Sub(history.LastReported).Seconds(); elapsed > 0 {
				intervalSeconds = elapsed
			}
			delta = metrics.TuplesReceived - history.LastReceived
			if delta < 0 {
				// Guard against counter reset; treat as absolute value since last snapshot
				delta = metrics.TuplesReceived
			}
		}

		if intervalSeconds > 0 {
			metrics.Throughput = float64(delta) / intervalSeconds
		} else {
			metrics.Throughput = 0
		}

		nm.metricsHistory[taskID] = metricsHistory{
			LastProcessed: metrics.TuplesProcessed,
			LastReceived:  metrics.TuplesReceived,
			LastReported:  now,
		}

	}

	// Collect task metrics
	taskMetrics := make([]*TaskMetrics, 0, len(nm.Metrics))
	for _, metrics := range nm.Metrics {
		// Create a copy to avoid race conditions
		metricsCopy := *metrics
		taskMetrics = append(taskMetrics, &metricsCopy)
	}

	// Collect task status
	taskStatus := make([]*TaskStatusReport, 0, len(nm.TaskStatus))
	for taskID, status := range nm.TaskStatus {
		taskStatus = append(taskStatus, &TaskStatusReport{
			TaskID: taskID,
			Status: status,
		})
	}
	nm.mu.Unlock()

	// Create report
	report := &NodeMetricsReport{
		NodeID:      nm.NodeID,
		TaskMetrics: taskMetrics,
		TaskStatus:  taskStatus,
	}

	// Send to leader via RPC
	if nm.server != nil {
		_, err := CallReportMetrics(nm.leaderAddress, report, nm.server)
		if err != nil {
			LogError(true, "NodeManager: Failed to send metrics to leader %s: %v", nm.leaderAddress, err)
		} else {
			LogInfo(false, "NodeManager: Sent metrics to leader (tasks: %d, metrics: %d)", len(taskStatus), len(taskMetrics))
		}
	}
}

// startCommandProcess creates a separate process to run the task's executable command
// The command reads tuples from stdin and writes output tuples to stdout
// Returns process, PID, stdin pipe writer, and stdout pipe reader
func (nm *NodeManager) startCommandProcess(task *Task, stageOp *StageOp, sourceDir string, inputRate int) (*os.Process, int, io.WriteCloser, io.ReadCloser, error) {
	// Convert executable path to absolute path
	// This is necessary because cmd.Dir changes the working directory to the task directory
	absExePath := stageOp.Exe
	if !filepath.IsAbs(stageOp.Exe) {
		// Resolve relative path to absolute path (relative to current working directory)
		absExe, err := filepath.Abs(stageOp.Exe)
		if err != nil {
			return nil, 0, nil, nil, fmt.Errorf("failed to resolve executable path %s: %w", stageOp.Exe, err)
		}
		absExePath = absExe
	}

	// Verify the executable exists
	if _, err := os.Stat(absExePath); os.IsNotExist(err) {
		return nil, 0, nil, nil, fmt.Errorf("executable not found: %s", absExePath)
	}

	// Build command to run the executable
	// The executable will read tuples from stdin and write to stdout
	cmd := exec.Command(absExePath, stageOp.Args...)

	// Create stdin pipe so we can write tuples to the process
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, 0, nil, nil, fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Create stdout pipe so we can read tuples from the process
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		stdinPipe.Close()
		return nil, 0, nil, nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	// Set up process attributes
	cmd.Stderr = os.Stderr // TODO: Redirect to task log file
	// cmd.Stdin and cmd.Stdout are set via pipes above

	// Start the process
	err = cmd.Start()
	if err != nil {
		stdinPipe.Close()
		stdoutPipe.Close()
		return nil, 0, nil, nil, fmt.Errorf("failed to start command process: %w", err)
	}

	process := cmd.Process
	pid := process.Pid

	LogInfo(true, "NodeManager: Started command process for task %s with PID=%d (exe: %s)", task.TaskID, pid, stageOp.Exe)

	return process, pid, stdinPipe, stdoutPipe, nil
}

// GetTaskInfo returns task info by ID
func (nm *NodeManager) GetTaskInfo(taskID string) (*TaskInfo, bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	taskInfo, exists := nm.TaskInfos[taskID]
	return taskInfo, exists
}

// ListTasks returns basic process information for all tasks on this node.
func (nm *NodeManager) ListTasks() []*TaskProcessInfo {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var vm string
	if nm.server != nil && nm.server.LocalDirectory != "" {
		// LocalDirectory is typically "./localdirectory/vmX"
		vm = filepath.Base(nm.server.LocalDirectory)
	}

	results := make([]*TaskProcessInfo, 0, len(nm.TaskInfos))
	for _, ti := range nm.TaskInfos {
		if ti == nil || ti.Task == nil {
			continue
		}
		stage := ti.Task.Stage
		opExe := ""
		if nm.currentJobSpec != nil {
			idx := stage - 1
			if idx >= 0 && idx < len(nm.currentJobSpec.StageOps) {
				opExe = nm.currentJobSpec.StageOps[idx].Exe
			}
		}

		results = append(results, &TaskProcessInfo{
			TaskID: ti.TaskID,
			PID:    ti.PID,
			Stage:  stage,
			OpExe:  opExe,
			VM:     vm,
		})
	}

	return results
}

// readTaskOutput reads stdout from the task process, parses tuples, and forwards them to downstream tasks
// Only called for processing stages (stage > 0), not for source tasks (stage 0)
func (nm *NodeManager) readTaskOutput(task *Task, taskInfo *TaskInfo) {
	if taskInfo.Stdout == nil {
		LogError(true, "NodeManager: Stdout pipe not available for task %s", task.TaskID)
		return
	}

	scanner := bufio.NewScanner(taskInfo.Stdout)
	lineNum := 0

	LogInfo(true, "NodeManager: Started reading output from task %s (PID=%d)", task.TaskID, taskInfo.PID)

	for scanner.Scan() {
		line := scanner.Text()
		lineNum++

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse structured output format: {OriginalTupleId, OutputTuples}
		var outputData map[string]interface{}
		if err := json.Unmarshal([]byte(line), &outputData); err != nil {
			LogError(true, "NodeManager: Task %s: Failed to parse output from line %d: %v (line: %s)", task.TaskID, lineNum, err, line)
			continue
		}

		// Extract OriginalTupleId
		originalID, ok := outputData["OriginalTupleId"].(string)
		if !ok || originalID == "" {
			LogError(true, "NodeManager: Task %s: Missing or invalid OriginalTupleId on line %d", task.TaskID, lineNum)
			continue
		}

		// Extract OutputTuples array
		outputTuplesRaw, ok := outputData["OutputTuples"]
		if !ok {
			LogError(true, "NodeManager: Task %s: Missing OutputTuples on line %d", task.TaskID, lineNum)
			continue
		}

		// Convert to []Tuple
		outputTuplesJSON, err := json.Marshal(outputTuplesRaw)
		if err != nil {
			LogError(true, "NodeManager: Task %s: Failed to marshal OutputTuples on line %d: %v", task.TaskID, lineNum, err)
			continue
		}

		var outputTuples []Tuple
		if err := json.Unmarshal(outputTuplesJSON, &outputTuples); err != nil {
			LogError(true, "NodeManager: Task %s: Failed to unmarshal OutputTuples on line %d: %v", task.TaskID, lineNum, err)
			continue
		}

		// Set expected output count based on array length
		outputCount := len(outputTuples)
		if taskInfo.ExactlyOnceMgr != nil {
			taskInfo.ExactlyOnceMgr.SetExpectedOutputCount(originalID, outputCount)
		}

		// Process each tuple in the array
		for i := range outputTuples {
			tuple := &outputTuples[i]
			// Set sender ID to this task
			tuple.SenderID = task.TaskID

			// Forward tuple to downstream tasks (this will add to sent_tuples if exactly-once is enabled)
			nm.forwardTupleToDownstream(task, taskInfo, tuple, originalID)

			// Increment output count for the original tuple
			if taskInfo.ExactlyOnceMgr != nil {
				taskInfo.ExactlyOnceMgr.IncrementOutputCount(originalID)
			}
		}

		// Increment processed tuple count (one per original input tuple)
		nm.incrementTaskMetrics(task.TaskID, true, false, false)

		LogInfo(true, "NodeManager: Task %s: Processed %d output tuples for original_id=%s", task.TaskID, outputCount, originalID)
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		LogError(true, "NodeManager: Task %s: Error reading stdout: %v", task.TaskID, err)
	} else {
		LogInfo(true, "NodeManager: Task %s: Stdout pipe closed (process may have exited)", task.TaskID)
	}
}

// forwardTupleToDownstream forwards a tuple to a downstream task based on key hashing
// If this is the final stage, it writes the tuple to the output file instead
// originalID is the ID of the received tuple that generated this output (for exactly-once tracking)
func (nm *NodeManager) forwardTupleToDownstream(task *Task, taskInfo *TaskInfo, tuple *Tuple, originalID string) {
	// Check if this is the final stage (last processing stage)
	// Stages are: 1, 2, ..., NStages (processing stages)
	// Final stage = NStages
	if nm.currentJobSpec != nil && task.Stage == nm.currentJobSpec.NStages {
		// This is the final stage - write to output file
		nm.writeTupleToOutputFile(tuple)
		return
	}

	if len(task.OutputTargets) == 0 {
		// No downstream tasks - this might be a sink task
		LogInfo(true, "NodeManager: Task %s: No downstream tasks for tuple ID=%s", task.TaskID, tuple.ID)
		return
	}

	// Select downstream task based on key hashing
	selectedTaskID, selectedAddr := nm.selectDownstreamTask(task, tuple.Key)
	if selectedTaskID == "" || selectedAddr == "" {
		LogError(true, "NodeManager: Task %s: Failed to select downstream task for tuple ID=%s (key=%s)",
			task.TaskID, tuple.ID, tuple.Key)
		return
	}

	// Create the tuple for the selected downstream task
	forwardTuple := Tuple{
		ID:        tuple.ID,
		Key:       tuple.Key,
		Value:     tuple.Value,
		Timestamp: tuple.Timestamp,
		SenderID:  tuple.SenderID,
		TaskID:    selectedTaskID, // Set target task ID for routing
	}

	// Send tuple to the selected downstream task via Rainstorm TCP RPC
	// Exactly-once semantics: tracking happens inside SendTuple if manager is provided
	// Note: We don't mark as processed here - that happens when all outputs are sent
	// (handled by IncrementOutputCount in readTaskOutput)
	var exactlyOnceMgr *ExactlyOnceManager
	if taskInfo.ExactlyOnceMgr != nil {
		exactlyOnceMgr = taskInfo.ExactlyOnceMgr
	}
	_, err := SendTuple(selectedAddr, &forwardTuple, exactlyOnceMgr, originalID)
	if err != nil {
		LogError(true, "NodeManager: Task %s: Failed to forward tuple ID=%s (key=%s) to downstream task %s at %s: %v",
			task.TaskID, tuple.ID, tuple.Key, selectedTaskID, selectedAddr, err)
		return
	}

	// Increment sent tuple count for this task
	nm.incrementTaskMetrics(task.TaskID, false, true, false)

	LogInfo(true, "NodeManager: Task %s: Forwarded tuple ID=%s (key=%s) to downstream task %s at %s",
		task.TaskID, tuple.ID, tuple.Key, selectedTaskID, selectedAddr)
}

// MergeWalForTask merges the WAL of a killed task into the WAL of a surviving
// task on this node. It does this purely via existing HyDFS operations:
//  1. GET the old task's WAL file from HyDFS to this node
//  2. Use the existing HyDFS append path (handleAppend) to append that local
//     file as a single append into the surviving task's WAL file in HyDFS.
func (nm *NodeManager) MergeWalForTask(oldTaskID, keepTaskID string) error {
	if nm.server == nil {
		return fmt.Errorf("server is nil")
	}

	if oldTaskID == "" || keepTaskID == "" {
		return fmt.Errorf("oldTaskID or keepTaskID is empty")
	}

	oldWalHyDFS := "wal-" + oldTaskID + ".log"
	keepWalHyDFS := "wal-" + keepTaskID + ".log"

	LogInfo(true, "NodeManager: Merging WAL %s into %s", oldWalHyDFS, keepWalHyDFS)

	// Step 1: GET old WAL from HyDFS to this node using existing HyDFS get path.
	// This will download wal-oldTaskID.log into the node's LocalDirectory.
	nm.server.handleGet(oldWalHyDFS, oldWalHyDFS)

	// Step 2: Use existing HyDFS append API to append srcPath into keepWalHyDFS.
	localDir := nm.server.LocalDirectory
	if localDir == "" {
		localDir = "."
	}

	srcPath := filepath.Join(localDir, oldWalHyDFS)

	// If the source file doesn't exist (e.g., WAL was never created), just log and return.
	if _, err := os.Stat(srcPath); err != nil {
		if os.IsNotExist(err) {
			LogInfo(true, "NodeManager: No WAL file %s found to merge (nothing to do)", srcPath)
			return nil
		}
		return fmt.Errorf("failed to stat WAL file %s: %w", srcPath, err)
	}

	// Merge the old WAL's REC/SENT/ACK records into the surviving task's
	// ExactlyOnceManager in-memory state, if available.
	nm.mu.RLock()
	keepTaskInfo, exists := nm.TaskInfos[keepTaskID]
	nm.mu.RUnlock()
	if exists && keepTaskInfo != nil && keepTaskInfo.ExactlyOnceMgr != nil {
		LogInfo(true, "NodeManager: Merging in-memory exactly-once state for keep task %s from WAL %s", keepTaskID, srcPath)
		keepTaskInfo.ExactlyOnceMgr.mergeStateFromLocalWal(srcPath)
	} else {
		LogInfo(true, "NodeManager: No ExactlyOnceMgr for keep task %s; skipping in-memory state merge", keepTaskID)
	}

	LogInfo(true, "NodeManager: Appending merged WAL file %s into HyDFS file %s", srcPath, keepWalHyDFS)
	nm.server.handleAppend(srcPath, keepWalHyDFS)

	return nil
}

// selectDownstreamTask selects a downstream task based on key hashing
// Returns the selected task ID and address, or empty strings if no downstream tasks exist
func (nm *NodeManager) selectDownstreamTask(task *Task, key string) (string, string) {
	if len(task.OutputTargets) == 0 {
		return "", ""
	}

	// Convert OutputTargets map to a sorted list for deterministic routing
	downstreamTaskIDs := make([]string, 0, len(task.OutputTargets))
	for taskID := range task.OutputTargets {
		downstreamTaskIDs = append(downstreamTaskIDs, taskID)
	}
	sort.Strings(downstreamTaskIDs) // Sort for deterministic ordering

	// Hash the key and select downstream task using modulo
	hash := hashKey(key)
	selectedIndex := int(hash) % len(downstreamTaskIDs)
	selectedTaskID := downstreamTaskIDs[selectedIndex]
	selectedAddr := task.OutputTargets[selectedTaskID]

	return selectedTaskID, selectedAddr
}

// selectTaskByKey selects a task from a list of tasks based on key hashing.
// When autoscale is enabled and a leader is present, this function will use
// the leader's current topology and task statuses to determine the current
// set of running stage-1 tasks, so that routing reflects dynamic autoscaling.
// Otherwise, it falls back to the provided tasks slice.
func selectTaskByKey(key string, tasks []*Task) *Task {
	candidates := tasks

	// If we have a leader with autoscale enabled, prefer the current set of
	// running stage-1 tasks from the leader's topology.
	if globalServer != nil && globalServer.Leader != nil {
		l := globalServer.Leader
		l.mu.RLock()
		job := l.JobSpec
		topology := l.Topology
		taskStatuses := l.TaskStatuses
		l.mu.RUnlock()

		if job != nil && job.Autoscale && topology != nil {
			dynamic := make([]*Task, 0)
			for _, t := range topology.Tasks {
				if t.Stage != 1 {
					continue
				}
				// Prefer tasks that are reported as running when status is known.
				if status, ok := taskStatuses[t.TaskID]; ok && status != TaskRunning {
					continue
				}
				dynamic = append(dynamic, t)
			}
			if len(dynamic) > 0 {
				candidates = dynamic
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	// Sort tasks by TaskID for deterministic routing
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].TaskID < candidates[j].TaskID
	})

	// Hash the key and select task using modulo
	hash := hashKey(key)
	selectedIndex := int(hash) % len(candidates)
	return candidates[selectedIndex]
}

// hashKey hashes a key string to a uint32 value for routing
func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// writeTupleToOutputFile writes a tuple to the output file
// Multiple tasks in the final stage may write to the same logical HyDFS file.
// Instead of writing directly, we append JSON lines to an in-memory buffer
// which is flushed periodically by startOutputFlushLoop.
func (nm *NodeManager) writeTupleToOutputFile(tuple *Tuple) {
	if nm.currentJobSpec == nil || nm.currentJobSpec.DestFile == "" {
		LogError(true, "NodeManager: Cannot write tuple - DestFile not set in JobSpec")
		return
	}

	// Marshal only the tuple ID, Key, and Value as a JSON line
	out := map[string]string{
		"Key":   tuple.Key,
		"Value": tuple.Value,
	}
	tupleJSON, err := json.Marshal(out)
	if err != nil {
		LogError(true, "NodeManager: Failed to marshal tuple for output file: %v", err)
		return
	}

	// Append JSON line to the shared output buffer. The background flush loop
	// will periodically write this buffer to HyDFS using AppendOrCreateFromBuffer.
	line := append(tupleJSON, '\n')

	nm.outputBufferMu.Lock()
	nm.outputBuffer = append(nm.outputBuffer, line...)
	nm.outputBufferMu.Unlock()

	LogInfo(false, "NodeManager: Buffered tuple ID=%s for HyDFS output (DestFile=%s)", tuple.ID, nm.currentJobSpec.DestFile)
}

// GetAllTaskInfos returns all task infos on this node
func (nm *NodeManager) GetAllTaskInfos() map[string]*TaskInfo {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	result := make(map[string]*TaskInfo)
	for id, taskInfo := range nm.TaskInfos {
		result[id] = taskInfo
	}
	return result
}

// StartStreaming starts all tasks. Leader will stream data directly to stage 1 tasks.
func (nm *NodeManager) StartStreaming() error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	LogInfo(true, "NodeManager: Starting streaming for %d tasks", len(nm.TaskInfos))

	for taskID, taskInfo := range nm.TaskInfos {
		status := nm.TaskStatus[taskID]
		if status != TaskRunning {
			return fmt.Errorf("task %s is not ready (status: %d)", taskID, status)
		}

		task := taskInfo.Task
		if task == nil {
			return fmt.Errorf("task %s has nil Task reference", taskID)
		}

		// Task process is already running (started in initializeTask)
		// Leader will stream data directly to stage 1 tasks
		LogInfo(true, "NodeManager: Task %s (PID: %d, Stage: %d) is now running", taskID, taskInfo.PID, task.Stage)
	}

	return nil
}
