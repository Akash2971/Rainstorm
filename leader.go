package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// StageOp represents an operation for a stage
type StageOp struct {
	Exe  string   // Path to executable
	Args []string // Arguments for the executable
}

// JobSpec represents a job specification with stages and operations
type JobSpec struct {
	NStages       int       // Number of stages
	TasksPerStage int       // Number of tasks per stage
	StageOps      []StageOp // One operation per stage
	SourceDir     string    // HyDFS source directory
	DestFile      string    // HyDFS output file
	SourceAddress string    // Leader address (source of data streaming)
	ExactlyOnce   bool      // Enable exactly-once semantics
	Autoscale     bool      // Enable autoscaling
	InputRate     int       // Tuples per second
	LW            int       // Lower watermark / batch size
	HW            int       // Higher watermark / batch size
}

// Topology represents the complete streaming topology
type Topology struct {
	JobID  string           // Unique job identifier
	Tasks  map[string]*Task // Map of task ID to Task
	Stages []int            // List of stage numbers in order
	mu     sync.RWMutex     // Mutex for thread-safe access
}

// NewTopology creates a new topology instance
func NewTopology(jobID string) *Topology {
	return &Topology{
		JobID:  jobID,
		Tasks:  make(map[string]*Task),
		Stages: make([]int, 0),
	}
}

// AddTask adds a task to the topology
func (t *Topology) AddTask(task *Task) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Tasks[task.TaskID] = task
}

// GetTask returns a task by ID
func (t *Topology) GetTask(taskID string) (*Task, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	task, exists := t.Tasks[taskID]
	return task, exists
}

// Leader manages job submission, task placement, and coordination
type Leader struct {
	Topology        *Topology               // Current topology (single job at a time)
	Nodes           map[string]*NodeManager // Map of node address to NodeManager
	JobSpec         *JobSpec                // Current job specification
	AutoscaleTicker *time.Ticker            // Ticker for autoscaling checks
	Metrics         map[string]*TaskMetrics // Map of task ID to TaskMetrics
	Assignments     map[string]string       // Map of task ID to node address
	server          *Server                 // Reference to the server
	TaskStatuses    map[string]TaskStatus   // Map of task ID to last reported TaskStatus
	ExactlyOnceMgr  *ExactlyOnceManager     // Exactly-once semantics manager for leader (source)
	mu              sync.RWMutex            // Mutex for thread-safe access
	nextScaleNode   int                     // Round-robin index for placing newly scaled tasks
}

// NewLeader creates a new leader instance
func NewLeader(server *Server) *Leader {
	leader := &Leader{
		Topology:     nil,
		Nodes:        make(map[string]*NodeManager),
		JobSpec:      nil,
		Metrics:      make(map[string]*TaskMetrics),
		Assignments:  make(map[string]string),
		TaskStatuses: make(map[string]TaskStatus),
		server:       server,
	}

	// Start autoscale ticker: periodically evaluate scaling decisions based on
	// the latest metrics rather than on every metrics report.
	leader.AutoscaleTicker = time.NewTicker(1 * time.Second) // TODO: move this to config
	go func() {
		for range leader.AutoscaleTicker.C {
			if err := leader.Autoscale(); err != nil {
				LogError(true, "Leader: Autoscale check failed: %v", err)
			}
		}
	}()

	// Start background task health monitor to restart failed tasks when
	// exactly-once execution is enabled.
	go leader.startTaskHealthMonitor()

	return leader
}

// SubmitJob handles job submission
func (l *Leader) SubmitJob(job *JobSpec) error {
	// If there is an existing job running, proactively kill all of its tasks
	// before starting a new one to ensure a clean slate.
	l.killExistingJobTasks()

	l.mu.Lock()
	l.JobSpec = job
	l.mu.Unlock()

	LogInfo(true, "Leader: Submitting job with %d stages, %d tasks per stage", job.NStages, job.TasksPerStage)

	// Step 1: Create topology from job spec
	topology, err := l.CreateTopology(job)
	if err != nil {
		return fmt.Errorf("failed to create topology: %w", err)
	}

	l.mu.Lock()
	l.Topology = topology
	l.mu.Unlock()

	LogInfo(true, "Leader: Created topology with %d tasks", len(topology.Tasks))

	// Step 2: Compute task assignments (assign tasks to nodes)
	assignments, err := l.computeTaskAssignments(topology)
	if err != nil {
		return fmt.Errorf("failed to compute task assignments: %w", err)
	}

	l.mu.Lock()
	l.Assignments = assignments
	l.mu.Unlock()

	LogInfo(true, "Leader: Computed task assignments for %d tasks", len(assignments))

	// Step 3: Initialize all tasks and update topology
	err = l.InitializeTasks()
	if err != nil {
		return fmt.Errorf("failed to send topology to nodes: %w", err)
	}

	LogInfo(true, "Leader: All nodes initialized tasks successfully")

	// Step 4: Start streaming after all nodes are ready
	err = l.StartStreaming()
	if err != nil {
		return fmt.Errorf("failed to start streaming: %w", err)
	}

	LogInfo(true, "Leader: Job submission completed successfully")
	return nil
}

// killExistingJobTasks sends KillTask RPCs for all tasks in the current
// topology/assignment map, if any. Failures are logged but do not prevent
// subsequent job submission.
func (l *Leader) killExistingJobTasks() {
	l.mu.RLock()
	topology := l.Topology
	assignments := l.Assignments
	l.mu.RUnlock()

	if topology == nil || len(assignments) == 0 {
		return
	}

	ConsolePrintf("Cleaning up %d task(s) from previous RainStorm job %s before starting a new job\n",
		len(assignments), topology.JobID)

	for taskID, nodeAddr := range assignments {
		resp, err := CallKillTask(nodeAddr, taskID, l.server)
		if err != nil {
			LogError(true, "Leader: Failed to kill existing task %s on node %s: %v", taskID, nodeAddr, err)
			continue
		}
		if !resp.Success {
			LogError(true, "Leader: KillTask for existing task %s on node %s failed: %s", taskID, nodeAddr, resp.Message)
			continue
		}
		LogInfo(true, "Leader: Killed existing task %s on node %s before starting new job", taskID, nodeAddr)
	}
	LogInfo(true, "Leader: Killed %d existing tasks before starting new job", len(assignments))
}

// CreateTopology creates a topology from job specification using server members
func (l *Leader) CreateTopology(job *JobSpec) (*Topology, error) {
	if l.server == nil || l.server.Members == nil {
		return nil, fmt.Errorf("server or members not available")
	}

	// Generate a unique job ID
	jobID := fmt.Sprintf("job-%d", time.Now().Unix())
	topology := NewTopology(jobID)

	// Get all alive members (excluding self)
	aliveMembers := l.server.Members.GetAliveMembers(l.server.Addr)
	if len(aliveMembers) == 0 {
		return nil, fmt.Errorf("no alive members available")
	}

	LogInfo(true, "Leader: Creating topology with %d alive members", len(aliveMembers))

	// Get timestamp for unique task IDs
	timestamp := time.Now().Unix()

	// Create tasks for processing stages (stages 1 to NStages)
	// Stages 1 to NStages are processing stages (use StageOps)
	totalStages := job.NStages // Only processing stages (no stage 0)

	for stage := 1; stage <= totalStages; stage++ {
		topology.Stages = append(topology.Stages, stage)

		// Create TasksPerStage tasks for this stage
		for taskIdx := 0; taskIdx < job.TasksPerStage; taskIdx++ {
			taskID := fmt.Sprintf("task-%d-%d-%d", stage, taskIdx, timestamp)

			// Create task
			task := NewTask(taskID, stage)

			// Set task properties from job spec
			task.ExactlyOnce = job.ExactlyOnce
			task.AutoscaleMode = job.Autoscale

			// Set up input/output connections
			// Stage 1 tasks receive data from leader (SourceAddress)
			// Stages 2+ connect to previous stage tasks
			if stage == 1 {
				if job.SourceAddress != "" {
					task.AddInputTask("leader", job.SourceAddress)
					LogInfo(true, "Leader: Set stage 1 task %s InputTask from leader at %s", taskID, job.SourceAddress)
				}
			} else if stage > 1 {
				// Will be filled in with actual node:port after assignments are computed
				for prevTaskIdx := 0; prevTaskIdx < job.TasksPerStage; prevTaskIdx++ {
					prevTaskID := fmt.Sprintf("task-%d-%d-%d", stage-1, prevTaskIdx, timestamp)
					task.AddInputTask(prevTaskID, "") // Empty for now, filled in Pass 2
				}
			}

			// Set up OutputTargets for downstream tasks
			if stage < totalStages {
				// Connect to all tasks in next stage
				for nextTaskIdx := 0; nextTaskIdx < job.TasksPerStage; nextTaskIdx++ {
					nextTaskID := fmt.Sprintf("task-%d-%d-%d", stage+1, nextTaskIdx, timestamp)
					// Will be filled in with actual node:port after assignments are computed
					task.AddOutputTarget(nextTaskID, "")
				}
			}

			// Add task to topology
			topology.AddTask(task)
		}
	}

	LogInfo(true, "Leader: Created topology with %d tasks across %d processing stages (stages 1-%d). Leader will stream data directly to stage 1 tasks.", len(topology.Tasks), totalStages, job.NStages)
	return topology, nil
}

// HandleTaskFailure handles task failure and triggers recovery
func (l *Leader) HandleTaskFailure(taskID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	LogInfo(true, "Leader: Handling failure of task %s", taskID)

	// TODO: Mark task as failed
	// TODO: Reassign
	// TODO: Update topology
	// TODO: Notify NodeManagers

	return nil
}

// InitializeTasks sends the full topology and assignments to all nodes to initialize tasks
// Returns error if any node fails to initialize tasks
// Assumes assignments have already been computed and stored in l.Assignments
func (l *Leader) InitializeTasks() error {
	l.mu.RLock()
	topology := l.Topology
	assignments := l.Assignments
	l.mu.RUnlock()

	if topology == nil {
		return fmt.Errorf("no topology to send")
	}

	if len(assignments) == 0 {
		return fmt.Errorf("no task assignments available")
	}

	LogInfo(true, "Leader: Sending topology to nodes (readiness acknowledged via InitializeTasks response)")

	// Step 1: Get all unique nodes that have tasks assigned
	nodeSet := make(map[string]bool)
	for _, nodeAddr := range assignments {
		nodeSet[nodeAddr] = true
	}

	LogInfo(true, "Leader: Sending topology to %d nodes", len(nodeSet))

	// Step 2: Send topology to each node and collect responses
	var errors []error
	// Get SourceDir and InputRate from JobSpec
	sourceDir := ""
	inputRate := 0
	if l.JobSpec != nil {
		sourceDir = l.JobSpec.SourceDir
		inputRate = l.JobSpec.InputRate
	}

	// TODO: OPTIMIZATION this can be done in parallel
	for nodeAddr := range nodeSet {
		resp, err := CallInitializeTasks(nodeAddr, topology.JobID, topology, assignments, l.JobSpec, sourceDir, inputRate, l.server)
		if err != nil {
			LogError(true, "Leader: Failed to send topology to node %s: %v", nodeAddr, err)
			errors = append(errors, fmt.Errorf("node %s: %w", nodeAddr, err))
			continue
		}

		if !resp.Success {
			LogError(true, "Leader: Node %s failed to initialize tasks: %s", nodeAddr, resp.Message)
			errors = append(errors, fmt.Errorf("node %s: %s", nodeAddr, resp.Message))
			continue
		}

		LogInfo(true, "Leader: Node %s initialized tasks successfully", nodeAddr)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to initialize tasks on %d nodes: %v", len(errors), errors)
	}
	// TODO: Handle failures

	LogInfo(true, "Leader: All nodes initialized tasks successfully")
	return nil
}

// SendTopologyUpdate sends topology updates to all nodes
// This updates existing tasks' InputTasks and OutputTargets on nodes
func (l *Leader) SendTopologyUpdate() error {
	l.mu.RLock()
	topology := l.Topology
	assignments := l.Assignments
	l.mu.RUnlock()

	if topology == nil {
		return fmt.Errorf("no topology to send")
	}

	LogInfo(true, "Leader: Sending topology update to nodes for job %s", topology.JobID)

	// Get all unique nodes that have tasks assigned
	nodeSet := make(map[string]bool)
	for _, nodeAddr := range assignments {
		nodeSet[nodeAddr] = true
	}

	LogInfo(true, "Leader: Sending topology update to %d nodes", len(nodeSet))

	// Send topology update to each node
	var errors []error
	for nodeAddr := range nodeSet {
		resp, err := CallUpdateTopology(nodeAddr, topology, l.server)
		if err != nil {
			LogError(true, "Leader: Failed to send topology update to node %s: %v", nodeAddr, err)
			errors = append(errors, fmt.Errorf("node %s: %w", nodeAddr, err))
			continue
		}

		if !resp.Success {
			LogError(true, "Leader: Node %s failed to update topology: %s", nodeAddr, resp.Message)
			errors = append(errors, fmt.Errorf("node %s: %s", nodeAddr, resp.Message))
			continue
		}

		LogInfo(true, "Leader: Node %s successfully updated topology", nodeAddr)
	}

	if len(errors) > 0 {
		return fmt.Errorf("failed to update topology on %d nodes: %v", len(errors), errors)
	}

	LogInfo(true, "Leader: All nodes successfully updated topology")
	return nil
}

// UpdateMetrics updates metrics and task status from a node's report.
func (l *Leader) UpdateMetrics(report *NodeMetricsReport) {
	// First update in-memory metrics under lock.
	l.mu.Lock()
	for _, metrics := range report.TaskMetrics {
		l.Metrics[metrics.TaskID] = metrics
	}

	// Update last known task status for each reported task.
	for _, statusReport := range report.TaskStatus {
		if l.TaskStatuses == nil {
			l.TaskStatuses = make(map[string]TaskStatus)
		}
		l.TaskStatuses[statusReport.TaskID] = statusReport.Status
		LogInfo(false, "Leader: Task %s status: %d (from node %s)", statusReport.TaskID, statusReport.Status, report.NodeID)
	}
	l.mu.Unlock()

	LogInfo(false, "Leader: Updated metrics for %d tasks from node %s", len(report.TaskMetrics), report.NodeID)
}

// startTaskHealthMonitor periodically checks task statuses and attempts to
// reinitialize failed or stopped tasks when exactly-once is enabled.
func (l *Leader) startTaskHealthMonitor() {
	ticker := time.NewTicker(1 * time.Second) // TODO : move this to config
	for range ticker.C {
		l.mu.RLock()
		job := l.JobSpec
		if job == nil || l.Topology == nil {
			l.mu.RUnlock()
			continue
		}

		// Collect tasks that need to be restarted outside the lock.
		type restartInfo struct {
			taskID   string
			nodeAddr string
			status   TaskStatus
		}
		var toRestart []restartInfo

		for taskID, status := range l.TaskStatuses {
			if status == TaskRunning {
				continue
			}

			task, ok := l.Topology.Tasks[taskID]
			if !ok || task == nil {
				continue
			}

			nodeAddr, ok := l.Assignments[taskID]
			if !ok || nodeAddr == "" {
				continue
			}

			toRestart = append(toRestart, restartInfo{
				taskID:   taskID,
				nodeAddr: nodeAddr,
				status:   status,
			})
		}
		l.mu.RUnlock()

		for _, r := range toRestart {
			l.mu.RLock()
			task := l.Topology.Tasks[r.taskID]
			l.mu.RUnlock()
			if task == nil {
				continue
			}

			LogInfo(true, "Leader: Detected non-running task %s on %s (status=%d); attempting reinitialize (ExactlyOnce=%v)",
				r.taskID, r.nodeAddr, r.status, task.ExactlyOnce)

			if err := l.AddTaskToNode(task, r.nodeAddr); err != nil {
				LogError(true, "Leader: Failed to reinitialize task %s on node %s: %v", r.taskID, r.nodeAddr, err)
			}
		}
	}
}

// computeTaskAssignments computes which tasks should run on which nodes (round-robin)
func (l *Leader) computeTaskAssignments(topology *Topology) (map[string]string, error) {
	if l.server == nil || l.server.Members == nil {
		return nil, fmt.Errorf("server or members not available")
	}

	// Get all alive members (excluding self)
	aliveMembers := l.server.Members.GetAliveMembers(l.server.Addr)
	if len(aliveMembers) == 0 {
		return nil, fmt.Errorf("no alive members available")
	}

	assignments := make(map[string]string)
	nodeIndex := 0

	// Assign each task to a node in round-robin fashion
	for taskID := range topology.Tasks {
		nodeAddr := aliveMembers[nodeIndex%len(aliveMembers)].Address
		assignments[taskID] = nodeAddr
		nodeIndex++
		LogInfo(true, "Leader: Assigned task %s to node %s", taskID, nodeAddr)
	}

	// Pass 1: Set NodeID and Address for all tasks (all tasks on same node use node port + 2000)
	for taskID, task := range topology.Tasks {
		if nodeAddr, exists := assignments[taskID]; exists {
			task.NodeID = nodeAddr

			// Extract host and port from node address, add 2000 to port
			host, portStr, _ := net.SplitHostPort(nodeAddr)

			port, _ := strconv.Atoi(portStr)

			// Set address with port + 2000
			task.Address = fmt.Sprintf("%s:%d", host, port+2000)
			LogInfo(true, "Leader: Set task %s NodeID=%s, Address=%s", taskID, nodeAddr, task.Address)
		}
	}

	// Pass 2: Fill InputTasks and OutputTargets (now all Addresses are set)
	for taskID, task := range topology.Tasks {
		// Update InputTasks with actual addresses for upstream tasks (for ACKs)
		for upstreamTaskID := range task.InputTasks {
			if _, exists := assignments[upstreamTaskID]; exists {
				// Find the address for the upstream task (already set in Pass 1)
				if upstreamTask, exists := topology.Tasks[upstreamTaskID]; exists {
					task.InputTasks[upstreamTaskID] = upstreamTask.Address
					LogInfo(true, "Leader: Set InputTask for task %s <- %s at %s", taskID, upstreamTaskID, upstreamTask.Address)
				}
			}
		}

		// Update OutputTargets with actual addresses for downstream tasks
		for downstreamTaskID := range task.OutputTargets {
			if _, exists := assignments[downstreamTaskID]; exists {
				// Find the address for the downstream task (already set in Pass 1)
				if downstreamTask, exists := topology.Tasks[downstreamTaskID]; exists {
					task.OutputTargets[downstreamTaskID] = downstreamTask.Address
					LogInfo(true, "Leader: Set OutputTarget for task %s -> %s at %s", taskID, downstreamTaskID, downstreamTask.Address)
				}
			}
		}
	}

	// Remember where we left off so future autoscale scale-ups can continue
	// the round-robin placement from the same point.
	l.nextScaleNode = nodeIndex % len(aliveMembers)
	// Comment: we may not need this. This can be computed dynamically.

	return assignments, nil
}

// Leader starts streaming and acts as a source as well
func (l *Leader) StartStreaming() error {

	// Start source task to stream data to stage 1 tasks
	l.mu.RLock()
	topology := l.Topology
	job := l.JobSpec
	l.mu.RUnlock()

	// Initialize ExactlyOnceManager if exactly-once is enabled

	l.mu.Lock()
	if l.ExactlyOnceMgr == nil && (job.ExactlyOnce || job.Autoscale) {
		// Leader is a source (isSource=true) and not final stage (isFinalStage=false).
		// No WAL file name is needed for the leader since it's just tracking sent tuples.
		l.ExactlyOnceMgr = NewExactlyOnceManager(true, false, nil, nil)
		LogInfo(true, "Leader: Initialized ExactlyOnceManager for source streaming")
	}
	l.mu.Unlock()

	if job != nil {
		go l.startSourceTask(job, topology)
	}

	return nil
}

// startSourceTask reads from source directory and streams tuples to stage 1 tasks
func (l *Leader) startSourceTask(job *JobSpec, topology *Topology) {
	LogInfo(true, "Leader: Starting source task - reading from %s", job.SourceDir)

	if job.SourceDir == "" {
		LogError(true, "Leader: SourceDir is empty, cannot read files")
		return
	}

	// Check if source directory exists (only once)
	fileInfo, err := os.Stat(job.SourceDir)
	if err != nil {
		LogError(true, "Leader: SourceDir does not exist: %s, error: %v", job.SourceDir, err)
		return
	}

	// Get all stage 1 tasks from topology
	stage1Tasks := make([]*Task, 0)
	for _, task := range topology.Tasks {
		if task.Stage == 1 {
			stage1Tasks = append(stage1Tasks, task)
		}
	}

	if len(stage1Tasks) == 0 {
		LogError(true, "Leader: No stage 1 tasks found in topology")
		return
	}

	LogInfo(true, "Leader: Found %d stage 1 tasks to stream to", len(stage1Tasks))

	// Calculate rate limiting delay if InputRate is specified
	var delay time.Duration
	if job.InputRate > 0 {
		delay = time.Second / time.Duration(job.InputRate)
	}

	// If it's a file, read it directly; if it's a directory, read all files in it
	if !fileInfo.IsDir() {
		// It's a single file, read it directly
		LogInfo(true, "Leader: SourceDir is a file, reading directly: %s", job.SourceDir)
		fileName := filepath.Base(job.SourceDir)
		l.readFileAndEmitTuples(job.SourceDir, fileName, delay, stage1Tasks)
		return
	}

	// Read all files from source directory
	files, err := os.ReadDir(job.SourceDir)
	if err != nil {
		LogError(true, "Leader: Failed to read source directory %s: %v", job.SourceDir, err)
		return
	}

	// Process each file
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(job.SourceDir, file.Name())
		l.readFileAndEmitTuples(filePath, file.Name(), delay, stage1Tasks)
	}

	LogInfo(true, "Leader: Finished reading all files from source directory")
}

// readFileAndEmitTuples reads a file and emits tuples for each line, routing to stage 1 tasks
func (l *Leader) readFileAndEmitTuples(filePath string, fileName string, delay time.Duration, stage1Tasks []*Task) {
	file, err := os.Open(filePath)
	if err != nil {
		LogError(true, "Leader: Failed to open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	// Read file line by line
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	ticker := time.NewTicker(delay)
	defer ticker.Stop()

	for scanner.Scan() {
		// Rate limiting: sleep if InputRate is specified
		<-ticker.C
		line := scanner.Text()
		lineNumber++

		// Create tuple with format: key = "filename:linenumber", value = line
		key := fmt.Sprintf("%s:%d", fileName, lineNumber)
		tuple := &Tuple{
			ID:        fmt.Sprintf("leader-%s-%d", fileName, lineNumber),
			Key:       key,
			Value:     line,
			Timestamp: time.Now(),
			SenderID:  "leader",
		}

		// Route tuple to stage 1 task based on key hashing
		selectedTask := selectTaskByKey(key, stage1Tasks)
		if selectedTask == nil {
			LogError(true, "Leader: Failed to select stage 1 task for tuple key=%s", key)
			continue
		}

		// Set target task ID
		tuple.TaskID = selectedTask.TaskID

		// Send tuple to selected stage 1 task (tracking happens inside SendTuple if manager is provided)
		_, err := SendTuple(selectedTask.Address, tuple, l.ExactlyOnceMgr, tuple.ID)
		if err != nil {
			LogError(true, "Leader: Failed to send tuple ID=%s (key=%s) to stage 1 task %s at %s: %v",
				tuple.ID, key, selectedTask.TaskID, selectedTask.Address, err)
			continue
		}

		LogInfo(true, "Leader: Sent tuple ID=%s (key=%s) to stage 1 task %s at %s",
			tuple.ID, key, selectedTask.TaskID, selectedTask.Address)

	}

	if err := scanner.Err(); err != nil {
		LogError(true, "Leader: Error reading file %s: %v", filePath, err)
		return
	}

	LogInfo(true, "Leader: Read %d lines from file %s", lineNumber, fileName)
}

// PrintTopology prints the topology in a formatted UI manner showing stages, tasks, and VM IDs
func (l *Leader) PrintTopology() {
	l.mu.RLock()
	topology := l.Topology
	assignments := l.Assignments
	l.mu.RUnlock()

	if topology == nil {
		ConsolePrintln("╔════════════════════════════════════╗")
		ConsolePrintln("║     No Topology Available          ║")
		ConsolePrintln("╚════════════════════════════════════╝")
		return
	}

	// Group tasks by stage
	stageTasks := make(map[int][]*Task)
	for _, task := range topology.Tasks {
		stageTasks[task.Stage] = append(stageTasks[task.Stage], task)
	}

	// Sort stages
	stages := make([]int, 0, len(stageTasks))
	for stage := range stageTasks {
		stages = append(stages, stage)
	}
	sort.Ints(stages)

	// Sort tasks within each stage by task ID for consistent display
	for stage := range stageTasks {
		sort.Slice(stageTasks[stage], func(i, j int) bool {
			return stageTasks[stage][i].TaskID < stageTasks[stage][j].TaskID
		})
	}

	// Print header
	ConsolePrintln()
	ConsolePrintln("╔════════════════════════════════════════════════════════════════════════════")
	ConsolePrintf("║  Topology: %-65s \n", topology.JobID)
	ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════")
	ConsolePrintf("║  Total Stages: %-3d  |  Total Tasks: %-3d                                  \n", len(stages), len(topology.Tasks))
	ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════")

	// Print each stage
	for _, stage := range stages {
		tasks := stageTasks[stage]
		ConsolePrintf("║  Stage %-3d (%d task(s))                                                          \n", stage, len(tasks))
		ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════")
		ConsolePrintln("║  Task ID                                    │  VM ID (Node Address)         ")
		ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════")

		// Print each task in this stage
		for _, task := range tasks {
			vmID := task.NodeID
			if vmID == "" {
				// Try to get from assignments if NodeID is not set
				if nodeAddr, exists := assignments[task.TaskID]; exists {
					vmID = nodeAddr
				} else {
					vmID = "N/A"
				}
			}
			ConsolePrintf("║  %-42s │  %-29s \n", task.TaskID, vmID)
		}
		ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════")
	}

	ConsolePrintln("╚════════════════════════════════════════════════════════════════════════════")
	ConsolePrintln()
}

// PrintStageInputRates prints the input rate (throughput) for each stage
func (l *Leader) PrintStageInputRates() {
	l.mu.RLock()
	topology := l.Topology
	metrics := l.Metrics
	l.mu.RUnlock()

	if topology == nil {
		ConsolePrintln("╔════════════════════════════════════╗")
		ConsolePrintln("║     No Topology Available         ║")
		ConsolePrintln("╚════════════════════════════════════╝")
		return
	}

	// Aggregate throughput per stage
	stageThroughput := make(map[int]float64)
	for taskID, taskMetrics := range metrics {
		if taskMetrics == nil {
			continue
		}
		task, ok := topology.Tasks[taskID]
		if !ok || task == nil {
			continue
		}
		stageThroughput[task.Stage] += taskMetrics.Throughput
	}

	// Get sorted stages from topology (to show all stages even if no metrics)
	stages := make([]int, 0)
	if len(topology.Stages) > 0 {
		stages = append(stages, topology.Stages...)
		sort.Ints(stages)
	} else {
		// Fallback: get stages from tasks if Stages list is empty
		stageSet := make(map[int]bool)
		for _, task := range topology.Tasks {
			if task != nil {
				stageSet[task.Stage] = true
			}
		}
		for stage := range stageSet {
			stages = append(stages, stage)
		}
		sort.Ints(stages)
	}

	// Print header
	ConsolePrintln()
	ConsolePrintln("╔════════════════════════════════════════════════════════════════════════════╗")
	ConsolePrintln("║  Stage Input Rates (Tuples per Second)                                     ║")
	ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════╣")
	ConsolePrintln("║  Stage  │  Input Rate (tuples/sec)                                        ║")
	ConsolePrintln("╠════════════════════════════════════════════════════════════════════════════╣")

	// Print each stage's input rate
	for _, stage := range stages {
		rate := stageThroughput[stage]
		if rate == 0 {
			ConsolePrintf("║  %-5d  │  %-55s ║\n", stage, "0.00 (no metrics)")
		} else {
			ConsolePrintf("║  %-5d  │  %-55.2f ║\n", stage, rate)
		}
	}

	// If no stages available, show message
	if len(stages) == 0 {
		ConsolePrintln("║  No stages available                                                    ║")
	}

	ConsolePrintln("╚════════════════════════════════════════════════════════════════════════════╝")
	ConsolePrintln()
}
