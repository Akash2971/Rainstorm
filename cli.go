package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

func (s *Server) startCLI(cmdChan chan<- string) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmdChan <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		LogError(true, "Error reading input: %v", err)
	}
}

// splitCommandLine splits a command string into arguments, treating substrings
// wrapped in double quotes as a single argument. Quotes are stripped.
// Example: `RainStorm 2 1 "./filter::ERROR with space" ./aggregate`
// becomes: ["RainStorm", "2", "1", "./filter::ERROR with space", "./aggregate"].
func splitCommandLine(cmd string) []string {
	var args []string
	var current []rune
	inQuotes := false

	for _, r := range cmd {
		switch r {
		case '"':
			// Toggle quote state, but don't include the quote itself.
			inQuotes = !inQuotes
		case ' ', '\t':
			if inQuotes {
				current = append(current, r)
			} else if len(current) > 0 {
				args = append(args, string(current))
				current = current[:0]
			}
		default:
			current = append(current, r)
		}
	}

	if len(current) > 0 {
		args = append(args, string(current))
	}

	return args
}

func (s *Server) handleCommand(cmd string) {
	// Split command into parts at the beginning.
	// Use a custom splitter that supports double-quoted arguments
	// so stage operation args can contain spaces.
	parts := splitCommandLine(strings.TrimSpace(cmd))
	if len(parts) == 0 {
		return
	}

	command := parts[0]

	switch command {
	case "list_self":
		ConsolePrintf("Self ID: %s\n", s.ID())
	case "leave":
		s.leaveGroup()
	case "display_suspects":
		s.Members.PrintSuspectedNodes()
	case "display_protocol":
		printCurrentProtocol()
	case "switch":
		if len(parts) != 3 {
			ConsolePrintf("Invalid switch command format. Expected: switch <protocol> <suspicion>\n")
			return
		}
		s.handleSwitch(parts[1], parts[2])
	case "drop":
		if len(parts) != 2 {
			ConsolePrintf("Invalid drop command format. Expected: drop <percentage>\n")
			return
		}
		SetMessageDropRate(parts[1])
	// Usage: create text file in mp2 directory and call command using "create <local filename> <HyDFS filename>"
	case "create":
		if len(parts) != 3 {
			ConsolePrintf("Invalid create command format. Expected: create <localfilename> <HyDFSfilename>\n")
			return
		}
		s.handleCreate(parts[1], parts[2])
	case "get":
		if len(parts) != 3 {
			ConsolePrintf("Invalid get command format. Expected: get <HyDFSfilename> <localfilename>\n")
			return
		}
		s.handleGet(parts[1], parts[2])
	case "append":
		if len(parts) != 3 {
			ConsolePrintf("Invalid append command format. Expected: append <localfilename> <HyDFSfilename>\n")
			return
		}
		s.handleAppend(parts[1], parts[2])
	case "merge":
		if len(parts) != 2 {
			ConsolePrintf("Invalid merge command format. Expected: merge <HyDFSfilename>\n")
			return
		}
		s.handleMergeCommand(parts[1])
	case "multiappend":
		if len(parts) < 4 {
			ConsolePrintf("Invalid multiappend command format. Expected: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>\n")
			return
		}
		hyDFSfilename, vmNames, localFiles := s.parseMultiAppend(parts[1:])
		s.handleMultiAppend(hyDFSfilename, vmNames, localFiles)
	case "printmeta": //for testing purposes
		if len(parts) != 2 {
			ConsolePrintf("Invalid printmeta command format. Expected: printmeta <HyDFSfilename>\n")
			return
		}
		s.Metadata.PrintFileMetadata(parts[1])
	case "ls":
		if len(parts) != 2 {
			ConsolePrintf("Invalid ls command format. Expected: ls <HyDFSfilename>\n")
			return
		}
		s.handleLS(parts[1])
	case "liststore":
		s.handleListStore()
	case "list_mem_ids":
		s.Members.Print(true)
	case "getfromreplica":
		if len(parts) != 4 {
			ConsolePrintf("Invalid getfromreplica command format. Expected: getfromreplica <VMID> <HyDFSfilename> <localfilename>\n")
			return
		}
		s.handleGetFromReplica(parts[1], parts[2], parts[3])
	case "RainStorm":
		s.handleRainStorm(parts[1:])
	case "list_tasks":
		s.handleListTasks()
	case "kill_task":
		if len(parts) != 3 {
			ConsolePrintf("Invalid kill_task command format. Expected: kill_task <VM_NAME> <PID>\n")
			return
		}
		s.handleKillTask(parts[1], parts[2])
	case "print_topology":
		s.handlePrintTopology()
	case "print_stage_rates":
		s.handlePrintStageRates()
	default:
		ConsolePrintf("Unknown command: %s\n", command)
	}
}

func (s *Server) leaveGroup() {
	LogInfo(true, "Initiating graceful leave from the group...")

	// Get current membership snapshot
	snapshot := s.Members.Snapshot()
	selfMember, exists := snapshot[s.ID()]

	if !exists {
		LogError(true, "Error: Self not found in membership list")
		return
	}

	// Mark self as voluntarily leaving
	selfMember.MarkVoluntaryLeave()
	selfMember.Incarnation = s.IncarnationNumber // Use current incarnation
	s.Members.AddOrUpdate(selfMember)

	LogInfo(true, "MEMBER_LEAVE: Marked self as voluntarily leaving: %s", s.ID())
}

func (s *Server) handleSwitch(protocolStr, suspicionStr string) {
	protocolStr = strings.ToLower(protocolStr)
	suspicionStr = strings.ToLower(suspicionStr)

	protocol, ok := ProtocolMap[protocolStr]
	if !ok {
		ConsolePrintf("Invalid protocol '%s'\n", protocolStr)
		return
	}

	suspicion, ok := SuspicionMap[suspicionStr]
	if !ok {
		ConsolePrintf("Invalid suspicion type '%s'\n", suspicionStr)
		return
	}

	SwitchProtocol(protocol, suspicion)

	// Broadcast protocol switch to all other nodes in the membership list
	snapshot := s.Members.Snapshot()

	for _, member := range snapshot {
		// Skip self
		if member.Address != s.Addr {
			go func(nodeAddr string) {
				resp, err := CallProtocolSwitch(nodeAddr, s.ID(), protocol, suspicion, s)
				if err != nil {
					LogError(true, "Failed to send protocol switch to %s: %v", nodeAddr, err)
				} else {
					LogInfo(true, "Sent protocol switch to %s, success: %v", nodeAddr, resp.Success)
				}
			}(member.Address)
		}
	}

	ConsolePrintln("Protocol switch broadcast completed")
}

func printCurrentProtocol() {
	ConsolePrintf("Current Protocol: %s | Suspicion: %s | Message Drop Rate: %.2f%%\n",
		Config.Protocol, Config.Suspicion, Config.MessageDropRate*100)
}

// handleListTasks queries all nodes (via the leader) for running task processes
// and prints their VM, PID, stage, task ID, and executable.
func (s *Server) handleListTasks() {
	// Only the leader has the full assignment map for tasks.
	if s.Leader == nil {
		ConsolePrintf("list_tasks can only be run on the leader (introducer).\n")
		return
	}

	s.Leader.mu.RLock()
	assignments := s.Leader.Assignments
	s.Leader.mu.RUnlock()

	if len(assignments) == 0 {
		ConsolePrintf("No tasks are currently assigned.\n")
		return
	}

	// Build the set of unique node addresses that host tasks.
	nodeSet := make(map[string]bool)
	for _, nodeAddr := range assignments {
		nodeSet[nodeAddr] = true
	}

	ConsolePrintf("Listing tasks across %d node(s):\n", len(nodeSet))

	for nodeAddr := range nodeSet {
		resp, err := CallListTasks(nodeAddr, s)
		if err != nil {
			ConsolePrintf("  Node %s: RPC error: %v\n", nodeAddr, err)
			continue
		}
		if !resp.Success {
			ConsolePrintf("  Node %s: error: %s\n", nodeAddr, resp.Message)
			continue
		}

		for _, t := range resp.Tasks {
			walFile := fmt.Sprintf("wal-%s.log", t.TaskID)
			ConsolePrintf("  vm=%s node=%s stage=%d task=%s pid=%d exe=%s wal=%s\n",
				t.VM, nodeAddr, t.Stage, t.TaskID, t.PID, t.OpExe, walFile)
		}
	}
}

// handleKillTask kills a task given a VM name and PID by finding the corresponding
// task on that VM and issuing a KillTask RPC to the node hosting it.
// Usage: kill_task <VM_NAME> <PID>
func (s *Server) handleKillTask(vmName string, pidStr string) {
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		ConsolePrintf("Invalid PID '%s'. PID must be a positive integer.\n", pidStr)
		return
	}

	// Resolve VM name to node address using the membership list.
	if s.Members == nil {
		ConsolePrintf("Membership list is not available; cannot resolve VM name.\n")
		return
	}

	var nodeAddr string
	for _, member := range s.Members.GetAll() {
		if AddressToVMName[member.Address] == vmName {
			nodeAddr = member.Address
			break
		}
	}
	if nodeAddr == "" {
		ConsolePrintf("Could not find node for VM '%s'. Ensure the VM is part of the membership list.\n", vmName)
		return
	}

	// Ask the target node for its task list to map PID -> TaskID.
	resp, err := CallListTasks(nodeAddr, s)
	if err != nil {
		ConsolePrintf("Failed to list tasks on node %s: %v\n", nodeAddr, err)
		return
	}
	if !resp.Success {
		ConsolePrintf("Node %s failed to list tasks: %s\n", nodeAddr, resp.Message)
		return
	}

	var targetTaskID string
	for _, t := range resp.Tasks {
		if t.PID == pid && (t.VM == "" || t.VM == vmName) {
			targetTaskID = t.TaskID
			break
		}
	}
	if targetTaskID == "" {
		ConsolePrintf("No task found on VM %s with PID %d\n", vmName, pid)
		return
	}

	// Issue KillTask RPC to the node that hosts this task.
	killResp, err := CallKillTask(nodeAddr, targetTaskID, s)
	if err != nil {
		ConsolePrintf("Failed to send KillTask to %s for task %s (PID=%d): %v\n", nodeAddr, targetTaskID, pid, err)
		return
	}
	if !killResp.Success {
		ConsolePrintf("Node %s failed to kill task %s (PID=%d): %s\n", nodeAddr, targetTaskID, pid, killResp.Message)
		return
	}

	ConsolePrintf("Killed task %s on VM %s (node=%s, PID=%d)\n", targetTaskID, vmName, nodeAddr, pid)
}

// parseMultiAppend parses multiappend command: multiappend HyDFSfilename VM1 ... VMN localfile1 ... localfileN
// Format: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>
// VM format: vm1, vm2, vm3, etc.
func (s *Server) parseMultiAppend(args []string) (string, []string, []string) {
	if len(args) < 3 {
		ConsolePrintf("Invalid multiappend: need at least HyDFSfilename, one VM, and one local file\n")
		return "", nil, nil
	}

	hyDFSfilename := args[0]

	// Validate that first argument is not a VM name
	if strings.HasPrefix(strings.ToLower(hyDFSfilename), "vm") {
		ConsolePrintf("Invalid multiappend: first argument should be HyDFSfilename, not VM name. Format: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>\n")
		return "", nil, nil
	}

	// Parse arguments: VMs are vm1, vm2, etc., local files follow after all VMs
	var vmNames []string
	var localFiles []string

	// Find all consecutive VM names starting from index 1
	// VM names should start with "vm" (case-insensitive)
	splitIndex := -1
	for i := 1; i < len(args); i++ {
		if strings.HasPrefix(strings.ToLower(args[i]), "vm") {
			vmNames = append(vmNames, args[i])
		} else {
			// First non-VM argument marks the start of local files
			splitIndex = i
			break
		}
	}

	if splitIndex == -1 {
		ConsolePrintf("Invalid multiappend: could not find local files. Format: multiappend <HyDFSfilename> <VM1> ... <VMN> <localfile1> ... <localfileN>\n")
		return "", nil, nil
	}

	localFiles = args[splitIndex:]

	// Validate: number of VMs should match number of local files
	if len(vmNames) != len(localFiles) {
		ConsolePrintf("Invalid multiappend: number of VMs (%d) must match number of local files (%d)\n",
			len(vmNames), len(localFiles))
		return "", nil, nil
	}

	if len(vmNames) == 0 {
		ConsolePrintf("Invalid multiappend: at least one VM is required\n")
		return "", nil, nil
	}

	ConsolePrintf("Parsed multiappend: HyDFSfilename=%s, VMs=%v, LocalFiles=%v\n",
		hyDFSfilename, vmNames, localFiles)

	return hyDFSfilename, vmNames, localFiles
}

// handleRainStorm parses and handles RainStorm job submission command
// Format: RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op2_exe> ... <opNstages_exe> <hydfs_src_directory> <hydfs_dest_filename> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>
// Each operation can include arguments using "::" as delimiter: "exe::arg1::arg2::..."
// Example: "filter::ERROR" means exe=filter, args=["ERROR"]
// Example: "transform" means exe=transform, args=[]
// TODO: modify the filter to pass regex and other expressions.
func (s *Server) handleRainStorm(args []string) {
	// Minimum required: 2 (Nstages, Ntasks_per_stage) + Nstages (exe paths) + 7 (fixed args at end)
	if len(args) < 9 {
		ConsolePrintf("Invalid RainStorm command format. Expected: RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op2_exe> ... <opNstages_exe> <hydfs_src_directory> <hydfs_dest_filename> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>\n")
		return
	}

	// Parse NStages and NTasksPerStage
	nStages, err := strconv.Atoi(args[0])
	if err != nil {
		ConsolePrintf("Invalid NStages: %s\n", args[0])
		return
	}

	nTasksPerStage, err := strconv.Atoi(args[1])
	if err != nil {
		ConsolePrintf("Invalid NTasksPerStage: %s\n", args[1])
		return
	}

	// Parse fixed arguments from the end (last 7 arguments)
	last7Args := args[len(args)-7:]
	hydfsSrcDir := last7Args[0]
	hydfsDestFile := last7Args[1]
	exactlyOnceStr := last7Args[2]
	autoscaleEnabledStr := last7Args[3]
	inputRateStr := last7Args[4]
	lwStr := last7Args[5]
	hwStr := last7Args[6]

	exactlyOnce, err := strconv.ParseBool(exactlyOnceStr)
	if err != nil {
		ConsolePrintf("Invalid exactly_once: %s (expected true/false)\n", exactlyOnceStr)
		return
	}

	autoscaleEnabled, err := strconv.ParseBool(autoscaleEnabledStr)
	if err != nil {
		ConsolePrintf("Invalid autoscale_enabled: %s (expected true/false)\n", autoscaleEnabledStr)
		return
	}

	inputRate, err := strconv.Atoi(inputRateStr)
	if err != nil {
		ConsolePrintf("Invalid INPUT_RATE: %s\n", inputRateStr)
		return
	}

	lw, err := strconv.Atoi(lwStr)
	if err != nil {
		ConsolePrintf("Invalid LW: %s\n", lwStr)
		return
	}

	hw, err := strconv.Atoi(hwStr)
	if err != nil {
		ConsolePrintf("Invalid HW: %s\n", hwStr)
		return
	}

	// Parse operation executables (args between beginning and the last 7 args)
	operationExes := args[2 : len(args)-7]

	if len(operationExes) < nStages {
		ConsolePrintf("Invalid RainStorm command: not enough operation executables. Expected %d, got %d\n", nStages, len(operationExes))
		return
	}

	// Create StageOps - parse exe and args
	// Format: "exe" or "exe::arg1::arg2::..." (using :: as delimiter)
	stageOps := buildStageOps(operationExes, nStages)

	host, portStr, err := net.SplitHostPort(s.IntroducerAddr)
	if err != nil {
		LogError(true, "Failed to parse UDP address: %v", err)
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		LogError(true, "Failed to parse port: %v", err)
		return
	}

	sourcePort := port + 2000
	sourceAddress := fmt.Sprintf("%s:%d", host, sourcePort)

	// Create JobSpec
	jobSpec := &JobSpec{
		NStages:       nStages,
		TasksPerStage: nTasksPerStage,
		StageOps:      stageOps,
		SourceDir:     hydfsSrcDir,
		DestFile:      hydfsDestFile,
		SourceAddress: sourceAddress, // introducer will be the source
		ExactlyOnce:   exactlyOnce,
		Autoscale:     autoscaleEnabled,
		InputRate:     inputRate,
		LW:            lw,
		HW:            hw,
	}

	LogInfo(true, "Parsed RainStorm job: NStages=%d, NTasksPerStage=%d, ExactlyOnce=%v, Autoscale=%v, InputRate=%d, LW=%d, HW=%d",
		nStages, nTasksPerStage, exactlyOnce, autoscaleEnabled, inputRate, lw, hw)

	// Submit job - if this node is leader, submit directly; else send RPC to introducer/leader
	if s.IsIntroducer && s.Leader != nil {
		err := s.Leader.SubmitJob(jobSpec)
		if err != nil {
			ConsolePrintf("Failed to submit job: %v\n", err)
			LogError(true, "Failed to submit job: %v", err)
		} else {
			ConsolePrintf("Job submitted successfully\n")
		}
	} else {
		err := s.sendSubmitJobToLeader(jobSpec)
		if err != nil {
			ConsolePrintf("Failed to submit job to leader: %v\n", err)
			LogError(true, "Failed to submit job to leader: %v", err)
		} else {
			ConsolePrintf("Job submitted to leader successfully\n")
		}
	}
}

// buildStageOps parses operation executable strings into StageOp values.
// Each operation string has format: "exe" or "exe::arg1::arg2::...".
func buildStageOps(operationExes []string, nStages int) []StageOp {
	stageOps := make([]StageOp, nStages)
	for i := 0; i < nStages; i++ {
		exeWithArgs := operationExes[i]

		// Split by "::" to separate exe from args
		parts := strings.Split(exeWithArgs, "::")
		exe := parts[0]
		var exeArgs []string

		if len(parts) > 1 {
			// Has arguments - everything after first part is an argument
			exeArgs = parts[1:]
		} else {
			// No arguments
			exeArgs = []string{}
		}

		stageOps[i] = StageOp{
			Exe:  exe,
			Args: exeArgs,
		}

		if len(exeArgs) > 0 {
			LogInfo(true, "Parsed stage %d operation: exe=%s, args=%v", i+1, exe, exeArgs)
		} else {
			LogInfo(true, "Parsed stage %d operation: exe=%s", i+1, exe)
		}
	}

	// If we have a filter followed by aggregate, move the first
	// argument of aggregate to be the (optional) second argument
	// of filter (used by filter as its 3rd process arg).
	for i := 0; i < nStages-1; i++ {
		curr := stageOps[i].Exe
		next := stageOps[i+1].Exe

		if strings.Contains(curr, "filter") && strings.Contains(next, "aggregate") && len(stageOps[i+1].Args) > 0 {
			aggArg := stageOps[i+1].Args[0]

			// Only add if filter doesn't already have a second argument.
			if len(stageOps[i].Args) < 2 {
				stageOps[i].Args = append(stageOps[i].Args, aggArg)
			}

			// Drop the argument from aggregate.
			stageOps[i+1].Args = stageOps[i+1].Args[1:]
		}
	}

	return stageOps
}

// sendSubmitJobToLeader sends job submission request to the leader (introducer)
func (s *Server) sendSubmitJobToLeader(jobSpec *JobSpec) error {
	LogInfo(true, "Sending job submission request to leader (introducer) at %s", s.IntroducerAddr)

	resp, err := CallSubmitJob(s.IntroducerAddr, jobSpec, s)
	if err != nil {
		return fmt.Errorf("RPC call failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("leader returned error: %s", resp.Message)
	}

	return nil
}

// handlePrintTopology prints the topology showing stages, tasks, and VM IDs
func (s *Server) handlePrintTopology() {
	if s.Leader == nil {
		ConsolePrintf("print_topology can only be run on the leader (introducer).\n")
		return
	}

	s.Leader.PrintTopology()
}

// handlePrintStageRates prints the input rate (throughput) for each stage
func (s *Server) handlePrintStageRates() {
	if s.Leader == nil {
		ConsolePrintf("print_stage_rates can only be run on the leader (introducer).\n")
		return
	}

	s.Leader.PrintStageInputRates()
}
