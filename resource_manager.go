package main

import (
	"fmt"
	"net"
	"strconv"
	"time"
)

// Autoscale performs autoscaling based on metrics.
func (l *Leader) Autoscale() error {
	if l.JobSpec == nil || !l.JobSpec.Autoscale {
		LogInfo(false, "Leader: Autoscale - autoscaling not enabled")
		return nil
	}

	if l.Topology == nil || len(l.Metrics) == 0 {
		LogInfo(false, "Leader: Autoscale - no metrics available")
		return nil
	}

	// Aggregate tuples received per second (Throughput) per stage and count tasks per stage.
	l.mu.Lock()
	stageRate := l.aggregateStageThroughput()
	lw := l.JobSpec.LW
	hw := l.JobSpec.HW
	nStages := l.JobSpec.NStages
	stageTaskCounts := make(map[int]int)
	for _, t := range l.Topology.Tasks {
		if t != nil {
			stageTaskCounts[t.Stage]++
		}
	}
	l.mu.Unlock()

	if len(stageRate) == 0 {
		return nil
	}

	// For each stage, decide whether to scale up or down by one task.
	for stage, rate := range stageRate {
		count := stageTaskCounts[stage]
		if count <= 0 {
			continue
		}

		avg := rate / float64(count) // average tuples/sec per task in this stage
		LogInfo(true, "Leader: Autoscale - stage %d: total=%.2f tuples/sec, tasks=%d, avg=%.2f", stage, rate, count, avg)

		if stage < 1 || stage > nStages {
			continue
		}

		// NOTE: As per spec: avg < LW -> decrease tasks; avg > HW -> increase tasks.
		if avg < float64(lw) {
			LogInfo(true, "Leader: Autoscale - stage %d: avg %.2f < LW=%d -> REMOVE 1 task", stage, avg, lw)
			if err := l.scaleDownStage(stage); err != nil {
				LogError(true, "Leader: Failed to scale down stage %d: %v", stage, err)
			}
		} else if avg > float64(hw) {
			LogInfo(true, "Leader: Autoscale - stage %d: avg %.2f > HW=%d -> ADD 1 task", stage, avg, hw)
			if err := l.scaleUpStage(stage); err != nil {
				LogError(true, "Leader: Failed to scale up stage %d: %v", stage, err)
			}
		} else {
			LogInfo(true, "Leader: Autoscale - stage %d: avg %.2f within [%d,%d] -> no change", stage, avg, lw, hw)
		}
	}

	return nil
}

// aggregateStageThroughput groups per-task throughput into per-stage totals.
// Caller must hold at least a read lock on l.mu.
func (l *Leader) aggregateStageThroughput() map[int]float64 {
	result := make(map[int]float64)
	if l.Topology == nil || len(l.Metrics) == 0 {
		return result
	}

	for taskID, metrics := range l.Metrics {
		if metrics == nil {
			continue
		}
		task, ok := l.Topology.Tasks[taskID]
		if !ok || task == nil {
			continue
		}
		result[task.Stage] += metrics.Throughput
	}

	return result
}

// scaleUpStage adds one new task to the given stage and updates assignments and topology on all nodes.
func (l *Leader) scaleUpStage(stage int) error {
	l.mu.Lock()
	if l.Topology == nil || l.JobSpec == nil {
		l.mu.Unlock()
		return fmt.Errorf("no topology or job spec available for scaling")
	}

	if stage < 1 || stage > l.JobSpec.NStages {
		l.mu.Unlock()
		return fmt.Errorf("invalid stage %d", stage)
	}

	// Find all existing tasks in this stage to help build the new one.
	var existingStageTasks []*Task
	for _, t := range l.Topology.Tasks {
		if t.Stage == stage {
			existingStageTasks = append(existingStageTasks, t)
		}
	}

	// Use current time and count as part of ID to avoid collision.
	timestamp := time.Now().Unix()
	index := len(existingStageTasks)
	taskID := fmt.Sprintf("task-%d-%d-%d", stage, index, timestamp)

	newTask := NewTask(taskID, stage)
	newTask.ExactlyOnce = l.JobSpec.ExactlyOnce
	newTask.AutoscaleMode = l.JobSpec.Autoscale

	// Choose a node in round-robin fashion from the currently alive members.
	if l.server == nil || l.server.Members == nil {
		l.mu.Unlock()
		return fmt.Errorf("server or members not available for scaling")
	}
	aliveMembers := l.server.Members.GetAliveMembers(l.server.Addr)
	if len(aliveMembers) == 0 {
		l.mu.Unlock()
		return fmt.Errorf("no alive members available for scaling")
	}
	// Use round-robin index starting from where the last assignment left off.
	idx := l.nextScaleNode % len(aliveMembers)
	nodeAddr := aliveMembers[idx].Address
	l.nextScaleNode = (idx + 1) % len(aliveMembers)

	// Set NodeID and Address (node port + 2000).
	newTask.NodeID = nodeAddr
	host, portStr, _ := net.SplitHostPort(nodeAddr)
	port, _ := strconv.Atoi(portStr)
	newTask.Address = fmt.Sprintf("%s:%d", host, port+2000)

	// Wire InputTasks from previous stage or leader.
	if stage == 1 {
		if l.JobSpec.SourceAddress != "" {
			newTask.AddInputTask("leader", l.JobSpec.SourceAddress)
		}
	} else {
		for _, t := range l.Topology.Tasks {
			if t.Stage == stage-1 {
				newTask.AddInputTask(t.TaskID, t.Address)
				// Also add this new task as an output target of previous stage tasks.
				t.AddOutputTarget(newTask.TaskID, newTask.Address)
			}
		}
	}

	// Wire OutputTargets to next stage.
	if stage < l.JobSpec.NStages {
		for _, t := range l.Topology.Tasks {
			if t.Stage == stage+1 {
				newTask.AddOutputTarget(t.TaskID, t.Address)
				// Also add this new task as an input task to next stage tasks.
				t.AddInputTask(newTask.TaskID, newTask.Address)
			}
		}
	}

	// Register new task in topology and assignments.
	l.Topology.Tasks[newTask.TaskID] = newTask
	if l.Assignments == nil {
		l.Assignments = make(map[string]string)
	}
	l.Assignments[newTask.TaskID] = nodeAddr
	l.mu.Unlock()

	ConsolePrintf("Leader: Autoscale adding task %s to stage %d on node %s (Address=%s)\n", newTask.TaskID, stage, nodeAddr, newTask.Address)

	// Start the task on the target node (no leader lock held).
	if err := l.AddTaskToNode(newTask, nodeAddr); err != nil {
		return fmt.Errorf("failed to add task %s to node %s: %w", newTask.TaskID, nodeAddr, err)
	}

	// Propagate updated topology (InputTasks and OutputTargets) to all nodes.
	if err := l.SendTopologyUpdate(); err != nil {
		return fmt.Errorf("failed to send topology update after scaling up: %w", err)
	}
	return nil
}

// scaleDownStage removes one task from the given stage and updates assignments and topology on all nodes.
func (l *Leader) scaleDownStage(stage int) error {
	l.mu.Lock()
	if l.Topology == nil || l.JobSpec == nil {
		l.mu.Unlock()
		return fmt.Errorf("no topology or job spec available for scaling")
	}

	if stage < 1 || stage > l.JobSpec.NStages {
		l.mu.Unlock()
		return fmt.Errorf("invalid stage %d", stage)
	}

	// Collect all tasks in this stage.
	var stageTasks []*Task
	for _, t := range l.Topology.Tasks {
		if t.Stage == stage {
			stageTasks = append(stageTasks, t)
		}
	}

	// Do not scale below 1 task per stage.
	if len(stageTasks) <= 1 {
		l.mu.Unlock()
		LogInfo(true, "Leader: Autoscale - stage %d already has %d task(s); not scaling down", stage, len(stageTasks))
		return nil
	}

	// Pick a task to remove (simple heuristic: remove the last one in the slice),
	// and also pick a surviving task in the same stage to own the merged WAL.
	taskToRemove := stageTasks[len(stageTasks)-1]
	taskID := taskToRemove.TaskID

	// Choose a "keep" task in the same stage (any task != taskToRemove).
	keepTaskID := ""
	for _, t := range stageTasks {
		if t.TaskID != taskID {
			keepTaskID = t.TaskID
			break
		}
	}
	if keepTaskID == "" {
		// Should not happen because len(stageTasks) > 1, but guard anyway.
		l.mu.Unlock()
		return fmt.Errorf("no suitable survivor task found in stage %d to merge WAL for %s", stage, taskID)
	}

	nodeAddr, ok := l.Assignments[taskID]
	if !ok {
		l.mu.Unlock()
		return fmt.Errorf("no assignment found for task %s", taskID)
	}

	ConsolePrintf("Leader: Autoscale removing task %s from stage %d on node %s (keep task %s)\n", taskID, stage, nodeAddr, keepTaskID)

	// Remove references from previous and next stages.
	for _, t := range l.Topology.Tasks {
		// Remove from previous stage OutputTargets.
		if stage > 1 && t.Stage == stage-1 {
			delete(t.OutputTargets, taskID)
		}
		// Remove from next stage InputTasks.
		if stage < l.JobSpec.NStages && t.Stage == stage+1 {
			delete(t.InputTasks, taskID)
		}
	}

	// Remove from topology and assignments.
	delete(l.Topology.Tasks, taskID)
	delete(l.Assignments, taskID)
	delete(l.Metrics, taskID)
	l.mu.Unlock()

	// Stop the task process on the target node.
	if err := l.KillTaskOnNode(taskID, nodeAddr); err != nil {
		return fmt.Errorf("failed to kill task %s on node %s: %w", taskID, nodeAddr, err)
	}

	// Trigger WAL merge on the node hosting the surviving task. We look up the
	// node address for keepTaskID from assignments after releasing the lock.
	l.mu.RLock()
	keepNodeAddr, ok := l.Assignments[keepTaskID]
	l.mu.RUnlock()
	if ok && keepNodeAddr != "" {
		if _, err := CallMergeWal(keepNodeAddr, taskID, keepTaskID, l.server); err != nil {
			LogError(true, "Leader: MergeWal RPC failed for oldTask=%s keepTask=%s on node %s: %v",
				taskID, keepTaskID, keepNodeAddr, err)
		}
	} else {
		LogError(true, "Leader: Could not find assignment for keepTaskID=%s to trigger WAL merge", keepTaskID)
	}

	// Propagate updated topology to all nodes.
	if err := l.SendTopologyUpdate(); err != nil {
		return fmt.Errorf("failed to send topology update after scaling down: %w", err)
	}
	return nil
}

// KillTaskOnNode kills a task process on a specific node
// This is used for task removal or scaling down
func (l *Leader) KillTaskOnNode(taskID string, nodeAddress string) error {
	if taskID == "" {
		return fmt.Errorf("taskID is empty")
	}

	if nodeAddress == "" {
		return fmt.Errorf("nodeAddress is empty")
	}

	LogInfo(true, "Leader: Killing task %s on node %s", taskID, nodeAddress)

	// Call KillTask RPC on the target node
	resp, err := CallKillTask(nodeAddress, taskID, l.server)
	if err != nil {
		return fmt.Errorf("failed to kill task %s on node %s: %w", taskID, nodeAddress, err)
	}

	if !resp.Success {
		return fmt.Errorf("node %s failed to kill task %s: %s", nodeAddress, taskID, resp.Message)
	}

	LogInfo(true, "Leader: Successfully killed task %s on node %s", taskID, nodeAddress)
	return nil
}

// AddTaskToNode adds a new task to a specific node via RPC
// This is used for dynamic task addition (e.g., autoscaling)
func (l *Leader) AddTaskToNode(task *Task, nodeAddress string) error {
	l.mu.RLock()
	jobSpec := l.JobSpec
	l.mu.RUnlock()

	if jobSpec == nil {
		return fmt.Errorf("no job spec available")
	}

	if task == nil {
		return fmt.Errorf("task is nil")
	}

	LogInfo(true, "Leader: Adding task %s to node %s", task.TaskID, nodeAddress)

	// Call AddTask RPC on the target node
	resp, err := CallAddTask(nodeAddress, task, jobSpec, l.server)
	if err != nil {
		return fmt.Errorf("failed to add task %s to node %s: %w", task.TaskID, nodeAddress, err)
	}

	if !resp.Success {
		return fmt.Errorf("node %s failed to add task %s: %s", nodeAddress, task.TaskID, resp.Message)
	}

	LogInfo(true, "Leader: Successfully added task %s to node %s", task.TaskID, nodeAddress)
	return nil
}
