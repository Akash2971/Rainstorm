package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
)

// RainstormMessage represents a message sent over the Rainstorm TCP RPC connection
type RainstormMessage struct {
	Type    string          `json:"type"` // Message type (e.g., "TUPLE", "ACK")
	Payload json.RawMessage `json:"payload"`
}

// startRainstormListener starts a TCP listener on port+2000 that can access NodeManager
func (s *Server) startRainstormListener() {
	// Extract port from server address and add 2000
	host, portStr, err := net.SplitHostPort(s.Addr)
	if err != nil {
		LogError(true, "Failed to parse server address for Rainstorm listener: %v", err)
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		LogError(true, "Failed to parse port for Rainstorm listener: %v", err)
		return
	}

	rainstormPort := port + 2000
	rainstormAddr := fmt.Sprintf("%s:%d", host, rainstormPort)

	// Start TCP listener
	listener, err := net.Listen("tcp", rainstormAddr)
	if err != nil {
		LogError(true, "Failed to start Rainstorm listener on %s: %v", rainstormAddr, err)
		return
	}
	defer listener.Close()

	LogInfo(true, "Rainstorm TCP listener started on %s (can access NodeManager)", rainstormAddr)
	ConsolePrintf("Rainstorm TCP listener started on %s\n", rainstormAddr)

	// Accept connections and handle messages
	for {
		conn, err := listener.Accept()
		if err != nil {
			LogError(true, "Rainstorm listener accept error: %v", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go s.handleRainstormConnection(conn)
	}
}

// handleRainstormConnection handles a single TCP connection for Rainstorm messages
func (s *Server) handleRainstormConnection(conn net.Conn) {
	defer conn.Close()

	LogInfo(true, "Rainstorm: Accepted connection from %s", conn.RemoteAddr())

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	// TODO: Optimize : Try to connect only once rather than connecting everytime when a new msg comes.

	for {
		var msg RainstormMessage
		if err := decoder.Decode(&msg); err != nil {
			// Connection closed or error
			LogInfo(true, "Rainstorm: Connection closed from %s: %v", conn.RemoteAddr(), err)
			return
		}

		// Route message based on type
		response, err := s.routeRainstormMessage(&msg)
		if err != nil {
			LogError(true, "Rainstorm: Error routing message type %s: %v", msg.Type, err)
			// Send error response
			errorResp := map[string]interface{}{
				"success": false,
				"error":   err.Error(),
			}
			encoder.Encode(errorResp)
			continue
		}

		// Send response if any
		if response != nil {
			if err := encoder.Encode(response); err != nil {
				LogError(true, "Rainstorm: Failed to send response: %v", err)
				return
			}
		}
	}
}

// routeRainstormMessage routes incoming messages to appropriate handlers based on message type
func (s *Server) routeRainstormMessage(msg *RainstormMessage) (interface{}, error) {
	switch msg.Type {
	case "TUPLE":
		return s.handleTupleMessage(msg.Payload)
	case "ACK":
		return s.handleACKMessage(msg.Payload)
	default:
		return nil, fmt.Errorf("unknown message type: %s", msg.Type)
	}
}

// handleTupleMessage handles tuple messages
func (s *Server) handleTupleMessage(payload json.RawMessage) (interface{}, error) {
	var tuple Tuple
	if err := json.Unmarshal(payload, &tuple); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tuple: %w", err)
	}

	LogInfo(true, "Rainstorm: Received tuple ID=%s, TaskID=%s, SenderID=%s", tuple.ID, tuple.TaskID, tuple.SenderID)

	// Check if NodeManager is available
	if s.NodeManager == nil {
		return nil, fmt.Errorf("NodeManager is not available")
	}

	// Get task info for the target task (thread-safe)
	taskInfo, exists := s.NodeManager.GetTaskInfo(tuple.TaskID)
	if !exists {
		return nil, fmt.Errorf("task %s not found on this node", tuple.TaskID)
	}

	// Check if task is running
	s.NodeManager.mu.RLock()
	taskStatus := s.NodeManager.TaskStatus[tuple.TaskID]
	task := taskInfo.Task
	s.NodeManager.mu.RUnlock()

	if taskStatus != TaskRunning {
		return nil, fmt.Errorf("task %s is not running (status: %d)", tuple.TaskID, taskStatus)
	}

	// Update per-task received tuple metrics
	s.NodeManager.incrementTaskMetrics(tuple.TaskID, false, false, true)

	// Exactly-once semantics: Store in received_tuples (if not source)
	if taskInfo.ExactlyOnceMgr != nil {
		// Check for duplicate
		if !taskInfo.ExactlyOnceMgr.AddReceivedTuple(&tuple) {
			// Duplicate tuple - still send ACK but don't process
			LogInfo(true, "Rainstorm: Duplicate tuple ID=%s, sending ACK but skipping processing", tuple.ID)
			// Send ACK for duplicate (idempotent)
			s.sendACKForTuple(tuple.ID, tuple.SenderID, task)
			return map[string]interface{}{
				"success": true,
				"message": "Duplicate tuple - ACK sent",
			}, nil
		}
	}

	// Write tuple to the task's stdin pipe
	if taskInfo.Stdin == nil {
		return nil, fmt.Errorf("stdin pipe not available for task %s", tuple.TaskID)
	}

	// Marshal tuple to JSON and write to stdin
	tupleJSON, err := json.Marshal(tuple)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal tuple: %w", err)
	}

	// Write tuple as JSON line (newline-terminated)
	if _, err := taskInfo.Stdin.Write(append(tupleJSON, '\n')); err != nil {
		return nil, fmt.Errorf("failed to write tuple to task stdin: %w", err)
	}

	LogInfo(true, "Rainstorm: Routed tuple ID=%s to task %s (PID=%d)", tuple.ID, tuple.TaskID, taskInfo.PID)

	return map[string]interface{}{
		"success": true,
		"message": "Tuple routed to task",
		"task_id": tuple.TaskID,
	}, nil
}

// sendACKForTuple sends an ACK for a received tuple, given its IDs and task.
func (s *Server) sendACKForTuple(tupleID string, senderID string, task *Task) {
	// Find sender address from InputTasks
	senderAddr, exists := task.InputTasks[senderID]
	if !exists {
		LogError(true, "Rainstorm: Cannot send ACK - sender %s not found in InputTasks for task %s", senderID, task.TaskID)
		return
	}

	// Send ACK
	_, err := SendACK(senderAddr, tupleID, task.TaskID)
	if err != nil {
		LogError(true, "Rainstorm: Failed to send ACK for tuple ID=%s to sender %s at %s: %v", tupleID, senderID, senderAddr, err)
	} else {
		LogInfo(true, "Rainstorm: Sent ACK for tuple ID=%s to sender %s at %s", tupleID, senderID, senderAddr)
	}
}

// handleACKMessage handles ACK messages
func (s *Server) handleACKMessage(payload json.RawMessage) (interface{}, error) {
	var ackData map[string]string
	if err := json.Unmarshal(payload, &ackData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ACK: %w", err)
	}

	tupleID, ok1 := ackData["tuple_id"]
	taskID, ok2 := ackData["task_id"]

	if !ok1 || !ok2 {
		return nil, fmt.Errorf("invalid ACK format - missing tuple_id or task_id")
	}

	LogInfo(true, "Rainstorm: Received ACK for tuple ID=%s from task %s", tupleID, taskID)

	// Check if NodeManager is available
	if s.NodeManager == nil {
		return nil, fmt.Errorf("NodeManager is not available")
	}

	// Find the task that sent this tuple (search through all tasks)
	// The ACK's taskID is the downstream task that received it, but we need to find
	// which task on this node sent the tuple
	s.NodeManager.mu.RLock()
	var senderTaskInfo *TaskInfo
	for _, ti := range s.NodeManager.TaskInfos {
		if ti.Task != nil && ti.ExactlyOnceMgr != nil {
			if _, exists := ti.ExactlyOnceMgr.GetSentTuple(tupleID); exists {
				senderTaskInfo = ti
				break
			}
		}
	}
	s.NodeManager.mu.RUnlock()

	// If not found in NodeManager tasks, check if leader sent this tuple
	if senderTaskInfo == nil && s.Leader != nil {
		s.Leader.mu.RLock()
		if s.Leader.ExactlyOnceMgr != nil {
			if _, exists := s.Leader.ExactlyOnceMgr.GetSentTuple(tupleID); exists {
				// Leader sent this tuple, remove it from leader's sent tuples
				s.Leader.ExactlyOnceMgr.RemoveSentTuple(tupleID)
				s.Leader.mu.RUnlock()
				LogInfo(true, "Rainstorm: Removed tuple ID=%s from leader's sent_tuples after ACK", tupleID)
				return map[string]interface{}{
					"success": true,
					"message": "ACK processed (from leader)",
				}, nil
			}
		}
		s.Leader.mu.RUnlock()
	}

	if senderTaskInfo == nil {
		LogError(true, "Rainstorm: Could not find sender task for tuple ID=%s in sent_tuples", tupleID)
		return map[string]interface{}{
			"success": false,
			"message": "Sender task not found",
		}, nil
	}

	// Remove from sent_tuples if exactly-once is enabled
	senderTaskInfo.ExactlyOnceMgr.RemoveSentTuple(tupleID)

	return map[string]interface{}{
		"success": true,
		"message": "ACK processed",
	}, nil
}
