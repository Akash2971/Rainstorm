package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// retry interval for resending unacked tuples
const exactlyOnceRetryInterval = 2 * time.Second

// ReceivedTuple tracks minimal metadata for a received tuple (no full payload),
// identified only by its ID and sender.
type ReceivedTuple struct {
	TupleID         string
	SenderID        string
	ReceivedAt      time.Time
	Processed       bool // True when all output tuples have been sent
	ExpectedOutputs int  // Number of output tuples expected (from command process)
	OutputCount     int  // Number of output tuples actually generated and sent
}

// SentTuple tracks a tuple that has been sent downstream
type SentTuple struct {
	Tuple       *Tuple
	SentAt      time.Time
	OriginalID  string // The ID of the received tuple that generated this output
	Destination string // Destination task address
	// NOTE: No per-tuple stop control here; lifecycle is tied to the owning ExactlyOnceManager.
}

// ExactlyOnceManager manages exactly-once semantics for a task
// Source tasks (stage 1) don't have receivedTuples
// Final stage tasks don't have sentTuples
type ExactlyOnceManager struct {
	receivedTuples map[string]*ReceivedTuple // Map of tuple ID to received tuple
	sentTuples     map[string]*SentTuple     // Map of tuple ID to sent tuple
	mu             sync.RWMutex              // Mutex for thread-safe access
	isSource       bool                      // True if this is a source task (no received tuples)
	isFinalStage   bool                      // True if this is the final stage (no sent tuples)
	// walFileName is the HyDFS WAL file name for this task (e.g., "wal-<taskID>.log").
	walFileName string
	// writeBuffer keeps JSON-encoded log bytes in memory before (or as) they are
	// written to the WAL file. This can later be used for batched durable writes
	// or for driving batched ACK decisions.
	writeBuffer []byte
	pendingAcks []string // List of tuple IDs for which ACK should be sent to the sender
	// nm and task are used by the background flush loop to send ACKs upstream
	// via the parent Server once WAL records are durable.
	nm *NodeManager
	// task is the owning task, used primarily to route ACKs.
	task *Task
	// stopCh is closed when this manager should stop all background goroutines
	// (retry loop, ACK flush loop). It is created in NewExactlyOnceManager and
	// closed exactly once via Stop().
	stopCh  chan struct{}
	stopped bool
}

// NewExactlyOnceManager creates a new exactly-once manager. The NodeManager and
// task parameters are optional and are only needed for non-source tasks that
// need to send ACKs upstream after WAL records are durably flushed.
func NewExactlyOnceManager(isSource bool, isFinalStage bool, nm *NodeManager, task *Task) *ExactlyOnceManager {
	walName := ""
	if task != nil {
		walName = "wal-" + task.TaskID + ".log"
	}

	eom := &ExactlyOnceManager{
		receivedTuples: make(map[string]*ReceivedTuple),
		sentTuples:     make(map[string]*SentTuple),
		isSource:       isSource,
		isFinalStage:   isFinalStage,
		walFileName:    walName,
		writeBuffer:    make([]byte, 0),
		pendingAcks:    make([]string, 0),
		nm:             nm,
		task:           task,
		stopCh:         make(chan struct{}),
	}

	// Best-effort restoration of state from existing logs. This is safe to call
	// even when logs are empty or partially written; errors are logged and
	// ignored so that the task can still start.
	eom.restoreFromLogs()

	// Start a background retry loop for resending unacked output tuples.
	// This only runs for non-final stages (since final stage tasks don't send).
	if !eom.isFinalStage {
		eom.startRetryLoop()
	}
	// Start background WAL flush + ACK loop for tasks that receive tuples.
	if !eom.isSource && eom.nm != nil && eom.task != nil {
		eom.startAckFlushLoop()
	}

	return eom
}

// Stop cleanly stops all background goroutines owned by this ExactlyOnceManager
// (retry loop and ACK flush loop). It is safe to call multiple times.
func (eom *ExactlyOnceManager) Stop() {
	eom.mu.Lock()
	defer eom.mu.Unlock()
	if eom.stopped {
		return
	}
	eom.stopped = true
	// Closing stopCh will unblock any select statements waiting on it.
	close(eom.stopCh)
}

// AddReceivedTuple adds a tuple to the received tuples map
// Returns true if the tuple was added (not a duplicate), false if duplicate
func (eom *ExactlyOnceManager) AddReceivedTuple(tuple *Tuple) bool {
	if eom.isSource {
		// Source tasks don't track received tuples
		return true
	}

	eom.mu.Lock()
	defer eom.mu.Unlock()

	// Check for duplicate
	if _, exists := eom.receivedTuples[tuple.ID]; exists {
		LogInfo(true, "ExactlyOnce: Duplicate tuple received: ID=%s", tuple.ID)
		return false
	}

	// Add to received tuples
	rt := &ReceivedTuple{
		TupleID:         tuple.ID,
		SenderID:        tuple.SenderID,
		ReceivedAt:      time.Now(),
		Processed:       false,
		ExpectedOutputs: 0, // Will be set when command process reports output count
		OutputCount:     0,
	}
	eom.receivedTuples[tuple.ID] = rt

	LogInfo(true, "ExactlyOnce: Added received tuple ID=%s (total received: %d)", tuple.ID, len(eom.receivedTuples))
	return true
}

// SetExpectedOutputCount sets the expected number of output tuples for a received tuple
// Returns true if all outputs have already been sent (and marks as processed)
func (eom *ExactlyOnceManager) SetExpectedOutputCount(tupleID string, count int) bool {
	if eom.isSource {
		return false
	}

	eom.mu.Lock()
	defer eom.mu.Unlock()

	if receivedTuple, exists := eom.receivedTuples[tupleID]; exists {
		receivedTuple.ExpectedOutputs = count
		LogInfo(true, "ExactlyOnce: Set expected output count for tuple ID=%s to %d (current output count: %d)",
			tupleID, count, receivedTuple.OutputCount)

		// Check if all outputs have already been sent
		if count > 0 && receivedTuple.OutputCount >= count {
			// this block is never reached - but keeping it for now
			receivedTuple.Processed = true
			LogInfo(true, "ExactlyOnce: Marked received tuple ID=%s as processed (all %d outputs already sent)",
				tupleID, receivedTuple.OutputCount)
			return true
		} else if count == 0 {
			// for filter tasks, no outputs are expected
			// No outputs expected - mark as processed immediately
			receivedTuple.Processed = true
			eom.logRecord("REC", tupleID, receivedTuple)
			eom.pendingAcks = append(eom.pendingAcks, tupleID)
			LogInfo(true, "ExactlyOnce: Marked received tuple ID=%s as processed (0 outputs expected)", tupleID)
			return true
		}
		return false
	} else {
		LogError(true, "ExactlyOnce: Cannot set expected output count for tuple ID=%s - not found", tupleID)
		return false
	}
}

// IncrementOutputCount increments the output count for a received tuple
// Returns true if all expected outputs have been sent (and marks as processed)
func (eom *ExactlyOnceManager) IncrementOutputCount(tupleID string) bool {
	if eom.isSource {
		return false
	}

	eom.mu.Lock()
	defer eom.mu.Unlock()

	if receivedTuple, exists := eom.receivedTuples[tupleID]; exists {
		receivedTuple.OutputCount++
		LogInfo(true, "ExactlyOnce: Incremented output count for tuple ID=%s to %d/%d",
			tupleID, receivedTuple.OutputCount, receivedTuple.ExpectedOutputs)

		// Check if all outputs have been sent
		if receivedTuple.ExpectedOutputs > 0 && receivedTuple.OutputCount >= receivedTuple.ExpectedOutputs {
			receivedTuple.Processed = true
			// Write a REC record to the unified exactly-once log (inputLog).
			eom.logRecord("REC", tupleID, receivedTuple)
			eom.pendingAcks = append(eom.pendingAcks, tupleID)
			LogInfo(true, "ExactlyOnce: Marked received tuple ID=%s as processed (all %d outputs sent)",
				tupleID, receivedTuple.OutputCount)
			return true
		}
		return false
	} else {
		LogError(true, "ExactlyOnce: Cannot increment output count for tuple ID=%s - not found", tupleID)
		return false
	}
}

// AddSentTuple adds a tuple to the sent tuples map
// originalID is the ID of the received tuple that generated this output
func (eom *ExactlyOnceManager) AddSentTuple(tuple *Tuple, originalID string, destination string) {
	if eom.isFinalStage {
		// Final stage tasks don't track sent tuples (they write to file)
		return
	}

	eom.mu.Lock()
	defer eom.mu.Unlock()

	st := &SentTuple{
		Tuple:       tuple,
		SentAt:      time.Now(),
		OriginalID:  originalID,
		Destination: destination,
	}
	eom.sentTuples[tuple.ID] = st

	// Write a SENT record to the logFile
	eom.logRecord("SENT", tuple.ID, st)

	LogInfo(true, "ExactlyOnce: Added sent tuple ID=%s (original: %s, destination: %s, total sent: %d)",
		tuple.ID, originalID, destination, len(eom.sentTuples))
}

// RemoveSentTuple removes a tuple from the sent tuples map (after receiving ACK)
func (eom *ExactlyOnceManager) RemoveSentTuple(tupleID string) {
	if eom.isFinalStage {
		return
	}

	eom.mu.Lock()
	defer eom.mu.Unlock()

	if sentTuple, exists := eom.sentTuples[tupleID]; exists {
		LogInfo(true, "ExactlyOnce: Removing sent tuple ID=%s (original: %s) after ACK", tupleID, sentTuple.OriginalID)
		delete(eom.sentTuples, tupleID)

		// Write an ACK record to the log
		eom.logRecord("ACK", tupleID, nil)
	} else {
		LogError(true, "ExactlyOnce: Cannot remove sent tuple ID=%s - not found", tupleID)
	}
}

// GetSentTuple returns a sent tuple by ID
func (eom *ExactlyOnceManager) GetSentTuple(tupleID string) (*SentTuple, bool) {
	if eom.isFinalStage {
		return nil, false
	}

	eom.mu.RLock()
	defer eom.mu.RUnlock()

	tuple, exists := eom.sentTuples[tupleID]
	return tuple, exists
}

// restoreFromLogs reconstructs receivedTuples and sentTuples from existing
// logs. It first tries to fetch the unified WAL for this task from HyDFS
// (using the same "wal-<taskID>.log" naming that AppendOrCreateFromBuffer
// uses), and falls back to any local log file if HyDFS is unavailable.
// The log is assumed to contain JSON lines written via logRecord, possibly in
// a single unified log (inputLog) or in legacy split logs (input/output).
func (eom *ExactlyOnceManager) restoreFromLogs() {
	// Source tasks don't maintain a WAL.
	if eom.isSource {
		return
	}

	var logReader *os.File

	// 1. Best-effort: check for WAL in HyDFS via metadata, then fetch it.
	if eom.nm != nil && eom.nm.server != nil && eom.walFileName != "" {
		walName := eom.walFileName

		// First, use LS-style metadata checks (GetFileMetadata on target nodes)
		// to see if this WAL file exists anywhere. If it doesn't, skip restore.
		fileHash := HashToMbits(walName)
		targetMembers := eom.nm.server.findTargetNodes(fileHash, Config.ReplicationFactor)

		exists := false
		for _, member := range targetMembers {
			resp, err := GetFileMetadata(member.Address, eom.nm.server, walName)
			if err != nil {
				continue
			}
			if resp == nil || !resp.Success || resp.Metadata == nil || resp.Metadata.Files == nil {
				continue
			}
			if _, ok := resp.Metadata.Files[walName]; ok {
				exists = true
				break
			}
		}

		if exists {
			// Ask HyDFS to fetch the WAL file for this task from the
			// appropriate replicas onto the local node. This reuses the
			// existing get path (handleGet / ReceiveFileFromNode).
			eom.nm.server.handleGet(walName, walName)

			localDir := eom.nm.server.LocalDirectory
			if localDir == "" {
				localDir = "."
			}
			localPath := filepath.Join(localDir, walName)

			f, err := os.Open(localPath)
			if err == nil {
				logReader = f
				defer f.Close()
			}
		}
	}

	if logReader == nil {
		return
	}

	if _, err := logReader.Seek(0, io.SeekStart); err != nil {
		return
	}

	// Restore tuples and state from the chosen log. This log may contain
	// REC (received), SENT (sent downstream), and ACK (acknowledged) records.
	scanner := bufio.NewScanner(logReader)
	for scanner.Scan() {
		line := scanner.Bytes()
		// Skip pure newline / empty separator lines that may have been injected
		// when files were merged via HyDFS GET.
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var rec struct {
			Type    string          `json:"type"`
			TupleID string          `json:"tuple_id"`
			Payload json.RawMessage `json:"payload,omitempty"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			LogError(true, "ExactlyOnce: Failed to unmarshal input.log record: %v", err)
			continue
		}

		switch rec.Type {
		case "REC":
			var rt ReceivedTuple
			if err := json.Unmarshal(rec.Payload, &rt); err != nil {
				LogError(true, "ExactlyOnce: Failed to unmarshal REC payload for tuple %s: %v", rec.TupleID, err)
				continue
			}
			// Store by tuple ID; overwrite if duplicate REC appears later.
			eom.receivedTuples[rec.TupleID] = &rt
		case "SENT":
			// SENT may also appear in the unified log in newer versions.
			var st SentTuple
			if err := json.Unmarshal(rec.Payload, &st); err != nil {
				LogError(true, "ExactlyOnce: Failed to unmarshal SENT payload for tuple %s: %v", rec.TupleID, err)
				continue
			}
			eom.sentTuples[rec.TupleID] = &st
		case "ACK":
			// Drop from sentTuples on ACK.
			delete(eom.sentTuples, rec.TupleID)
		}
	}
	if err := scanner.Err(); err != nil {
		LogError(true, "ExactlyOnce: Error scanning input.log: %v", err)
	}

	// Only log if we actually restored something from the log.
	if len(eom.receivedTuples) > 0 || len(eom.sentTuples) > 0 {
		LogInfo(true, "ExactlyOnce: Restored %d received tuples and %d pending sent tuples from log",
			len(eom.receivedTuples), len(eom.sentTuples))
	}
}

// mergeStateFromLocalWal merges REC/SENT/ACK records from a local WAL file
// into this ExactlyOnceManager's in-memory maps. It does not change the
// walFileName or perform any HyDFS operations; callers are responsible for
// having already fetched the WAL locally.
func (eom *ExactlyOnceManager) mergeStateFromLocalWal(localPath string) {
	if eom == nil || localPath == "" {
		return
	}

	f, err := os.Open(localPath)
	if err != nil {
		LogError(true, "ExactlyOnce: Failed to open WAL for merge (%s): %v", localPath, err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var rec struct {
			Type    string          `json:"type"`
			TupleID string          `json:"tuple_id"`
			Payload json.RawMessage `json:"payload,omitempty"`
		}
		if err := json.Unmarshal(line, &rec); err != nil {
			LogError(true, "ExactlyOnce: Failed to unmarshal WAL merge record: %v", err)
			continue
		}

		eom.mu.Lock()
		switch rec.Type {
		case "REC":
			var rt ReceivedTuple
			if err := json.Unmarshal(rec.Payload, &rt); err != nil {
				eom.mu.Unlock()
				LogError(true, "ExactlyOnce: Failed to unmarshal REC payload during merge for tuple %s: %v", rec.TupleID, err)
				continue
			}
			eom.receivedTuples[rec.TupleID] = &rt
		case "SENT":
			var st SentTuple
			if err := json.Unmarshal(rec.Payload, &st); err != nil {
				eom.mu.Unlock()
				LogError(true, "ExactlyOnce: Failed to unmarshal SENT payload during merge for tuple %s: %v", rec.TupleID, err)
				continue
			}
			eom.sentTuples[rec.TupleID] = &st
		case "ACK":
			delete(eom.sentTuples, rec.TupleID)
		}
		eom.mu.Unlock()
	}

	if err := scanner.Err(); err != nil {
		LogError(true, "ExactlyOnce: Error scanning WAL during merge (%s): %v", localPath, err)
	}
}

// logRecord writes a single JSON log line with the given type, tuple ID, and optional payload.
// The payload should be a serializable struct (e.g., *ReceivedTuple or *SentTuple) or nil.
func (eom *ExactlyOnceManager) logRecord(recType string, tupleID string, payload interface{}) {
	rec := map[string]interface{}{
		"type":     recType,
		"tuple_id": tupleID,
	}
	if payload != nil {
		rec["payload"] = payload
	}

	data, err := json.Marshal(rec)
	if err != nil {
		LogError(true, "ExactlyOnce: Failed to marshal %s log record (ID=%s): %v", recType, tupleID, err)
		return
	}

	// Prepare the actual log line (JSON + newline) and place it into the in-memory
	// write buffer. The buffer will be written and fsynced in batches when
	// FlushAndGetPendingAcks is invoked.
	line := append(data, '\n')
	eom.writeBuffer = append(eom.writeBuffer, line...)
}

// startAckFlushLoop starts a background goroutine that, once per second,
// flushes any buffered WAL records to disk and then sends ACKs for all
// fully-processed tuples via ackCallback.
func (eom *ExactlyOnceManager) startAckFlushLoop() {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Snapshot buffer and pending ACKs under lock.
				eom.mu.Lock()
				if len(eom.writeBuffer) == 0 && len(eom.pendingAcks) == 0 {
					eom.mu.Unlock()
					continue
				}

				bufCopy := append([]byte(nil), eom.writeBuffer...)
				ids := append([]string(nil), eom.pendingAcks...)

				eom.writeBuffer = eom.writeBuffer[:0]
				eom.pendingAcks = eom.pendingAcks[:0]
				eom.mu.Unlock()

				// Write WAL buffer directly to HyDFS if possible.
				if len(bufCopy) > 0 && eom.nm != nil && eom.nm.server != nil && eom.walFileName != "" {
					eom.nm.server.AppendOrCreateFromBuffer(eom.walFileName, bufCopy)
				}

				// Send ACKs outside the lock.
				if eom.nm != nil && eom.nm.server != nil && eom.task != nil {
					for _, id := range ids {
						// Look up the received tuple metadata to recover sender information.
						eom.mu.RLock()
						rt, ok := eom.receivedTuples[id]
						eom.mu.RUnlock()
						if !ok || rt == nil || rt.SenderID == "" {
							continue
						}
						// Send each ACK in its own goroutine so slow network I/O does not
						// block the flush loop.
						go eom.nm.server.sendACKForTuple(rt.TupleID, rt.SenderID, eom.task)
					}
				}
			case <-eom.stopCh:
				// Manager has been stopped; exit the loop.
				LogInfo(true, "ExactlyOnce: Stopping ACK flush loop")
				return
			}
		}
	}()
}

// RetryPendingSends iterates over sentTuples and retries any tuple whose last
// send time is older than retryInterval. The actual send logic is provided by
// the caller via the send callback. This method is safe to call concurrently
// with other ExactlyOnceManager operations.
// TODO: i see we are passing function in param. make sure how it is being used
func (eom *ExactlyOnceManager) RetryPendingSends(retryInterval time.Duration, send func(*SentTuple) error) {
	if eom.isFinalStage || send == nil {
		return
	}

	eom.mu.Lock()
	now := time.Now()
	var toRetry []*SentTuple
	for _, st := range eom.sentTuples {
		// If we haven't waited long enough since last send, skip for now.
		if now.Sub(st.SentAt) < retryInterval {
			continue
		}
		// Update the last sent time so we don't immediately retry again.
		st.SentAt = now
		toRetry = append(toRetry, st)
	}
	eom.mu.Unlock()

	for _, st := range toRetry {
		if err := send(st); err != nil {
			LogError(true, "ExactlyOnce: Retry send failed for tuple ID=%s to %s: %v", st.Tuple.ID, st.Destination, err)
		} else {
			LogInfo(false, "ExactlyOnce: Retried send for tuple ID=%s to %s", st.Tuple.ID, st.Destination)
		}
	}
}

// startRetryLoop starts a background goroutine that periodically retries
// sending any unacked tuples in the sentTuples map.
func (eom *ExactlyOnceManager) startRetryLoop() {
	go func() {
		ticker := time.NewTicker(exactlyOnceRetryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				eom.RetryPendingSends(exactlyOnceRetryInterval, func(st *SentTuple) error {
					if st == nil || st.Tuple == nil || st.Destination == "" {
						return nil
					}

					// After WAL merges during autoscale scale-down, some SentTuples may have
					// originated from a killed task. Ensure that any retried sends use the
					// *current* owning task as the sender so that downstream ACKs route
					// back to the alive task instead of the dead one.
					if eom.task != nil {
						st.Tuple.SenderID = eom.task.TaskID
					}

					// Default destination is the original one captured when the tuple was first sent.
					destAddr := st.Destination

					// When autoscale is enabled, redirect retries using the *current* topology.
					// Source (leader) tasks rely on the leader's dynamic view of stage-1 tasks.
					// Non-source tasks use their NodeManager's updated OutputTargets after topology updates.
					if eom.isSource {
						if globalServer != nil && globalServer.Leader != nil {
							l := globalServer.Leader
							l.mu.RLock()
							job := l.JobSpec
							l.mu.RUnlock()

							if job != nil && job.Autoscale {
								if sel := selectTaskByKey(st.Tuple.Key, nil); sel != nil {
									st.Tuple.TaskID = sel.TaskID
									destAddr = sel.Address
								}
							}
						}
					} else if eom.nm != nil && eom.task != nil && eom.nm.currentJobSpec != nil && eom.nm.currentJobSpec.Autoscale {
						// Non-source tasks: use NodeManager's downstream selection based on current OutputTargets.
						selectedTaskID, selectedAddr := eom.nm.selectDownstreamTask(eom.task, st.Tuple.Key)
						if selectedTaskID != "" && selectedAddr != "" {
							st.Tuple.TaskID = selectedTaskID
							destAddr = selectedAddr
						}
					}

					// Pass eom as exactlyOnceMgr so retries remain tracked in sentTuples (SENT entries
					// may be duplicated in the WAL, but the latest record wins on restore).
					if _, err := SendTuple(destAddr, st.Tuple, eom, st.OriginalID); err != nil {
						LogError(true, "ExactlyOnce: Failed to retry send for tuple ID=%s to %s: %v",
							st.Tuple.ID, destAddr, err)
						return err
					}
					LogInfo(true, "ExactlyOnce: Retried send for tuple ID=%s to %s", st.Tuple.ID, destAddr)
					return nil
				})
			case <-eom.stopCh:
				// Manager has been stopped; exit the loop.
				return
			}
		}
	}()
}

// GetStats returns statistics about the exactly-once manager
func (eom *ExactlyOnceManager) GetStats() (receivedCount int, sentCount int, unprocessedCount int) {
	eom.mu.RLock()
	defer eom.mu.RUnlock()

	receivedCount = len(eom.receivedTuples)
	sentCount = len(eom.sentTuples)

	unprocessedCount = 0
	for _, rt := range eom.receivedTuples {
		if !rt.Processed {
			unprocessedCount++
		}
	}

	return receivedCount, sentCount, unprocessedCount
}
