package main

import (
	"crypto/sha256"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

func (s *Server) handleCreate(localFilename, hyDFSfilename string) {
	// Hash the HyDFS filename to determine target nodes
	fileHash := HashToMbits(hyDFSfilename)
	ConsolePrintf("File hash for %s: %s\n", hyDFSfilename, fileHash.String())

	// Find target nodes (primary + replicas)
	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)

	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	// Read local file
	localPath := filepath.Join(s.LocalDirectory, filepath.Base(localFilename))
	fileData, err := os.ReadFile(localPath)
	if err != nil {
		ConsolePrintf("Error reading file %s: %v\n", localFilename, err)
		return
	}

	ConsolePrintf("Read file %s (%d bytes)\n", localFilename, len(fileData))

	// Create file metadata
	fileContentHash := fmt.Sprintf("%x", sha256.Sum256(fileData))
	creationTime := fmt.Sprintf("%d", time.Now().Unix())

	fileMetadata := FileMetadata{
		FileName:        hyDFSfilename,
		FileContentHash: fileContentHash,
		FileNameHash:    fileHash,
		CreationTime:    creationTime,
		Appends:         []AppendInfo{},
	}

	ConsolePrintf("Created file metadata: %+v\n", fileMetadata)

	// Send file to target nodes (W=1, R=1: wait for only 1 successful response)
	successChan := make(chan bool, len(targetMembers))
	doneChan := make(chan struct{})

	for _, targetMember := range targetMembers {
		go func(addr string) {
			ConsolePrintf("Sending file to %s | %s...\n", AddressToVMName[addr], convertToGRPCAddress(addr))
			if err := s.SendFileToNode(addr, fileData, fileMetadata); err != nil {
				ConsolePrintf("Error sending file to %s: %v\n", convertToGRPCAddress(addr), err)
				successChan <- false
			} else {
				successChan <- true
			}
		}(targetMember.Address)
	}

	go func() {
		successCount := 0
		for i := 0; i < len(targetMembers); i++ {
			if <-successChan {
				successCount++
			}
		}
		if successCount == 0 {
			ConsolePrintf("CREATE FAILED: all replicas failed for file %s\n", hyDFSfilename)
		} else {
			ConsolePrintf("File creation completed: %s -> %s (received %d/%d responses)\n", localFilename, hyDFSfilename, successCount, len(targetMembers))
		}
		close(doneChan)
	}()

	<-doneChan
}

func (s *Server) handleGet(hyDFSfilename, localFilename string) {
	// Hash the HyDFS filename to determine target nodes
	fileHash := HashToMbits(hyDFSfilename)

	// Find target nodes (primary + replicas)
	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)

	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	// Use gRPC GetFile similar to handleCreate's SendFileToNode
	downloadFromNode := func(address string) error {
		return s.ReceiveFileFromNode(address, hyDFSfilename, s.LocalDirectory, localFilename)
	}

	// Try primary first, then replicas sequentially. Each attempt has ReadTimeout.
	for _, m := range targetMembers {
		addr := m.Address
		done := make(chan error, 1)
		go func() { done <- downloadFromNode(addr) }()

		select {
		case err := <-done:
			if err == nil {
				return
			}
			ConsolePrintf("GET error from %s: %v\n", convertToGRPCAddress(addr), err)
			// try next replica
		case <-time.After(ReadTimeout):
			ConsolePrintf("GET timeout from %s after %s\n", convertToGRPCAddress(addr), ReadTimeout)
			// try next replica
		}
	}

	ConsolePrintf("GET FAILED: no node could serve %s\n", hyDFSfilename)
}

func (s *Server) handleAppend(localFilename, hyDFSfilename string) {
	fileHash := HashToMbits(hyDFSfilename)
	LogInfo(true, "File hash for %s: %s\n", hyDFSfilename, fileHash.String())

	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)
	if len(targetMembers) == 0 {
		ConsolePrintf("No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}
	localPath := filepath.Join(s.LocalDirectory, filepath.Base(localFilename))
	fileData, err := os.ReadFile(localPath)
	if err != nil {
		ConsolePrintf("Error reading file %s: %v\n", localFilename, err)
		return
	}

	LogInfo(true, "Read file %s (%d bytes)\n", localFilename, len(fileData))

	appendContentHash := fmt.Sprintf("%x", sha256.Sum256(fileData))
	clientTimestamp := fmt.Sprintf("%d", time.Now().Unix())

	appendInfo := AppendInfo{
		FileName:        hyDFSfilename,
		AppendHash:      appendContentHash,
		AppendID:        fmt.Sprintf("%s_%s_%s", hyDFSfilename, clientTimestamp, AddressToVMName[s.Addr]),
		ClientTimestamp: clientTimestamp,
		ClientID:        s.ID(),
		Size:            int64(len(fileData)),
	}

	LogInfo(true, "Created append metadata: %+v\n", appendInfo)

	successChan := make(chan bool, len(targetMembers))
	doneChan := make(chan struct{})
	for _, targetMember := range targetMembers {
		go func(addr string) {
			ConsolePrintf("Sending file to %s | %s...\n", AddressToVMName[addr], convertToGRPCAddress(addr))
			if err := s.SendAppendToNode(addr, fileData, appendInfo); err != nil {
				ConsolePrintf("Error sending file to %s: %v\n", convertToGRPCAddress(addr), err)
				successChan <- false
			} else {
				successChan <- true
			}
		}(targetMember.Address)
	}

	go func() {
		successCount := 0
		for i := 0; i < len(targetMembers); i++ {
			if <-successChan {
				successCount++
			}
		}
		if successCount == 0 {
			ConsolePrintf("APPEND FAILED: all replicas failed for file %s\n", hyDFSfilename)
		} else {
			ConsolePrintf("File Append completed: %s -> %s (received %d/%d responses)\n", localFilename, hyDFSfilename, successCount, len(targetMembers))
		}
		close(doneChan)
	}()

	<-doneChan
}

// handleMergeCommand processes the merge command from CLI
func (s *Server) handleMergeCommand(hyDFSfilename string) {
	// Hash the filename to get its hash value
	fileHash := HashToMbits(hyDFSfilename)
	ConsolePrintf("Started Merge for file for %s | Hash: %s\n", hyDFSfilename, fileHash.String())

	// Get current node's primary key range
	start, end := GetPrimaryKeyRange(s.Members.GetAll(), s.ID())
	if start == nil || end == nil {
		LogError(true, "[handleMergeCommand] Could not determine primary key range\n")
		return
	}

	// Check if file hash is within the current node's primary range
	fileHashPtr := &fileHash
	inRange := IsHashInRange(fileHashPtr, start, end)

	if inRange {
		LogInfo(true, "[handleMergeCommand] File %s is in current node's primary range, executing merge\n", hyDFSfilename)
		s.executeMerge(hyDFSfilename)
		ConsolePrintf("Merge completed for file:%s\n", hyDFSfilename)
	} else {
		LogInfo(true, "[handleMergeCommand] File %s is not in current node's primary range (range: %s - %s)\n",
			hyDFSfilename, start.String(), end.String())
		LogInfo(true, "[handleMergeCommand] Finding primary node for this file\n")

		// Find target nodes for the file hash
		targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)
		if len(targetMembers) == 0 {
			ConsolePrintf("[handleMergeCommand] No suitable nodes found for file %s\n", hyDFSfilename)
			return
		}

		// Primary node is the first target member
		primaryNode := targetMembers[0]
		LogInfo(true, "[handleMergeCommand] Primary node for file %s: %s (%s)\n",
			hyDFSfilename, primaryNode.ID(), primaryNode.Address)

		// Send RPC to primary node to execute merge
		resp, err := CallMerge(primaryNode.Address, hyDFSfilename, s)
		if err != nil {
			ConsolePrintf("[handleMergeCommand] Failed to send merge RPC to primary node %s: %v\n",
				primaryNode.ID(), err)
		} else if !resp.Success {
			ConsolePrintf("[handleMergeCommand] Merge operation failed on primary node %s: %s\n",
				primaryNode.ID(), resp.Message)
		} else {
			ConsolePrintf("[handleMergeCommand] Merge operation completed on primary node %s: %s\n",
				primaryNode.ID(), resp.Message)
		}
	}
}

func (s *Server) handleMultiAppend(hyDFSfilename string, vmNames []string, localFiles []string) {
	// Send multiappend RPC calls to VMs in parallel
	done := make(chan struct {
		vmName string
		err    error
	}, len(vmNames))

	for i := 0; i < len(vmNames); i++ {
		vmName := vmNames[i]
		localFile := localFiles[i]

		// Get VM address from NodeMap
		vmAddress, ok := NodeMap[vmName]
		if !ok {
			ConsolePrintf("Invalid VM name: %s (not found in NodeMap)\n", vmName)
			done <- struct {
				vmName string
				err    error
			}{vmName: vmName, err: fmt.Errorf("VM %s not found in NodeMap", vmName)}
			continue
		}

		// Call RPC in parallel using goroutine
		go func(addr, localFile, hyDFSFile string, vm string) {
			resp, err := CallMultiAppend(addr, hyDFSFile, localFile, s)
			if err != nil {
				ConsolePrintf("MultiAppend failed for VM %s (%s): %v\n", vm, addr, err)
				done <- struct {
					vmName string
					err    error
				}{vmName: vm, err: err}
			} else {
				ConsolePrintf("MultiAppend succeeded for VM %s (%s): %s\n", vm, addr, resp.Message)
				done <- struct {
					vmName string
					err    error
				}{vmName: vm, err: nil}
			}
		}(vmAddress, localFile, hyDFSfilename, vmName)
	}

	// Wait for all RPC calls to complete
	successCount := 0
	for i := 0; i < len(vmNames); i++ {
		result := <-done
		if result.err == nil {
			successCount++
		}
	}

	ConsolePrintf("MultiAppend completed: %d/%d successful\n", successCount, len(vmNames))
}

// handleLS lists all VM addresses and IDs where a file is stored, along with the file ID
func (s *Server) handleLS(hyDFSfilename string) {
	// Hash the filename to get the file ID
	fileHash := HashToMbits(hyDFSfilename)
	fileID := fileHash.String()

	ConsolePrintf("File: %s\n", hyDFSfilename)
	ConsolePrintf("FileID: %s\n", fileID)

	// Find all target nodes where the file is stored (primary + replicas)
	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)

	if len(targetMembers) == 0 {
		ConsolePrintf("No nodes found for file %s\n", hyDFSfilename)
		return
	}

	ConsolePrintf("Stored on node(s):\n")
	for _, member := range targetMembers {
		fileMetadataResponse, err := GetFileMetadata(member.Address, s, hyDFSfilename)
		// if error or metadata response does not contain filename, dont print
		if err != nil {
			continue
		}
		if fileMetadataResponse == nil || !fileMetadataResponse.Success || fileMetadataResponse.Metadata == nil || fileMetadataResponse.Metadata.Files == nil {
			continue
		}
		if _, exists := fileMetadataResponse.Metadata.Files[hyDFSfilename]; !exists {
			continue
		}
		vmName := AddressToVMName[member.Address]
		if vmName == "" {
			vmName = member.Address // Fallback to address if VM name not found
		}
		ConsolePrintf(" VM: %s | Address: %s | ID: %s | Hash: %s\n",
			vmName, member.Address, member.ID(), member.Hash.String())
	}
}

// handleListStore lists all files stored on this VM's HyDFS along with their fileIDs
func (s *Server) handleListStore() {
	// Get VM name for this process
	vmName := AddressToVMName[s.Addr]
	if vmName == "" {
		vmName = s.Addr // Fallback to address if VM name not found
	}

	// Get process/VM's ID on the ring
	vmID := s.ID()

	ConsolePrintf("VM: %s | Address: %s | ID:  %s | Hash: %s\n", vmName, s.Addr, vmID, s.Hash.String())

	// Get all files stored in this VM's metadata
	fileNames := s.Metadata.ListFiles()

	if len(fileNames) == 0 {
		ConsolePrintf("No files stored on this VM's HyDFS\n")
		return
	}

	ConsolePrintf("Files stored on this VM's HyDFS (%d file(s)):\n", len(fileNames))
	for i, filename := range fileNames {
		fileMeta, ok := s.Metadata.GetFile(filename)
		if !ok {
			continue
		}
		fileID := fileMeta.FileNameHash.String()
		ConsolePrintf("  [%d] File: %s | FileID: %s\n", i+1, filename, fileID)
	}
}

// handleGetFromReplica gets a file from a specific replica VM and stores it locally
func (s *Server) handleGetFromReplica(vmID, hyDFSfilename, localFilename string) {
	// Map VM ID to address using NodeMap
	vmAddress, ok := NodeMap[vmID]
	if !ok {
		ConsolePrintf("[getfromreplica] Invalid VM ID: %s (not found in NodeMap)\n", vmID)
		return
	}

	ConsolePrintf("[getfromreplica] Fetching file %s from replica %s (%s)\n", hyDFSfilename, vmID, vmAddress)

	// Use ReceiveFileFromNode to download from the specific VM address
	err := s.ReceiveFileFromNode(vmAddress, hyDFSfilename, s.LocalDirectory, localFilename)
	if err != nil {
		ConsolePrintf("[getfromreplica] Failed to get file %s from %s (%s): %v\n", hyDFSfilename, vmID, vmAddress, err)
		return
	}

	ConsolePrintf("[getfromreplica] Successfully retrieved file %s from %s (%s) and saved as %s\n", hyDFSfilename, vmID, vmAddress, localFilename)
}

func (s *Server) executeMerge(hyDFSfilename string) {
	startTime := time.Now()
	successors := GetSuccessors(s.Members.GetAll(), s.ID(), Config.ReplicationFactor)
	if len(successors) == 0 {
		LogError(true, "[handleMerge] No successors found\n")
		return
	}

	localMeta, ok := s.Metadata.GetFile(hyDFSfilename)
	if !ok {
		LogError(true, "[handleMerge] File %s not found locally, skipping\n", hyDFSfilename)
		return
	}

	localAppendIDs := make([]string, len(localMeta.Appends))
	for i, app := range localMeta.Appends {
		localAppendIDs[i] = app.AppendID
	}

	for _, successor := range successors {
		if s.ensureRemoteFileInSync(successor, hyDFSfilename, localMeta, localAppendIDs) {
			s.syncRemoteAppendOrder(successor, hyDFSfilename, localMeta, localAppendIDs)
		}
	}
	duration := time.Since(startTime)
	LogInfo(true, "Merge completed for node %s (filename=%s) in %s", s.ID(), hyDFSfilename, duration)

	LogInfo(true, "[handleMerge] Merge operation completed\n")
}

// findTargetNodes finds the primary node and replicas for a given file hash
func (s *Server) findTargetNodes(fileHash big.Int, replicationFactor int) []Member {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return []Member{}
	}
	fileHashPtr := new(big.Int).Set(&fileHash)

	// Find primary node (first node with hash >= file hash)
	primaryIndex := -1
	for i, member := range members {
		if member.Status != StatusAlive {
			continue
		}

		memberHashInt := member.Hash

		if memberHashInt.Cmp(fileHashPtr) >= 0 {
			primaryIndex = i
			break
		}
	}

	// If no node found with hash >= file hash, wrap around to first node
	if primaryIndex == -1 {
		for i, member := range members {
			if member.Status == StatusAlive {
				primaryIndex = i
				break
			}
		}
	}

	if primaryIndex == -1 {
		return []Member{}
	}

	// Get primary node and replicas
	var targetMembers []Member
	for i := 0; i < replicationFactor; i++ {
		index := (primaryIndex + i) % len(members)
		if members[index].Status == StatusAlive {
			targetMembers = append(targetMembers, members[index])
		}
	}

	return targetMembers
}

func (s *Server) ensureRemoteFileInSync(successor Member, filename string, localMeta FileMetadata, localAppendIDs []string) bool {
	succMeta, err := GetFileMetadata(successor.Address, s, filename)
	if err != nil {
		LogError(true, "[handleMerge] successor %s missing metadata for %s: %v\n", successor.Address, filename, err)
		return false
	}

	remoteMeta, exists := succMeta.Metadata.Files[filename]
	if !exists {
		LogInfo(true, "[handleMerge] Remote file %s missing on %s, triggering stabilization\n", filename, successor.ID())
		s.stabilizeRing(filename)
		return false
	}

	if !filesAndAppendsMatch(localMeta, remoteMeta) {
		LogInfo(true, "[handleMerge] Remote file %s out of sync on %s; stabilizing\n", filename, successor.ID())
		s.stabilizeRing(filename)

	}

	return true
}

func filesAndAppendsMatch(localMeta FileMetadata, remoteMeta FileMetadata) bool {
	if localMeta.FileContentHash != remoteMeta.FileContentHash {
		return false
	}

	if len(localMeta.Appends) != len(remoteMeta.Appends) {
		return false
	}

	localIDs := make(map[string]struct{}, len(localMeta.Appends))
	for _, app := range localMeta.Appends {
		localIDs[app.AppendID] = struct{}{}
	}

	for _, app := range remoteMeta.Appends {
		if _, exists := localIDs[app.AppendID]; !exists {
			return false
		}
	}

	return true
}

func (s *Server) syncRemoteAppendOrder(successor Member, filename string, localMeta FileMetadata, localAppendIDs []string) {
	succMeta, err := GetFileMetadata(successor.Address, s, filename)
	if err != nil {
		LogError(true, "[handleMerge] Failed to fetch metadata for order sync from %s: %v\n", successor.ID(), err)
		return
	}

	remoteMeta, exists := succMeta.Metadata.Files[filename]
	if !exists {
		LogError(true, "[handleMerge] Remote file %s missing while syncing order on %s\n", filename, successor.ID())
		return
	}

	if ordersMatch(localAppendIDs, remoteMeta.Appends) {
		LogInfo(true, "[handleMerge] Append order matches for file %s on successor %s\n", filename, successor.ID())
		return
	}

	LogInfo(true, "[handleMerge] Append order differs for file %s on successor %s, updating...\n", filename, successor.ID())
	LogInfo(true, "[handleMerge] Local order: %v\n", localAppendIDs)
	LogInfo(true, "[handleMerge] Remote order: %v\n", remoteMeta.Appends)

	resp, err := CallUpdateAppendOrder(successor.Address, filename, localMeta.Appends, s)
	if err != nil {
		LogError(true, "[handleMerge] Failed to update append order on %s: %v\n", successor.ID(), err)
		return
	}

	if !resp.Success {
		LogError(true, "[handleMerge] Update append order failed on %s: %s\n", successor.ID(), resp.Message)
		return
	}

	LogInfo(true, "[handleMerge] Successfully updated append order for file %s on %s\n", filename, successor.ID())
}

func ordersMatch(localAppendIDs []string, remoteAppends []AppendInfo) bool {
	if len(localAppendIDs) != len(remoteAppends) {
		return false
	}

	for i := 0; i < len(localAppendIDs); i++ {
		if localAppendIDs[i] != remoteAppends[i].AppendID {
			return false
		}
	}

	return true
}

func (s *Server) executeMergeForAllFiles() {
	start, end := GetPrimaryKeyRange(s.Members.GetAll(), s.ID())
	allFiles := GetFilesWithinRange(s.Metadata.Files, start, end)
	LogInfo(true, "[executeMergeForAllFiles] Executing Merge for all files\n")
	for _, name := range allFiles {
		s.executeMerge(name)

	}
}

// AppendOrCreateFromBuffer writes a WAL or log buffer directly to HyDFS.
// It first checks if the HyDFS file exists using GetFileMetadata (LS-style check).
// If the file does not exist, it creates it with the provided buffer contents.
// If it exists, it appends the buffer contents as a new append.
func (s *Server) AppendOrCreateFromBuffer(hyDFSfilename string, data []byte) {
	// If there is nothing to write, return immediately.
	if len(data) == 0 {
		return
	}

	// Determine target nodes based on HyDFS filename hash.
	fileHash := HashToMbits(hyDFSfilename)
	LogInfo(true, "HyDFS WAL: File hash for %s: %s", hyDFSfilename, fileHash.String())

	targetMembers := s.findTargetNodes(fileHash, Config.ReplicationFactor)
	if len(targetMembers) == 0 {
		ConsolePrintf("HyDFS WAL: No suitable nodes found for file %s\n", hyDFSfilename)
		return
	}

	// LS-style existence check via GetFileMetadata on target nodes.
	exists := false
	for _, member := range targetMembers {
		resp, err := GetFileMetadata(member.Address, s, hyDFSfilename)
		if err != nil {
			LogError(true, "HyDFS WAL: Error getting file metadata from %s: %v\n", member.Address, err)
			continue
		}
		if resp == nil || !resp.Success || resp.Metadata == nil || resp.Metadata.Files == nil {
			LogError(true, "HyDFS WAL: Failed to get file metadata from %s: %v\n", member.Address, resp)
			continue
		}
		if _, ok := resp.Metadata.Files[hyDFSfilename]; ok {
			exists = true
			break
		}
	}

	if !exists {
		// CREATE path: build FileMetadata and send full data as initial file contents.
		fileContentHash := fmt.Sprintf("%x", sha256.Sum256(data))
		creationTime := fmt.Sprintf("%d", time.Now().Unix())

		fileMetadata := FileMetadata{
			FileName:        hyDFSfilename,
			FileContentHash: fileContentHash,
			FileNameHash:    fileHash,
			CreationTime:    creationTime,
			Appends:         []AppendInfo{},
		}

		LogInfo(true, "HyDFS WAL: Creating new file %s with metadata %+v", hyDFSfilename, fileMetadata)

		successChan := make(chan bool, len(targetMembers))
		doneChan := make(chan struct{})

		for _, targetMember := range targetMembers {
			go func(addr string) {
				LogInfo(true, "HyDFS WAL: Sending initial WAL file to %s | %s...\n",
					AddressToVMName[addr], convertToGRPCAddress(addr))
				if err := s.SendFileToNode(addr, data, fileMetadata); err != nil {
					ConsolePrintf("HyDFS WAL: Error sending file to %s: %v\n", convertToGRPCAddress(addr), err)
					successChan <- false
				} else {
					successChan <- true
				}
			}(targetMember.Address)
		}

		go func() {
			successCount := 0
			for i := 0; i < len(targetMembers); i++ {
				if <-successChan {
					successCount++
				}
			}
			if successCount == 0 {
				ConsolePrintf("HyDFS WAL: CREATE FAILED: all replicas failed for file %s\n", hyDFSfilename)
			} else {
				ConsolePrintf("HyDFS WAL: File creation completed: %s (received %d/%d responses)\n",
					hyDFSfilename, successCount, len(targetMembers))
			}
			close(doneChan)
		}()

		<-doneChan
		return
	}

	// APPEND path: reuse append metadata logic, but operate on the in-memory buffer.
	appendContentHash := fmt.Sprintf("%x", sha256.Sum256(data))
	clientTimestamp := fmt.Sprintf("%d", time.Now().Unix())

	appendInfo := AppendInfo{
		FileName:        hyDFSfilename,
		AppendHash:      appendContentHash,
		AppendID:        fmt.Sprintf("%s_%s_%s", hyDFSfilename, clientTimestamp, AddressToVMName[s.Addr]),
		ClientTimestamp: clientTimestamp,
		ClientID:        s.ID(),
		Size:            int64(len(data)),
	}

	LogInfo(true, "HyDFS WAL: Created append metadata: %+v", appendInfo)

	successChan := make(chan bool, len(targetMembers))
	doneChan := make(chan struct{})
	for _, targetMember := range targetMembers {
		go func(addr string) {
			LogInfo(true, "HyDFS WAL: Sending append to %s | %s...\n",
				AddressToVMName[addr], convertToGRPCAddress(addr))
			if err := s.SendAppendToNode(addr, data, appendInfo); err != nil {
				ConsolePrintf("HyDFS WAL: Error sending append to %s: %v\n", convertToGRPCAddress(addr), err)
				successChan <- false
			} else {
				successChan <- true
			}
		}(targetMember.Address)
	}

	go func() {
		successCount := 0
		for i := 0; i < len(targetMembers); i++ {
			if <-successChan {
				successCount++
			}
		}
		if successCount == 0 {
			ConsolePrintf("HyDFS WAL: APPEND FAILED: all replicas failed for file %s\n", hyDFSfilename)
		} else {
			LogInfo(true, "HyDFS WAL: Append completed for file %s (received %d/%d responses)\n",
				hyDFSfilename, successCount, len(targetMembers))
		}
		close(doneChan)
	}()

	<-doneChan
}
