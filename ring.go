package main

import (
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GetNodeIndex returns the index of nodeID in a member slice
func GetNodeIndex(members []Member, nodeID string) int {
	for i, m := range members {
		if m.ID() == nodeID {
			return i
		}
	}
	return -1
}

// GetRingSuccessorIdx returns the index for the (idx+step)th node in the ring, wrapping
func GetRingSuccessorIdx(idx, step, n int) int {
	return (idx + step) % n
}

// GetRingPredecessorIdx returns the predecessor index for idx in the ring
func GetRingPredecessorIdx(idx, n int) int {
	return (idx - 1 + n) % n
}

// GetPrimaryKeyRange returns the predecessor and own hash for a node
func GetPrimaryKeyRange(members []Member, nodeID string) (start, end *big.Int) {
	N := len(members)
	myIdx := GetNodeIndex(members, nodeID)
	if N == 0 || myIdx == -1 {
		return nil, nil
	}
	// Find the nearest ALIVE predecessor and use its id
	predIdx := myIdx
	found := false
	for i := 0; i < N; i++ {
		predIdx = GetRingPredecessorIdx(predIdx, N)
		if members[predIdx].IsAlive() {
			found = true
			break
		}
	}

	if !found {
		return nil, nil
	}

	startHash := new(big.Int).Set(&members[predIdx].Hash)
	endHash := new(big.Int).Set(&members[myIdx].Hash)

	LogInfo(true, "Primary Key Range for nodeId:%s | start :%s | end :%s |\n", nodeID, startHash, endHash)
	return startHash, endHash
}

// GetAllKeyRange returns the overall key range that a node is responsible for based on replication factor
// This includes the node's primary range and all replica ranges (where this node is a replica for other nodes)
func GetAllKeyRange(members []Member, nodeID string, replicationFactor int) (start, end *big.Int) {
	N := len(members)
	myIdx := GetNodeIndex(members, nodeID)
	if N == 0 || myIdx == -1 {
		return nil, nil
	}

	// Get own hash (end of range)
	myHash := &members[myIdx].Hash
	end = new(big.Int).Set(myHash)

	// Find predecessor at index: myIdx - replicationFactor + 1
	predIdx := (myIdx - replicationFactor + N) % N
	predHash := &members[predIdx].Hash

	// Start = predecessor's hash + 1
	start = new(big.Int).Set(predHash)
	LogInfo(true, "Getting All Key Range for node:%s | start:%s | end:%s", nodeID, start, end)

	return start, end
}

// GetFilesWithinRange returns filenames whose hashes are in (start, end]
func GetFilesWithinRange(files map[string]FileMetadata, startRange, endRange *big.Int) []string {

	var result []string
	for name, meta := range files {
		fileHashPtr := &meta.FileNameHash
		cmpStart := fileHashPtr.Cmp(startRange)
		cmpEnd := fileHashPtr.Cmp(endRange)
		LogInfo(true, "Files: %s | FileHash: %s\n", name, fileHashPtr.String())

		if startRange.Cmp(endRange) < 0 {
			if cmpStart > 0 && cmpEnd <= 0 {
				result = append(result, name)
			}
		} else { // wrap around the ring
			if cmpStart > 0 || cmpEnd <= 0 {
				result = append(result, name)
			}
		}
	}
	LogInfo(false, "Files in range : %s - %s | %v \n", startRange, endRange, result)
	return result
}

// IsHashInRange checks if a hash value is within the range (start, end]
// Returns true if hash is in the range, false otherwise
func IsHashInRange(fileHash, startRange, endRange *big.Int) bool {
	cmpStart := fileHash.Cmp(startRange)
	cmpEnd := fileHash.Cmp(endRange)

	if startRange.Cmp(endRange) < 0 {
		// Normal case: no wrap-around
		return cmpStart > 0 && cmpEnd <= 0
	} else {
		// Wrap-around case
		return cmpStart > 0 || cmpEnd <= 0
	}
}

// GetSuccessors returns the next (n-1) alive successors after nodeID in sorted/liveness-filtered members
func GetSuccessors(members []Member, nodeID string, n int) []Member {
	myIdx := GetNodeIndex(members, nodeID)
	if myIdx == -1 || len(members) == 0 {
		return nil
	}
	succ := []Member{}
	count := 0
	for i := 1; count < n-1 && i < len(members); i++ {
		idx := GetRingSuccessorIdx(myIdx, i, len(members))
		if members[idx].IsAlive() {
			succ = append(succ, members[idx])
			count++
		}
	}
	return succ
}

// pushFilesToSuccessors pushes files and appends to successors that they don't have
func (s *Server) pushFilesToSuccessors(myFiles []string, successors []Member, start, end big.Int) {
	for _, successor := range successors {
		succMeta := s.fetchMetadataFromNode(successor, start, end)
		LogInfo(true, "Pushing files to successor: %s\n", successor.ID())

		for _, filename := range myFiles {
			LogInfo(true, "Checking file: %s", filename)
			localMeta, ok := s.Metadata.GetFile(filename)
			if !ok {
				LogError(true, "File not found locally: %s", filename)
				continue // file locally deleted
			}

			remoteMeta, exists := succMeta.Files[filename]

			// 1. Push missing or outdated file
			if !exists || localMeta.FileContentHash != remoteMeta.FileContentHash {
				ConsolePrintf("[pushFilesToSuccessors] File '%s' is missing or outdated on %s: transferring.\n", filename, successor.Address)
				data, err := os.ReadFile(filepath.Join(s.FileDirectory, filename))
				if err == nil {
					s.SendFileToNode(successor.Address, data, localMeta)
				} else {
					ConsolePrintf("[pushFilesToSuccessors] WARN: Could not find file data for transfer: %s\n", filename)
				}
			}

			// 2. Push missing appends (local appends not in remote)
			remoteAppends := map[string]bool{}
			if exists {
				for _, app := range remoteMeta.Appends {
					remoteAppends[app.AppendID] = true
				}
			}

			for _, app := range localMeta.Appends {
				if !remoteAppends[app.AppendID] {
					data, err := os.ReadFile(filepath.Join(s.FileDirectory, app.AppendID))
					if err == nil {
						s.SendAppendToNode(successor.Address, data, app)
					} else {
						ConsolePrintf("[pushFilesToSuccessors] WARN: No append data for %s on file %s, skipping copy.\n", app.AppendID, filename)
					}
				}
			}
		}
	}
}

// getFilesFromSuccessors gets files and appends from successors that we don't have locally
func (s *Server) getFilesFromSuccessors(successors []Member, start, end big.Int) {
	for _, successor := range successors {
		succMeta := s.fetchMetadataFromNode(successor, start, end)
		LogInfo(true, "Getting files from successor: %s", successor.ID())

		for remoteFilename, remoteMeta := range succMeta.Files {
			localMeta, existsLocally := s.Metadata.GetFile(remoteFilename)

			// Case 1: File doesn't exist locally, download it and its appends
			if !existsLocally {
				LogInfo(true, "[getFilesFromSuccessors] File '%s' exists on successor %s but not locally: downloading.\n", remoteFilename, successor.Address)

				// Download the base file (GetFile returns merged file: base + appends)
				err := s.ReceiveFileFromNode(successor.Address, remoteFilename, s.FileDirectory, remoteFilename)
				if err != nil {
					LogInfo(true, "[getFilesFromSuccessors] WARN: Failed to download file %s from %s: %v\n", remoteFilename, successor.Address, err)
					continue
				}

				// Add file metadata (without appends first, appends will be added as we download them)
				fileMetaToAdd := FileMetadata{
					FileName:        remoteMeta.FileName,
					FileContentHash: remoteMeta.FileContentHash,
					FileNameHash:    remoteMeta.FileNameHash,
					CreationTime:    remoteMeta.CreationTime,
					Appends:         []AppendInfo{},
				}
				s.Metadata.AddFile(fileMetaToAdd)

			}

			// Case 2: File exists locally, check for missing appends
			localAppends := make(map[string]bool)
			for _, app := range localMeta.Appends {
				localAppends[app.AppendID] = true
			}

			// Download missing appends from successor
			for _, remoteAppend := range remoteMeta.Appends {
				if !localAppends[remoteAppend.AppendID] {
					ConsolePrintf("[getFilesFromSuccessors] Append '%s' for file '%s' exists on successor %s but not locally: downloading.\n", remoteAppend.AppendID, remoteFilename, successor.Address)
					err := s.ReceiveFileFromNode(successor.Address, remoteAppend.AppendID, s.FileDirectory, remoteAppend.AppendID)
					if err != nil {
						ConsolePrintf("[getFilesFromSuccessors] WARN: Failed to download append %s from %s: %v\n", remoteAppend.AppendID, successor.Address, err)
					} else {
						// Add append to metadata after successful download
						s.Metadata.AppendToFile(remoteFilename, remoteAppend)
						ConsolePrintf("[getFilesFromSuccessors] Successfully downloaded append %s for file %s from %s\n", remoteAppend.AppendID, remoteFilename, successor.Address)
					}
				}
			}
		}
	}
}

// stabilizeRing coordinates the stabilization process by pushing and getting files
func (s *Server) stabilizeRing(filename string) {
	startTime := time.Now()

	start, end := GetPrimaryKeyRange(s.Members.GetAll(), s.ID())
	if start == nil || end == nil {
		return
	}

	allFiles := GetFilesWithinRange(s.Metadata.Files, start, end)

	var myFiles []string
	if filename != "" {
		// Check if the provided filename is in the list
		for _, f := range allFiles {
			if f == filename {
				// If filename is in the list, use only that filename
				myFiles = []string{filename}
				break
			}
		}
		// If filename not found in list, use the whole list
		if len(myFiles) == 0 {
			myFiles = allFiles
		}
	} else {
		// No filename provided, use the whole list
		myFiles = allFiles
	}

	successors := GetSuccessors(s.Members.GetAll(), s.ID(), Config.ReplicationFactor)
	if len(successors) == 0 {
		return
	}

	// Push files/appends to successors
	s.pushFilesToSuccessors(myFiles, successors, *start, *end)

	// Get files/appends from successors
	s.getFilesFromSuccessors(successors, *start, *end)
	duration := time.Since(startTime)
	LogInfo(true, "stabilizeRing completed for node %s (filename=%s) in %s", s.ID(), filename, duration)
}

func (s *Server) handleReplicationWindowChange(nodeID string) {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return
	}
	changedIdx := GetNodeIndex(members, nodeID)
	myIdx := GetNodeIndex(members, s.ID())
	if changedIdx == -1 || myIdx == -1 {
		ConsolePrintf("Could not find node in ring for stabilization check\n")
		return
	}
	for i := 1; i < Config.ReplicationFactor; i++ {
		if GetRingSuccessorIdx(myIdx, i, len(members)) == changedIdx || GetRingPredecessorIdx(myIdx, len(members)) == changedIdx {
			startTime := time.Now()

			ConsolePrintf("Triggering stabilization because of %s | %s \n", AddressToVMName[strings.Split(nodeID, "-")[0]], nodeID)
			s.stabilizeRing("")
			duration := time.Since(startTime)
			ConsolePrintf("Stabilization completed for node %s in time %s\n", nodeID, duration)
			return
		}
	}
	LogInfo(true, "Node %s is not in my replication set; no stabilization needed\n", nodeID)
}

// Background process which works as a garbage collector --> If file is outside my range, delete it
func (s *Server) deleteFilesOutsideMyRange() {
	members := s.Members.GetAll()
	if len(members) == 0 {
		return
	}

	// Get the overall key range this node is responsible for
	startRange, endRange := GetAllKeyRange(members, s.ID(), Config.ReplicationFactor)
	if startRange == nil || endRange == nil {
		ConsolePrintf("[deleteFilesOutsideMyRange] Could not determine key range\n")
		return
	}

	fileNames := s.Metadata.ListFiles()
	deletedCount := 0

	for _, filename := range fileNames {
		fileMeta, ok := s.Metadata.GetFile(filename)
		if !ok {
			continue
		}

		fileHash := &fileMeta.FileNameHash
		if !IsHashInRange(fileHash, startRange, endRange) {
			// File is outside our range - delete it
			ConsolePrintf("Deleting file outside range: %s (hash: %s)\n", filename, fileHash.String())

			// Delete the main file from filesystem
			filePath := filepath.Join(s.FileDirectory, filename)
			if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
				LogError(true, "Failed to delete file %s: %v", filePath, err)
			}

			// Delete all append files from filesystem
			for _, append := range fileMeta.Appends {
				appendPath := filepath.Join(s.FileDirectory, append.AppendID)
				if err := os.Remove(appendPath); err != nil && !os.IsNotExist(err) {
					LogError(true, "Failed to delete append file %s: %v", appendPath, err)
				}
			}

			// Delete from metadata (thread-safe)
			if s.Metadata.DeleteFile(filename) {
				deletedCount++
			}
		}
	}

	if deletedCount > 0 {
		ConsolePrintf("Deleted %d file(s) outside allowed range\n", deletedCount)
	}
}
