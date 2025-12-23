package main

import (
	"encoding/json"
	"errors"
	"math/big"
	"sync"
)

// Metadata is now concurrency safe with per-file locks
type Metadata struct {
	mu    sync.RWMutex // map-level protection
	Files map[string]FileMetadata
	locks map[string]*sync.RWMutex // per-file locks
}

// FileMetadata represents information about a file stored in HyDFS.
type FileMetadata struct {
	FileName        string       `json:"file_name"`
	FileContentHash string       `json:"file_content_hash"`
	FileNameHash    big.Int      `json:"file_name_hash"`
	CreationTime    string       `json:"creation_time"`
	Appends         []AppendInfo `json:"appends"`
}

// MarshalJSON implements custom JSON marshaling for FileMetadata
func (fm FileMetadata) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		FileName        string       `json:"file_name"`
		FileContentHash string       `json:"file_content_hash"`
		FileNameHash    string       `json:"file_name_hash"`
		CreationTime    string       `json:"creation_time"`
		Appends         []AppendInfo `json:"appends"`
	}{
		FileName:        fm.FileName,
		FileContentHash: fm.FileContentHash,
		FileNameHash:    fm.FileNameHash.String(),
		CreationTime:    fm.CreationTime,
		Appends:         fm.Appends,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for FileMetadata
func (fm *FileMetadata) UnmarshalJSON(data []byte) error {
	aux := &struct {
		FileName        string       `json:"file_name"`
		FileContentHash string       `json:"file_content_hash"`
		FileNameHash    string       `json:"file_name_hash"`
		CreationTime    string       `json:"creation_time"`
		Appends         []AppendInfo `json:"appends"`
	}{}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	// Parse the FileNameHash string back to big.Int
	fileNameHash, ok := new(big.Int).SetString(aux.FileNameHash, 10)
	if !ok {
		return errors.New("invalid file_name_hash format")
	}
	fm.FileName = aux.FileName
	fm.FileContentHash = aux.FileContentHash
	fm.FileNameHash = *fileNameHash
	fm.CreationTime = aux.CreationTime
	fm.Appends = aux.Appends
	return nil
}

// AppendInfo represents one append operation to a file.
type AppendInfo struct {
	FileName        string
	AppendID        string // FileName + ClientTimestamp + ClientID
	ClientID        string
	ClientTimestamp string // Unix time in microseconds (guaranteed increasing by client)
	AppendHash      string
	Size            int64
}

func (fileMeta *FileMetadata) insertAppend(newAppend AppendInfo) {
	inserted := false
	for i, a := range fileMeta.Appends {
		if a.ClientID == newAppend.ClientID &&
			a.ClientTimestamp > newAppend.ClientTimestamp {
			// Insert before this append
			fileMeta.Appends = append(fileMeta.Appends[:i],
				append([]AppendInfo{newAppend}, fileMeta.Appends[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		fileMeta.Appends = append(fileMeta.Appends, newAppend)
	}
}

// getFileLock returns the per-file mutex (helper, not exported)
func (m *Metadata) getFileLock(filename string) *sync.RWMutex {
	m.mu.RLock()
	lock := m.locks[filename]
	m.mu.RUnlock()
	return lock
}

// AddFile adds or overwrites a file (creates per-file lock if missing)
func (m *Metadata) AddFile(meta FileMetadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Files[meta.FileName] = meta
	if m.locks == nil {
		m.locks = make(map[string]*sync.RWMutex)
	}
	if _, ok := m.locks[meta.FileName]; !ok {
		m.locks[meta.FileName] = &sync.RWMutex{}
	}
}

// GetFile returns a copy (safe for outside use)
func (m *Metadata) GetFile(filename string) (FileMetadata, bool) {
	m.mu.RLock()
	entry, ok := m.Files[filename]
	m.mu.RUnlock()
	if !ok {
		return FileMetadata{}, false
	}
	lock := m.getFileLock(filename)
	if lock != nil {
		lock.RLock()
		defer lock.RUnlock()
	}
	return entry, true
}

// AppendToFile safely appends to the file; avoids duplicates
func (m *Metadata) AppendToFile(filename string, appendInfo AppendInfo) error {
	lock := m.getFileLock(filename)
	if lock == nil {
		return errors.New("file does not exist")
	}
	lock.Lock()
	defer lock.Unlock()

	m.mu.RLock()
	fileMeta := m.Files[filename]
	m.mu.RUnlock()

	// Only append if not a duplicate
	for _, a := range fileMeta.Appends {
		if a.AppendID == appendInfo.AppendID {
			return nil // already exists
		}
	}
	fileMeta.insertAppend(appendInfo)

	m.mu.Lock()
	m.Files[filename] = fileMeta
	m.mu.Unlock()
	return nil
}

// ListFiles returns a snapshot of file names
func (m *Metadata) ListFiles() []string {
	m.mu.RLock()
	names := make([]string, 0, len(m.Files))
	for name := range m.Files {
		names = append(names, name)
	}
	m.mu.RUnlock()
	return names
}

// DeleteFile deletes a file from metadata in a thread-safe manner
// Returns true if the file was deleted, false if it didn't exist
func (m *Metadata) DeleteFile(filename string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if file exists
	if _, exists := m.Files[filename]; !exists {
		return false
	}

	// Delete from Files map
	delete(m.Files, filename)

	// Delete from locks map if it exists
	if m.locks != nil {
		delete(m.locks, filename)
	}

	return true
}

// PrintFileMetadata prints the metadata for a file with its appends in order
func (m *Metadata) PrintFileMetadata(filename string) {
	fileMeta, ok := m.GetFile(filename)
	if !ok {
		ConsolePrintf("File '%s' not found in metadata\n", filename)
		return
	}

	ConsolePrintf("\n=== File Metadata: %s ===\n", filename)
	ConsolePrintf("FileName:        %s\n", fileMeta.FileName)
	ConsolePrintf("FileContentHash: %s\n", fileMeta.FileContentHash)
	ConsolePrintf("FileNameHash:    %s\n", fileMeta.FileNameHash.String())
	ConsolePrintf("CreationTime:    %s\n", fileMeta.CreationTime)
	ConsolePrintf("Number of Appends: %d\n", len(fileMeta.Appends))

	if len(fileMeta.Appends) > 0 {
		ConsolePrintf("\n--- Appends (in order) ---\n")
		for i, append := range fileMeta.Appends {
			ConsolePrintf("  [%d] AppendID:        %s\n", i+1, append.AppendID)
			ConsolePrintf("      ClientID:        %s\n", append.ClientID)
			ConsolePrintf("      ClientTimestamp: %s\n", append.ClientTimestamp)
			ConsolePrintf("      AppendHash:      %s\n", append.AppendHash)
			ConsolePrintf("      Size:            %d bytes\n", append.Size)
			ConsolePrintf("\n")
		}
	}
	ConsolePrintf("===========================\n\n")
}
