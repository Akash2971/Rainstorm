package main

import (
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	pb "rainstorm/proto"

	"google.golang.org/grpc"
)

// HyDFSServer implements the gRPC HyDFSService
type HyDFSServer struct {
	pb.UnimplementedHyDFSServiceServer
	server *Server
}

// FileTransfer handles streaming file upload
func (h *HyDFSServer) FileTransfer(stream grpc.ClientStreamingServer[pb.FileChunk, pb.UploadStatus]) error {
	var filename string
	var fileData []byte
	var fileMetadata *FileMetadata

	// Receive file chunks from the client
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// End of stream - save the file
			break
		}
		if err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("Error receiving chunk: %v", err),
			})
		}

		// First chunk contains the filename and metadata
		if filename == "" {
			filename = chunk.GetFilename()
		}

		// Extract metadata from first chunk only if it has FileContentHash
		// This is because metadata is only in the first chunk
		if fileMetadata == nil && chunk.GetFileContentHash() != "" {
			fileNameHash := big.NewInt(int64(chunk.GetFileNameHash()))
			fileMetadata = &FileMetadata{
				FileName:        filename,
				FileContentHash: chunk.GetFileContentHash(),
				FileNameHash:    *fileNameHash,
				CreationTime:    chunk.GetCreationTime(),
				Appends:         []AppendInfo{},
			}
			LogInfo(false, "Received file metadata: %+v\n", fileMetadata)
		}

		// Append data
		fileData = append(fileData, chunk.GetData()...)
		if h.server.BandwidthStats != nil {
			h.server.BandwidthStats.AddReceived(uint64(len(chunk.GetData())))
		}
	}

	// Check if filename is already present in metadata
	if _, exists := h.server.Metadata.GetFile(filename); exists {
		return stream.SendAndClose(&pb.UploadStatus{
			Success: false,
			Message: fmt.Sprintf("File %s already exists in metadata", filename),
		})
	}

	// Save the file
	if err := h.saveFile(fileData, fileMetadata); err != nil {
		return stream.SendAndClose(&pb.UploadStatus{
			Success: false,
			Message: fmt.Sprintf("Error saving file: %v", err),
		})
	}

	LogInfo(true, "FILE RECEIVED: %s (%d bytes)\n", filename, len(fileData))

	return stream.SendAndClose(&pb.UploadStatus{
		Success: true,
		Message: fmt.Sprintf("File %s received successfully", filename),
	})
}

// GetFile streams file chunks to the client
func (h *HyDFSServer) GetFile(req *pb.FileRequest, stream grpc.ServerStreamingServer[pb.FileChunk]) error {
	// Open file from hydfs

	filename := req.GetFilename()
	hydfsDir := h.server.FileDirectory

	LogInfo(true, "RECEIVED GET REQUEST for: file=%s\n", filename)

	meta, ok := h.server.Metadata.GetFile(filename)
	if !ok {
		return fmt.Errorf("file %s not found in metadata", filename)
	}

	// --- Gather all file paths in order ---
	filePaths := []string{filepath.Join(hydfsDir, meta.FileName)}

	// Add append file paths
	for _, appendInfo := range meta.Appends {
		LogInfo(true, "Appending chunks for Get Request with AppendID:%s\n", appendInfo.AppendID)
		appendPath := filepath.Join(hydfsDir, appendInfo.AppendID)
		filePaths = append(filePaths, appendPath)
	}

	buf := make([]byte, 64*1024)
	for i, path := range filePaths {
		f, err := os.Open(path)
		if err != nil {
			// If append file missing, log and skip (to preserve availability)
			ConsolePrintf("Warning: failed to open %s: %v\n", path, err)
			continue
		}

		for {
			n, err := f.Read(buf)
			if n > 0 {
				chunk := &pb.FileChunk{
					Filename: filename,
					Data:     append([]byte(nil), buf[:n]...), // copy slice
				}
				if sendErr := stream.Send(chunk); sendErr != nil {
					f.Close()
					return fmt.Errorf("failed to send chunk from %s: %v", path, sendErr)
				}
				if h.server.BandwidthStats != nil {
					h.server.BandwidthStats.AddSent(uint64(len(chunk.GetData())))
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return fmt.Errorf("read error from %s: %v", path, err)
			}
		}
		f.Close()

		// Add newline separator between files (after original file and each append)
		// Skip if this is the last file
		if i < len(filePaths)-1 {
			newlineChunk := &pb.FileChunk{
				Filename: filename,
				Data:     []byte("\n"),
			}
			if sendErr := stream.Send(newlineChunk); sendErr != nil {
				return fmt.Errorf("failed to send newline separator: %v", sendErr)
			}
			if h.server.BandwidthStats != nil {
				h.server.BandwidthStats.AddSent(uint64(len(newlineChunk.GetData())))
			}
		}
	}
	LogInfo(true, "Completed GET REQUEST for: file=%s\n", filename)

	return nil
}

// AppendTransfer handles streaming append upload
func (h *HyDFSServer) AppendTransfer(stream pb.HyDFSService_AppendTransferServer) error {
	var filename, appendId, clientId, clientTimestamp, appendContentHash string
	var appendData []byte
	var appendInfo AppendInfo
	receivedFirstChunk := false

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("Error receiving append chunk: %v", err),
			})
		}

		if !receivedFirstChunk {
			filename = chunk.GetFilename()
			appendId = chunk.GetAppendId()
			clientId = chunk.GetClientId()
			clientTimestamp = chunk.GetClientTimestamp()
			appendContentHash = chunk.GetAppendContentHash()
			receivedFirstChunk = true
		}
		appendData = append(appendData, chunk.GetData()...)
		if h.server.BandwidthStats != nil {
			h.server.BandwidthStats.AddReceived(uint64(len(chunk.GetData())))
		}
	}

	if _, ok := h.server.Metadata.GetFile(filename); !ok {
		LogError(true, "Append couldnt be completed as the file:%s does not exist", filename)
		return stream.SendAndClose(&pb.UploadStatus{
			Success: false,
			Message: fmt.Sprintf("file %s does not exist", filename),
		})
	}

	// Compose AppendInfo from chunk meta info
	appendInfo = AppendInfo{
		FileName:        filename,
		AppendID:        appendId,
		ClientID:        clientId,
		ClientTimestamp: clientTimestamp,
		AppendHash:      appendContentHash,
		Size:            int64(len(appendData)),
	}
	LogInfo(true, "RECEIVED APPEND: file=%s appendID=%s clientID=%s ts=%s (%d bytes)\n", filename, appendId, clientId, clientTimestamp, len(appendData))

	// For most HyDFS files, we save each append as a unique chunk file (filename.appendid).
	// For WAL files (identified by "wal-" prefix) and Rainstorm output files (identified
	// by "out-" prefix), we instead append directly to the base file so that all data
	// lives in a single file per logical stream.
	hydfsDir := h.server.FileDirectory
	if err := os.MkdirAll(hydfsDir, 0755); err != nil {
		return stream.SendAndClose(&pb.UploadStatus{
			Success: false,
			Message: fmt.Sprintf("failed to create hydfs dir: %v", err),
		})
	}

	if strings.HasPrefix(filename, "wal-") || strings.HasPrefix(filename, "out-") {
		// Append directly to the main WAL/output file.
		filePath := filepath.Join(hydfsDir, filename)
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("failed to open WAL file for append: %v", err),
			})
		}
		if _, err := f.Write(appendData); err != nil {
			f.Close()
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("failed to append to WAL file: %v", err),
			})
		}
		if err := f.Close(); err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("failed to close WAL file after append: %v", err),
			})
		}
	} else {
		// Default behavior: store each append as a separate chunk file named by appendId.
		filePath := filepath.Join(hydfsDir, appendId)
		if err := os.WriteFile(filePath, appendData, 0644); err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("failed to write append file: %v", err),
			})
		}
	}

	// Store/insert append to metadata for regular files.
	// For WAL/output files (wal-/out- prefix), we keep the on-disk data as a single file
	// and skip per-append metadata entries to avoid bloating HyDFS metadata.
	if !strings.HasPrefix(filename, "wal-") && !strings.HasPrefix(filename, "out-") {
		if err := h.server.Metadata.AppendToFile(filename, appendInfo); err != nil {
			return stream.SendAndClose(&pb.UploadStatus{
				Success: false,
				Message: fmt.Sprintf("append metadata update failed: %v", err),
			})
		}
		LogInfo(true, "Stored append metadata for file: %s, appendId: %s\n", filename, appendId)
	} else {
		LogInfo(true, "Skipping metadata append entry for WAL file: %s (appendId: %s)\n", filename, appendId)
	}

	return stream.SendAndClose(&pb.UploadStatus{
		Success: true,
		Message: fmt.Sprintf("Append for file %s (appendId %s) received", filename, appendId),
	})
}

// saveFile saves the file to the hydfs directory and stores metadata
func (h *HyDFSServer) saveFile(data []byte, metadata *FileMetadata) error {
	// Create hydfs directory if it doesn't exist
	hydfsDir := h.server.FileDirectory
	if err := os.MkdirAll(hydfsDir, 0755); err != nil {
		return fmt.Errorf("failed to create hydfs directory: %v", err)
	}

	// Write file
	filePath := filepath.Join(hydfsDir, metadata.FileName)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	// Store metadata if provided
	if metadata != nil {
		// Initialize metadata map if it doesn't exist
		if h.server.Metadata.Files == nil {
			h.server.Metadata.Files = make(map[string]FileMetadata)
		}
		h.server.Metadata.AddFile(*metadata)
		LogInfo(true, "Stored metadata for file: %s\n", metadata.FileName)
	}

	return nil
}

// StartHyDFSGRPCServer starts the gRPC server for HyDFS
func (s *Server) StartHyDFSGRPCServer() {
	// Parse UDP address and create gRPC address (UDP port + 1000)
	host, portStr, err := net.SplitHostPort(s.Addr)
	if err != nil {
		LogError(true, "Failed to parse UDP address: %v", err)
		return
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		LogError(true, "Failed to parse port: %v", err)
		return
	}

	grpcPort := port + 1000
	grpcAddr := fmt.Sprintf("%s:%d", host, grpcPort)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		LogError(true, "Failed to listen on gRPC port %s: %v", grpcAddr, err)
		return
	}

	grpcServer := grpc.NewServer()
	pb.RegisterHyDFSServiceServer(grpcServer, &HyDFSServer{server: s})

	ConsolePrintf("HyDFS gRPC server listening on %s\n", grpcAddr)

	if err := grpcServer.Serve(lis); err != nil {
		LogError(true, "Failed to serve gRPC: %v", err)
	}
}
