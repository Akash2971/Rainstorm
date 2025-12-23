package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "rainstorm/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Global connection pool for HyDFS gRPC clients, keyed by gRPC address (host:port).
// This lets us reuse a single connection per target instead of dialing/closing
// on every CREATE/GET/APPEND, which is especially important on platforms like
// Windows that have stricter limits on concurrent socket usage.
var (
	hydfsConnMu sync.Mutex
	hydfsConns  = make(map[string]*grpc.ClientConn)
)

func getHyDFSConn(grpcAddr string) (*grpc.ClientConn, error) {
	hydfsConnMu.Lock()
	defer hydfsConnMu.Unlock()

	if conn, ok := hydfsConns[grpcAddr]; ok {
		return conn, nil
	}

	conn, err := grpc.Dial(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", grpcAddr, err)
	}

	hydfsConns[grpcAddr] = conn
	return conn, nil
}

// SendFileToNode sends a file to a target node via gRPC streaming
func (s *Server) SendFileToNode(targetAddr string, data []byte, metadata FileMetadata) error {
	// Convert UDP address to gRPC address
	grpcAddr := convertToGRPCAddress(targetAddr)

	// Get (or create) a reusable gRPC connection to the target node.
	conn, err := getHyDFSConn(grpcAddr)
	if err != nil {
		return err
	}

	client := pb.NewHyDFSServiceClient(conn)
	stream, err := client.FileTransfer(context.Background())
	if err != nil {
		return fmt.Errorf("failed to create stream: %v", err)
	}

	// Send file in chunks (64KB chunks)
	chunkSize := 64 * 1024
	isFirstChunk := true
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := &pb.FileChunk{
			Data: data[i:end],
		}

		// Add filename and metadata to first chunk only
		if isFirstChunk {
			chunk.Filename = metadata.FileName
			chunk.FileContentHash = metadata.FileContentHash
			chunk.FileNameHash = int32(metadata.FileNameHash.Int64())
			chunk.CreationTime = metadata.CreationTime
			isFirstChunk = false
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send chunk: %v", err)
		}
		if s.BandwidthStats != nil {
			s.BandwidthStats.AddSent(uint64(len(chunk.GetData())))
		}
	}

	// Log metadata for debugging
	LogInfo(true, "Sent file metadata: %+v\n", metadata)

	// Close and receive response
	status, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close stream: %v", err)
	}

	if !status.GetSuccess() {
		return fmt.Errorf("upload failed: %s", status.GetMessage())
	}

	LogInfo(true, "File sent to %s | %s : %s\n", AddressToVMName[targetAddr], targetAddr, status.GetMessage())
	return nil
}

// ReceiveFileFromNode downloads a file from target node via server streaming and writes to local path
func (s *Server) ReceiveFileFromNode(targetAddr string, hyDFSfilename string, directory string, localPath string) error {
	grpcAddr := convertToGRPCAddress(targetAddr)

	conn, err := getHyDFSConn(grpcAddr)
	if err != nil {
		return err
	}

	client := pb.NewHyDFSServiceClient(conn)
	stream, err := client.GetFile(context.Background(), &pb.FileRequest{Filename: hyDFSfilename})
	if err != nil {
		return fmt.Errorf("GetFile RPC failed: %v", err)
	}

	if err := os.MkdirAll(directory, 0755); err != nil {
		return fmt.Errorf("failed to ensure directory: %v", err)
	}
	downloadPath := filepath.Join(directory, filepath.Base(localPath))

	f, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %v", err)
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if s.BandwidthStats != nil {
			s.BandwidthStats.AddReceived(uint64(len(chunk.GetData())))
		}
		if err != nil {
			f.Close()
			os.Remove(downloadPath)
			return fmt.Errorf("stream recv error: %v", err)
		}

		defer f.Close()
		if _, err := f.Write(chunk.GetData()); err != nil {
			f.Close()
			os.Remove(downloadPath)
			return fmt.Errorf("write error: %v", err)
		}
	}

	ConsolePrintf("File downloaded from %s | %s to %s\n", AddressToVMName[targetAddr], targetAddr, downloadPath)
	return nil
}

// SendFileToNode sends a file to a target node via gRPC streaming
func (s *Server) SendAppendToNode(targetAddr string, data []byte, appendInfo AppendInfo) error {
	// Convert UDP address to gRPC address
	grpcAddr := convertToGRPCAddress(targetAddr)

	// Get (or create) a reusable gRPC connection to the target node.
	conn, err := getHyDFSConn(grpcAddr)
	if err != nil {
		return err
	}

	client := pb.NewHyDFSServiceClient(conn)
	stream, err := client.AppendTransfer(context.Background()) // use AppendTransfer
	if err != nil {
		return fmt.Errorf("failed to create append stream: %v", err)
	}

	// Send file in chunks (64KB chunks)
	chunkSize := 64 * 1024
	isFirstChunk := true
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := &pb.AppendChunk{
			Data: data[i:end],
		}
		if isFirstChunk {
			chunk.Filename = appendInfo.FileName
			chunk.AppendContentHash = appendInfo.AppendHash // (add these fields to AppendInfo if missing)
			chunk.ClientId = appendInfo.ClientID
			chunk.ClientTimestamp = appendInfo.ClientTimestamp
			chunk.AppendId = appendInfo.AppendID
			isFirstChunk = false
		}

		if err := stream.Send(chunk); err != nil {
			return fmt.Errorf("failed to send append chunk: %v", err)
		}
		if s.BandwidthStats != nil {
			s.BandwidthStats.AddSent(uint64(len(chunk.GetData())))
		}
	}

	// Log appendInfo for debugging
	LogInfo(true, "Sent append metadata: %+v\n", appendInfo)

	status, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close append stream: %v", err)
	}

	if !status.GetSuccess() {
		return fmt.Errorf("append upload failed: %s", status.GetMessage())
	}

	LogInfo(true, "Append sent to %s (gRPC: %s): %s\n", targetAddr, grpcAddr, status.GetMessage())
	return nil
}
