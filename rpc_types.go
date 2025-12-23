package main

import "math/big"

// RPC Message wrapper
type RPCMessage struct {
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	ID     uint64      `json:"id"`
}

type RPCResponse struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error,omitempty"`
	ID     uint64      `json:"id"`
}

// Request/Response types for RPC calls

type JoinRequest struct {
	Member Member `json:"member"`
}

type JoinResponse struct {
	Success        bool          `json:"success"`
	Message        string        `json:"message"`
	MembershipList []Member      `json:"membership_list"`
	Protocol       ProtocolType  `json:"protocol_type"`
	Suspicion      SuspicionType `json:"suspicion_type"`
	IntroducerID   string        `json:"introducer_id"`
}

type GossipRequest struct {
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type GossipResponse struct {
	Success bool `json:"success"`
}

type PingRequest struct {
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type Ack struct {
	SenderID       string   `json:"sender_address"`
	MembershipList []Member `json:"membership_list"`
}

type ProtocolSwitchRequest struct {
	SenderID  string        `json:"sender_address"`
	Protocol  ProtocolType  `json:"protocol"`
	Suspicion SuspicionType `json:"suspicion"`
}

type ProtocolSwitchResponse struct {
	Success bool `json:"success"`
}

type GetFilesMetadataRequest struct {
	KeyStartRange big.Int `json:"sender_address"`
	KeyEndRange   big.Int `json:"protocol"`
}

type GetFileMetadataResponse struct {
	Success  bool `json:"success"`
	Metadata *Metadata
}

type GetFileMetadataRequest struct {
	Filename string
}

type MultiAppendRequest struct {
	HyDFSFileName string `json:"hydfs_filename"`
	LocalFileName string `json:"local_filename"`
}

type MultiAppendResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type UpdateAppendOrderRequest struct {
	FileName string       `json:"file_name"`
	Appends  []AppendInfo `json:"appends"`
}

type UpdateAppendOrderResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type MergeRequest struct {
	HyDFSFileName string `json:"hydfs_filename"`
}

type MergeResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type ExecuteCommandRequest struct {
	Command string
}

type ExecuteCommandResponse struct {
	Success bool
	Message string
}

// Streaming RPC types

type InitializeTasksRequest struct {
	JobID       string            `json:"job_id"`
	Topology    *Topology         `json:"topology"`
	Assignments map[string]string `json:"assignments"` // task ID -> node address
	JobSpec     *JobSpec          `json:"job_spec"`    // JobSpec with StageOps for task initialization
	SourceDir   string            `json:"source_dir"`  // HyDFS source directory (from JobSpec)
	InputRate   int               `json:"input_rate"`  // Tuples per second (from JobSpec)
}

type InitializeTasksResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type StartStreamingRequest struct {
	JobID string `json:"job_id"`
}

type StartStreamingResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type SubmitJobRequest struct {
	JobSpec *JobSpec `json:"job_spec"`
}

type SubmitJobResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type AddTaskRequest struct {
	Task        *Task    `json:"task"`         // The new task to add
	NodeAddress string   `json:"node_address"` // Node address where task should run
	JobSpec     *JobSpec `json:"job_spec"`     // Job spec for task initialization
}

type AddTaskResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type UpdateTopologyRequest struct {
	Topology *Topology `json:"topology"` // Updated topology
}

type UpdateTopologyResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type KillTaskRequest struct {
	TaskID string `json:"task_id"` // Task ID to kill
}

type KillTaskResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

type TaskStatusReport struct {
	TaskID string     `json:"task_id"`
	Status TaskStatus `json:"status"`
}

type NodeMetricsReport struct {
	NodeID      string              `json:"node_id"`      // Node identifier
	TaskMetrics []*TaskMetrics      `json:"task_metrics"` // Metrics for all tasks on this node
	TaskStatus  []*TaskStatusReport `json:"task_status"`  // Status for all tasks on this node
}

type ReportMetricsRequest struct {
	Report *NodeMetricsReport `json:"report"` // Metrics and status report from node
}

type ReportMetricsResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// MergeWalRequest requests that a node merge the WAL of a killed task into
// the WAL of a surviving task in the same stage. This is used during
// autoscaling scale-down to preserve at-least-once semantics.
type MergeWalRequest struct {
	OldTaskID  string `json:"old_task_id"`  // Task ID being removed
	KeepTaskID string `json:"keep_task_id"` // Task ID that will own the merged WAL
}

type MergeWalResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// TaskProcessInfo represents basic runtime information about a task process on a node.
type TaskProcessInfo struct {
	TaskID string `json:"task_id"` // Task identifier
	PID    int    `json:"pid"`     // OS process ID
	Stage  int    `json:"stage"`   // Stage number
	OpExe  string `json:"op_exe"`  // Executable used for this stage
	VM     string `json:"vm"`      // VM name (e.g., vm1, vm2)
}

// ListTasksRequest is sent to a node to request information about all local tasks.
type ListTasksRequest struct{}

// ListTasksResponse contains task process information for a node.
type ListTasksResponse struct {
	Success bool               `json:"success"`
	Message string             `json:"message"`
	Tasks   []*TaskProcessInfo `json:"tasks"`
}
