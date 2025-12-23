package main

import (
	"encoding/json"
	"fmt"
	"net"
)

// SendRainstormMessage sends a message to a Rainstorm TCP RPC server
func SendRainstormMessage(address string, msgType string, payload interface{}) (interface{}, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Rainstorm server at %s: %w", address, err)
	}
	defer conn.Close()

	// Marshal payload to JSON
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create message
	msg := RainstormMessage{
		Type:    msgType,
		Payload: payloadJSON,
	}

	// Send message
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(&msg); err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	// Receive response
	var response map[string]interface{}
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to receive response: %w", err)
	}

	// Check for error in response
	if success, ok := response["success"].(bool); ok && !success {
		if errMsg, ok := response["error"].(string); ok {
			return nil, fmt.Errorf("server error: %s", errMsg)
		}
		return nil, fmt.Errorf("server returned error response")
	}

	return response, nil
}

// SendTuple sends a tuple message to a Rainstorm server
// If exactlyOnceMgr is provided, the tuple will be tracked in sentTuples before sending
// If originalID is provided, it will be used as the original tuple ID; otherwise tuple.ID is used
func SendTuple(address string, tuple *Tuple, exactlyOnceMgr *ExactlyOnceManager, originalID ...string) (interface{}, error) {
	// If ExactlyOnceManager is provided, track the tuple before sending

	origID := tuple.ID
	if len(originalID) > 0 {
		origID = originalID[0]
	}
	exactlyOnceMgr.AddSentTuple(tuple, origID, address)

	return SendRainstormMessage(address, "TUPLE", tuple)
}

// SendACK sends an ACK message to a Rainstorm server
func SendACK(address string, tupleID string, taskID string) (interface{}, error) {
	ack := map[string]string{
		"tuple_id": tupleID,
		"task_id":  taskID,
	}
	return SendRainstormMessage(address, "ACK", ack)
}

// SendControlMessage sends a control message to a Rainstorm server
func SendControlMessage(address string, action string, params map[string]interface{}) (interface{}, error) {
	control := map[string]interface{}{
		"action": action,
	}
	// Merge params into control
	for k, v := range params {
		control[k] = v
	}
	return SendRainstormMessage(address, "CONTROL", control)
}
