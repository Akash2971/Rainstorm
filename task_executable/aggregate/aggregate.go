package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Tuple represents a data tuple (same structure as in task.go)
type Tuple struct {
	ID        string
	Key       string
	Value     string
	Timestamp time.Time
	SenderID  string
	TaskID    string
	EOF       *bool `json:"EOF,omitempty"` // Optional EOF marker
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	// Map to store count of tuples by key
	// Key -> count
	keyCounts := make(map[string]int)

	// Read lines from stdin
	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse JSON tuple
		var tuple Tuple
		if err := json.Unmarshal([]byte(line), &tuple); err != nil {
			// If it's not valid JSON, skip it
			fmt.Fprintf(os.Stderr, "Error unmarshaling tuple: %v\n", err)
			continue
		}

		// If this is an EOF tuple, ignore it in streaming mode
		if tuple.EOF != nil && *tuple.EOF {
			continue
		}

		// Not an EOF tuple - increment count for this key and output immediately
		keyCounts[tuple.Key]++
		count := keyCounts[tuple.Key]

		// Create output tuple with current count as value
		outputTuple := Tuple{
			ID:        tuple.ID,
			Key:       tuple.Key,
			Value:     fmt.Sprintf("%d", count),
			Timestamp: time.Now(),
		}

		outputTuples := []Tuple{outputTuple}

		// Output structured format with OriginalTupleId and OutputTuples
		output := map[string]interface{}{
			"OriginalTupleId": tuple.ID,
			"OutputTuples":    outputTuples,
		}

		outputJSON, err := json.Marshal(output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling output: %v\n", err)
			continue
		}

		// Write to stdout
		fmt.Println(string(outputJSON))
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
		os.Exit(1)
	}
}
