package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Tuple represents a data tuple (same structure as in task.go)
type Tuple struct {
	ID         string
	Key        string
	Value      string
	Timestamp  time.Time
	SenderID   string
	TaskID     string
	OriginalID string // Original input tuple ID that generated this output
}

// nthCSVCell returns the 1-based nth cell from a CSV line, handling quoted
// fields that may contain commas. It returns ok=false if parsing fails or
// the requested cell does not exist.
func nthCSVCell(line string, n int) (cell string, ok bool) {
	if n <= 0 {
		return "", false
	}
	r := csv.NewReader(strings.NewReader(line))
	r.FieldsPerRecord = -1

	record, err := r.Read()
	if err != nil || len(record) < n {
		return "", false
	}
	// Return the cell as-is (including any spaces) so callers can
	// decide how to handle whitespace.
	return record[n-1], true
}

func main() {
	// Check for pattern argument
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <pattern>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s \"ERROR\"\n", os.Args[0])
		os.Exit(1)
	}

	pattern := os.Args[1]

	// Optional: 1-based column index for comma-separated Value
	colIndex := 0
	if len(os.Args) >= 3 {
		if idx, err := strconv.Atoi(os.Args[2]); err == nil && idx > 0 {
			colIndex = idx
		}
	}

	scanner := bufio.NewScanner(os.Stdin)

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
			// If it's not valid JSON, check if the line itself contains the pattern
			continue
		}

		// Check if the tuple's Value contains the pattern (case-sensitive)
		outputTuples := make([]Tuple, 0)

		if strings.Contains(tuple.Value, pattern) {
			// If a column index is provided, use the Nth CSV cell from Value
			// as both Key and Value.
			if colIndex > 0 {
				if cell, ok := nthCSVCell(tuple.Value, colIndex); ok {
					tuple.Key = cell
					tuple.Value = cell
				} else {
					// If the requested cell doesn't exist, don't emit this tuple.
					continue
				}
			}

			// Set OriginalID to track which input tuple generated this output
			if tuple.OriginalID == "" {
				tuple.OriginalID = tuple.ID
			}
			// Add tuple to output array (filter passes through)
			outputTuples = append(outputTuples, tuple)
		}
		// If pattern doesn't match, outputTuples remains empty (filtered out)

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

		// Write to stdout (one line per input tuple)
		fmt.Println(string(outputJSON))
	}

	// Check for scanner errors
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading from stdin: %v\n", err)
		os.Exit(1)
	}
}
