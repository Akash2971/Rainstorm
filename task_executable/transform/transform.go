package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
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
}

// firstNCSVCells returns a CSV string containing the first n cells from a CSV line,
// correctly handling quoted fields that may contain commas.
func firstNCSVCells(line string, n int) (string, error) {
	r := csv.NewReader(strings.NewReader(line))
	// Allow variable number of fields
	r.FieldsPerRecord = -1

	record, err := r.Read()
	if err != nil {
		return "", err
	}
	if len(record) == 0 {
		return "", nil
	}
	if len(record) > n {
		record = record[:n]
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(record); err != nil {
		return "", err
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return "", err
	}

	// csv.Writer adds a trailing newline; trim it.
	return strings.TrimRight(buf.String(), "\r\n"), nil
}

func main() {
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
			// If it's not valid JSON, try to transform it as a CSV line
			if transformed, err := firstNCSVCells(line, 3); err == nil && transformed != "" {
				fmt.Println(transformed)
			} else {
				// Pass through if parsing fails
				fmt.Println(line)
			}
			continue
		}

		// Transform the Value field: keep only the first 3 CSV cells
		trimmedValue := tuple.Value
		if v, err := firstNCSVCells(tuple.Value, 3); err == nil && v != "" {
			trimmedValue = v
		}

		// Create array of output tuples
		outputTuples := make([]Tuple, 0)

		outputTuple := Tuple{
			ID:        tuple.ID,
			Key:       tuple.Key,
			Value:     trimmedValue,
			Timestamp: tuple.Timestamp,
			SenderID:  tuple.SenderID,
			TaskID:    tuple.TaskID,
		}
		outputTuples = append(outputTuples, outputTuple)

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
