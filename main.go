package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <nodeName> or %s task-listener <taskConfigPath>\n", os.Args[0], os.Args[0])
		os.Exit(1)
	}

	// Normal server mode
	nodeName := os.Args[1]

	// Initialize logger first
	if err := InitializeLogger(nodeName); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	ConsolePrintf("Current node name: %s\n", nodeName)
	nodeAddr := NodeMap[nodeName]
	ConsolePrintf("Current node address: %s\n", nodeAddr)
	var isIntroducer = false
	if nodeAddr == Config.IntroducerAddr {
		isIntroducer = true
	}

	s := NewServer(nodeAddr, Config.IntroducerAddr, isIntroducer)
	if err := s.Start(); err != nil {
		LogError(true, "Server error: %v", err)
	}
}
