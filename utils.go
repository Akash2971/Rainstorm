package main

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

// HashToMbits takes any input string (could be "IP:port" or a filename)
// and hashes it down to an m-bit decimal value using Config.HashBits.
func HashToMbits(input string) big.Int {
	// Compute SHA-1 hash
	hash := sha1.Sum([]byte(input))

	// Convert hash to big.Int
	hashInt := new(big.Int).SetBytes(hash[:])

	// Truncate to m bits: hash % 2^m
	mod := new(big.Int).Lsh(big.NewInt(1), uint(Config.HashBits))
	hashInt.Mod(hashInt, mod)

	return *hashInt
}

// GetAddressFromID extracts address from member ID
func GetAddressFromID(memberID string) string {
	lastDash := strings.LastIndex(memberID, "-")
	if lastDash == -1 {
		return ""
	}
	return memberID[:lastDash]
}

// convertToGRPCAddress converts UDP address to gRPC address (port + 1000)
func convertToGRPCAddress(udpAddr string) string {
	parts := strings.Split(udpAddr, ":")
	if len(parts) != 2 {
		return udpAddr
	}

	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return udpAddr
	}

	grpcPort := port + 1000
	return fmt.Sprintf("%s:%d", host, grpcPort)
}
