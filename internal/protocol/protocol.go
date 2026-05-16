package protocol

import (
	"fmt"
	"strconv"
	"strings"
)

// Request is the parsed representation of one client protocol line.
type Request struct {
	Version       string
	CorrelationID string
	Command       string
	Args          []string
}

// Response documents the standard response envelope fields.
type Response struct {
	Version       string
	CorrelationID string
	Status        string
	Code          string
	Message       string
	Payload       string
}

// Version is the only currently supported protocol version token.
const Version = "V1"

// ParseRequest validates and parses one wire line into a typed Request.
func ParseRequest(line string) (*Request, error) {
	parts := strings.SplitN(strings.TrimSpace(line), "|", 4)
	if len(parts) != 4 {
		return nil, fmt.Errorf("expected format V1|<correlation_id>|<command>|<args>")
	}

	version := strings.TrimSpace(parts[0])
	correlationID := strings.TrimSpace(parts[1])
	command := strings.ToUpper(strings.TrimSpace(parts[2]))
	argLine := strings.TrimSpace(parts[3])

	if version != Version {
		return nil, fmt.Errorf("unsupported version")
	}
	if correlationID == "" {
		return nil, fmt.Errorf("correlation id required")
	}
	if command == "" {
		return nil, fmt.Errorf("command required")
	}

	args := []string{}
	if argLine != "" {
		args = strings.Fields(argLine)
	}

	return &Request{
		Version:       version,
		CorrelationID: correlationID,
		Command:       command,
		Args:          args,
	}, nil
}

// Ok formats a success response preserving correlation ID.
func Ok(correlationID string, payload string) string {
	if payload == "" {
		return fmt.Sprintf("%s|%s|OK\n", Version, correlationID)
	}
	return fmt.Sprintf("%s|%s|OK|%s\n", Version, correlationID, payload)
}

// Err formats a protocol-level error response.
func Err(correlationID string, code string, message string) string {
	if correlationID == "" {
		correlationID = "0"
	}
	return fmt.Sprintf("%s|%s|ERR|%s|%s\n", Version, correlationID, code, message)
}

// ParseInt converts numeric request arguments with field-specific errors.
func ParseInt(value string, field string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s", field)
	}
	return parsed, nil
}
