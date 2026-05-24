package replication

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// TCPTransport performs inter-broker replication RPCs over Flux text protocol.
type TCPTransport struct {
	LeaderAddr string
	Timeout    time.Duration
	seq        uint64
}

func NewTCPTransport(leaderAddr string, timeout time.Duration) *TCPTransport {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &TCPTransport{
		LeaderAddr: leaderAddr,
		Timeout:    timeout,
	}
}

func (t *TCPTransport) Fetch(topic string, partition int, followerID int, offset int, maxMessages int) (FetchResult, error) {
	if maxMessages <= 0 {
		maxMessages = 100
	}
	args := fmt.Sprintf("%s %d %d %d %d", topic, partition, followerID, offset, maxMessages)
	line, err := t.send("BROKER_FETCH", args)
	if err != nil {
		return FetchResult{}, err
	}
	payload, err := parseOKPayload(line)
	if err != nil {
		return FetchResult{}, err
	}

	hw, err := extractIntField(payload, "hw")
	if err != nil {
		return FetchResult{}, err
	}
	recordsField := extractStringField(payload, "records")
	if recordsField == "" {
		return FetchResult{HighWatermark: hw, Records: []ReplicationRecord{}}, nil
	}
	records := make([]ReplicationRecord, 0)
	for _, item := range strings.Split(recordsField, ",") {
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			return FetchResult{}, fmt.Errorf("malformed record payload: %s", payload)
		}
		off, err := strconv.Atoi(parts[0])
		if err != nil {
			return FetchResult{}, fmt.Errorf("invalid record offset: %s", parts[0])
		}
		records = append(records, ReplicationRecord{Offset: off, Value: parts[1]})
	}

	return FetchResult{
		HighWatermark: hw,
		Records:       records,
	}, nil
}

func (t *TCPTransport) Ack(topic string, partition int, followerID int, ackedOffset int) error {
	args := fmt.Sprintf("%s %d %d %d", topic, partition, followerID, ackedOffset)
	line, err := t.send("BROKER_REPLICA_ACK", args)
	if err != nil {
		return err
	}
	_, err = parseOKPayload(line)
	return err
}

func (t *TCPTransport) send(command string, args string) (string, error) {
	if t.LeaderAddr == "" {
		return "", fmt.Errorf("leader address is required")
	}

	conn, err := net.DialTimeout("tcp", t.LeaderAddr, t.Timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(t.Timeout))

	correlation := strconv.FormatUint(atomic.AddUint64(&t.seq, 1), 10)
	line := fmt.Sprintf("V1|%s|%s|%s\n", correlation, command, args)
	if _, err := conn.Write([]byte(line)); err != nil {
		return "", err
	}

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(resp), nil
}

func parseOKPayload(line string) (string, error) {
	parts := strings.Split(line, "|")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid response line: %s", line)
	}
	if parts[2] == "ERR" {
		code := "UNKNOWN_ERROR"
		msg := "unknown error"
		if len(parts) > 3 {
			code = parts[3]
		}
		if len(parts) > 4 {
			msg = strings.Join(parts[4:], "|")
		}
		return "", fmt.Errorf("%s: %s", code, msg)
	}
	if parts[2] != "OK" {
		return "", fmt.Errorf("unknown response status: %s", parts[2])
	}
	if len(parts) < 4 {
		return "", nil
	}
	return strings.Join(parts[3:], "|"), nil
}

func extractIntField(payload string, key string) (int, error) {
	raw := extractStringField(payload, key)
	if raw == "" {
		return 0, fmt.Errorf("missing field %s in payload: %s", key, payload)
	}
	return strconv.Atoi(raw)
}

func extractStringField(payload string, key string) string {
	token := key + "="
	idx := strings.Index(payload, token)
	if idx == -1 {
		return ""
	}
	rest := payload[idx+len(token):]
	space := strings.Index(rest, " ")
	if space == -1 {
		return rest
	}
	return rest[:space]
}
