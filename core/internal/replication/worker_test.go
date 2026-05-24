package replication

import (
	"fmt"
	"testing"
)

type fakeTransport struct {
	fetchCalls []int
	records    []ReplicationRecord
	ackOffsets []int
}

func (f *fakeTransport) Fetch(_ string, _ int, _ int, offset int, maxMessages int) (FetchResult, error) {
	f.fetchCalls = append(f.fetchCalls, offset)
	out := make([]ReplicationRecord, 0, maxMessages)
	for _, r := range f.records {
		if r.Offset >= offset {
			out = append(out, r)
			if len(out) >= maxMessages {
				break
			}
		}
	}
	return FetchResult{HighWatermark: len(f.records) - 1, Records: out}, nil
}

func (f *fakeTransport) Ack(_ string, _ int, _ int, ackedOffset int) error {
	f.ackOffsets = append(f.ackOffsets, ackedOffset)
	return nil
}

type fakeStorage struct {
	applied map[int]string
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{applied: map[int]string{}}
}

func (f *fakeStorage) AppendReplicated(_ string, _ int, offset int, value string) error {
	if _, exists := f.applied[offset]; exists {
		return fmt.Errorf("duplicate offset applied: %d", offset)
	}
	f.applied[offset] = value
	return nil
}

func TestFollowerWorkerStepOnceAppliesAndAcks(t *testing.T) {
	transport := &fakeTransport{
		records: []ReplicationRecord{
			{Offset: 0, Value: "a"},
			{Offset: 1, Value: "b"},
		},
	}
	storage := newFakeStorage()
	worker, err := NewFollowerWorker(WorkerConfig{
		FollowerID:  1,
		Topic:       "orders",
		Partition:   0,
		StartOffset: 0,
		MaxBatch:    10,
	}, transport, storage)
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}

	applied, err := worker.StepOnce()
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}
	if applied != 2 {
		t.Fatalf("expected 2 applied records, got %d", applied)
	}
	if worker.NextOffset() != 2 {
		t.Fatalf("expected next offset 2, got %d", worker.NextOffset())
	}
	if len(transport.ackOffsets) != 1 || transport.ackOffsets[0] != 1 {
		t.Fatalf("unexpected ack offsets: %#v", transport.ackOffsets)
	}
	if storage.applied[0] != "a" || storage.applied[1] != "b" {
		t.Fatalf("unexpected storage state: %#v", storage.applied)
	}
}

func TestFollowerWorkerStepOnceNoRecordsNoAck(t *testing.T) {
	transport := &fakeTransport{}
	storage := newFakeStorage()
	worker, err := NewFollowerWorker(WorkerConfig{
		FollowerID:  1,
		Topic:       "orders",
		Partition:   0,
		StartOffset: 0,
		MaxBatch:    10,
	}, transport, storage)
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}

	applied, err := worker.StepOnce()
	if err != nil {
		t.Fatalf("step failed: %v", err)
	}
	if applied != 0 {
		t.Fatalf("expected 0 applied records, got %d", applied)
	}
	if len(transport.ackOffsets) != 0 {
		t.Fatalf("expected no acks, got %#v", transport.ackOffsets)
	}
}

func TestFollowerWorkerRejectsGaps(t *testing.T) {
	transport := &fakeTransport{
		records: []ReplicationRecord{
			{Offset: 1, Value: "gap"},
		},
	}
	storage := newFakeStorage()
	worker, err := NewFollowerWorker(WorkerConfig{
		FollowerID:  1,
		Topic:       "orders",
		Partition:   0,
		StartOffset: 0,
		MaxBatch:    10,
	}, transport, storage)
	if err != nil {
		t.Fatalf("new worker failed: %v", err)
	}

	if _, err := worker.StepOnce(); err == nil {
		t.Fatalf("expected replication gap error")
	}
}
