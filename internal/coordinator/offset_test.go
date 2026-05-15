package coordinator

import "testing"

func TestCommitAndGetOffset(t *testing.T) {
	om := NewOffsetManager(t.TempDir())

	om.Commit("g1", "orders", 2, 11)
	offset := om.GetOffset("g1", "orders", 2)

	if offset != 11 {
		t.Fatalf("expected offset 11, got %d", offset)
	}
}

func TestOffsetPersistence(t *testing.T) {
	dir := t.TempDir()

	om := NewOffsetManager(dir)
	om.Commit("g2", "payments", 1, 9)

	om2 := NewOffsetManager(dir)
	offset := om2.GetOffset("g2", "payments", 1)
	if offset != 9 {
		t.Fatalf("expected persisted offset 9, got %d", offset)
	}
}
