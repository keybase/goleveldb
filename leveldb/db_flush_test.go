package leveldb

import (
	"errors"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/testutil"
)

func tablesPerLevel(h *dbHarness) []int {
	v := h.db.s.version()
	defer v.release()
	counts := make([]int, len(v.levels))
	for level, tables := range v.levels {
		counts[level] = len(tables)
	}
	return counts
}

// closeUnlessFailed closes the harness only if the test passed: a failed
// test may have left the write lock held, and Close would then block forever.
func closeUnlessFailed(t *testing.T, h *dbHarness) {
	if !t.Failed() {
		h.close()
	}
}

// putWithin fails the test if a Put does not return within a few seconds,
// which means the write lock is held.
func putWithin(t *testing.T, h *dbHarness, key string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- h.db.Put([]byte(key), []byte("v"), h.wo) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("Put: got error: ", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Put blocked: the write lock is held")
	}
}

func TestDB_FlushMemdb(t *testing.T) {
	h := newDbHarness(t)
	defer closeUnlessFailed(t, h)

	// Existing tables that the flushed memdb overlaps, so a table compaction
	// would have inputs.
	for round := 0; round < 2; round++ {
		h.put("a", "v")
		h.put("z", "v")
		h.compactMem()
	}
	h.put("m", "v")
	before := tablesPerLevel(h)
	beforeTotal := h.totalTables()

	if err := h.db.FlushMemdb(); err != nil {
		t.Fatal("FlushMemdb: got error: ", err)
	}

	after := tablesPerLevel(h)
	for level, n := range before {
		if after[level] < n {
			t.Errorf("level %d: %d tables, had %d; FlushMemdb compacted tables", level, after[level], n)
		}
	}
	if n := h.totalTables(); n != beforeTotal+1 {
		t.Errorf("total tables is %d, want %d", n, beforeTotal+1)
	}
	if n := h.db.getEffectiveMem(); n.Len() != 0 {
		t.Errorf("memdb has %d entries after FlushMemdb", n.Len())
		n.decref()
	} else {
		n.decref()
	}
	h.getVal("m", "v")

	// An empty memdb writes no table.
	if err := h.db.FlushMemdb(); err != nil {
		t.Fatal("FlushMemdb: got error: ", err)
	}
	if n := h.totalTables(); n != beforeTotal+1 {
		t.Errorf("empty FlushMemdb: total tables is %d, want %d", n, beforeTotal+1)
	}
}

func TestDB_FlushMemdbDoesNotBlockWritersWhileWriting(t *testing.T) {
	h := newDbHarness(t)
	defer closeUnlessFailed(t, h)

	h.put("a", "v")
	h.stor.Stall(testutil.ModeSync, storage.TypeTable)
	done := make(chan error, 1)
	go func() { done <- h.db.FlushMemdb() }()

	// Wait until the flush is writing the table, which the stall holds up.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var stats DBStats
		if err := h.db.Stats(&stats); err != nil {
			t.Fatal("Stats: got error: ", err)
		}
		if stats.MemCompactionActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the memdb compaction never started")
		}
		time.Sleep(time.Millisecond)
	}

	// The memdb table write is stalled; writers must still get through.
	putWithin(t, h, "b")
	select {
	case err := <-done:
		t.Fatal("FlushMemdb returned before its table was written: ", err)
	default:
	}

	h.stor.Release(testutil.ModeSync, storage.TypeTable)
	if err := <-done; err != nil {
		t.Fatal("FlushMemdb: got error: ", err)
	}
}

func TestDB_FlushMemdbErrorReleasesWriteLock(t *testing.T) {
	h := newDbHarness(t)
	defer closeUnlessFailed(t, h)

	h.put("a", "v")
	h.stor.EmulateError(testutil.ModeCreate, storage.TypeJournal, errors.New("no space left on device"))
	if err := h.db.FlushMemdb(); err == nil {
		t.Fatal("FlushMemdb: expected an error")
	}
	h.stor.EmulateError(testutil.ModeCreate, storage.TypeJournal, nil)

	putWithin(t, h, "b")
	if err := h.db.FlushMemdb(); err != nil {
		t.Fatal("FlushMemdb: got error: ", err)
	}
}

func TestDB_OpenTransactionErrorReleasesWriteLock(t *testing.T) {
	h := newDbHarness(t)
	defer closeUnlessFailed(t, h)

	h.put("a", "v")
	h.stor.EmulateError(testutil.ModeCreate, storage.TypeJournal, errors.New("no space left on device"))
	if _, err := h.db.OpenTransaction(); err == nil {
		t.Fatal("OpenTransaction: expected an error")
	}
	h.stor.EmulateError(testutil.ModeCreate, storage.TypeJournal, nil)

	putWithin(t, h, "b")
	tr, err := h.db.OpenTransaction()
	if err != nil {
		t.Fatal("OpenTransaction: got error: ", err)
	}
	tr.Discard()
}
