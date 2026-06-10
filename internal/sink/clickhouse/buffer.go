// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache
// License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the
// License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// File: buffer.go is a thread-safe in-memory ring buffer used between row
// producers and the periodic flusher. Supports the two overflow policies
// declared in config (drop_oldest, block).
package clickhouse

import (
	"fmt"
	"sync"

	"github.com/open-gpdb/yagpcc/internal/config"
)

// OverflowMode controls Buffer behaviour when Append is called on a full
// buffer.
type OverflowMode int

const (
	// OverflowDropOldest evicts the oldest queued row to make room for the new
	// one. Append never blocks. The onOverflow callback is invoked once per
	// dropped row so callers can update a metric.
	OverflowDropOldest OverflowMode = iota
	// OverflowBlock makes Append wait on a condition variable until Drain
	// frees a slot.
	OverflowBlock
)

// String implements fmt.Stringer so Buffer errors render the mode in a
// recognisable form.
func (m OverflowMode) String() string {
	switch m {
	case OverflowDropOldest:
		return config.OnBufferOverflowDropOldest
	case OverflowBlock:
		return config.OnBufferOverflowBlock
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// ParseOverflowMode converts the YAML string ("drop_oldest" / "block") into
// the typed OverflowMode the Buffer accepts. Returns an error for any other
// value so callers fail loudly during construction rather than at append time.
func ParseOverflowMode(s string) (OverflowMode, error) {
	switch s {
	case config.OnBufferOverflowDropOldest:
		return OverflowDropOldest, nil
	case config.OnBufferOverflowBlock:
		return OverflowBlock, nil
	default:
		return 0, fmt.Errorf("clickhouse: unknown on_buffer_overflow %q", s)
	}
}

// Buffer is a fixed-capacity FIFO of arbitrary rows. All methods are safe for
// concurrent use.
//
// The buffer is implemented as a slice-backed ring (head/tail/count) so both
// Append and Drain are O(1) per element regardless of how full the buffer is.
type Buffer struct {
	mu   sync.Mutex
	cond *sync.Cond

	capacity int
	mode     OverflowMode

	// onOverflow is called once per row dropped under OverflowDropOldest. It
	// is held without the buffer lock to avoid surprising lock orderings if
	// the callback is itself synchronised. May be nil.
	onOverflow func()

	storage []any
	head    int // index of the oldest queued row
	tail    int // index where the next Append will write
	count   int
	closed  bool
}

// NewBuffer constructs a Buffer with the given capacity and overflow mode.
// onOverflow may be nil; when non-nil it is invoked once per row dropped due
// to OverflowDropOldest (it is NOT called for explicit Drain or for OverflowBlock).
func NewBuffer(capacity int, mode OverflowMode, onOverflow func()) (*Buffer, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("clickhouse: buffer capacity must be > 0, got %d", capacity)
	}
	if mode != OverflowDropOldest && mode != OverflowBlock {
		return nil, fmt.Errorf("clickhouse: invalid overflow mode %s", mode)
	}
	b := &Buffer{
		capacity:   capacity,
		mode:       mode,
		onOverflow: onOverflow,
		storage:    make([]any, capacity),
	}
	b.cond = sync.NewCond(&b.mu)
	return b, nil
}

// Append adds row to the buffer.
//
// Behaviour when the buffer is full:
//   - OverflowDropOldest: evicts the oldest row, invokes onOverflow, then
//     stores the new row. Never blocks.
//   - OverflowBlock: blocks the caller until another goroutine calls Drain
//     or Close to free a slot.
//
// Once Close has been called, Append drops the row (counted as overflow when
// onOverflow is set) and returns immediately so producers can unwind during
// shutdown without deadlocking on cond.Wait.
//
// Append returns immediately if the row was stored. Drop callbacks fire while
// the buffer mutex is held — keep them constant-time (e.g. counter Inc).
func (b *Buffer) Append(row any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.count >= b.capacity {
		if b.closed {
			if b.onOverflow != nil {
				b.onOverflow()
			}
			return
		}
		switch b.mode {
		case OverflowDropOldest:
			b.dropOldestLocked()
		case OverflowBlock:
			b.cond.Wait()
		}
	}
	if b.closed {
		if b.onOverflow != nil {
			b.onOverflow()
		}
		return
	}
	b.storage[b.tail] = row
	b.tail = (b.tail + 1) % b.capacity
	b.count++
}

// Close marks the buffer as closed and wakes any Append blocked under
// OverflowBlock so they can return without storing the row. Subsequent
// Appends short-circuit. Drain is unaffected — already-queued rows can still
// be drained after Close. Idempotent.
func (b *Buffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	b.cond.Broadcast()
}

// dropOldestLocked discards the oldest queued row and fires the onOverflow
// callback. The caller must hold b.mu.
func (b *Buffer) dropOldestLocked() {
	b.storage[b.head] = nil
	b.head = (b.head + 1) % b.capacity
	b.count--
	if b.onOverflow != nil {
		b.onOverflow()
	}
}

// Drain removes up to n rows from the head of the buffer and returns them in
// FIFO order. If the buffer holds fewer than n rows, all queued rows are
// returned. Drain on an empty buffer returns nil.
//
// Drain wakes any goroutine blocked in Append (OverflowBlock mode) so it can
// re-check capacity.
func (b *Buffer) Drain(n int) []any {
	if n <= 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.count == 0 {
		return nil
	}
	if n > b.count {
		n = b.count
	}
	out := make([]any, n)
	for i := 0; i < n; i++ {
		out[i] = b.storage[b.head]
		b.storage[b.head] = nil
		b.head = (b.head + 1) % b.capacity
	}
	b.count -= n
	b.cond.Broadcast()
	return out
}

// Len returns the number of rows currently buffered.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

// Cap returns the buffer's fixed capacity.
func (b *Buffer) Cap() int {
	return b.capacity
}

// Mode returns the overflow policy the buffer was constructed with.
func (b *Buffer) Mode() OverflowMode {
	return b.mode
}
