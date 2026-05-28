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

package clickhouse

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-gpdb/yagpcc/internal/config"
)

func TestParseOverflowMode(t *testing.T) {
	cases := []struct {
		in      string
		want    OverflowMode
		wantErr bool
	}{
		{config.OnBufferOverflowDropOldest, OverflowDropOldest, false},
		{config.OnBufferOverflowBlock, OverflowBlock, false},
		{"", 0, true},
		{"DROP_OLDEST", 0, true},
	}
	for _, tc := range cases {
		got, err := ParseOverflowMode(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseOverflowMode(%q): expected error, got nil", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseOverflowMode(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("ParseOverflowMode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestOverflowModeString(t *testing.T) {
	if OverflowDropOldest.String() != config.OnBufferOverflowDropOldest {
		t.Errorf("OverflowDropOldest.String() = %q", OverflowDropOldest.String())
	}
	if OverflowBlock.String() != config.OnBufferOverflowBlock {
		t.Errorf("OverflowBlock.String() = %q", OverflowBlock.String())
	}
	if got := OverflowMode(99).String(); got != "unknown(99)" {
		t.Errorf("unknown mode string = %q", got)
	}
}

func TestNewBuffer_Validation(t *testing.T) {
	if _, err := NewBuffer(0, OverflowDropOldest, nil); err == nil {
		t.Error("expected error for zero capacity")
	}
	if _, err := NewBuffer(-1, OverflowDropOldest, nil); err == nil {
		t.Error("expected error for negative capacity")
	}
	if _, err := NewBuffer(10, OverflowMode(42), nil); err == nil {
		t.Error("expected error for invalid mode")
	}
}

func TestBuffer_ModeReturnsConstructorValue(t *testing.T) {
	b1, err := NewBuffer(4, OverflowDropOldest, nil)
	if err != nil {
		t.Fatalf("NewBuffer drop_oldest: %v", err)
	}
	if got := b1.Mode(); got != OverflowDropOldest {
		t.Errorf("Mode() = %v, want OverflowDropOldest", got)
	}
	b2, err := NewBuffer(4, OverflowBlock, nil)
	if err != nil {
		t.Fatalf("NewBuffer block: %v", err)
	}
	if got := b2.Mode(); got != OverflowBlock {
		t.Errorf("Mode() = %v, want OverflowBlock", got)
	}
}

func TestBuffer_AppendAndDrainFIFO(t *testing.T) {
	b, err := NewBuffer(8, OverflowDropOldest, nil)
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	for i := 0; i < 5; i++ {
		b.Append(i)
	}
	if got := b.Len(); got != 5 {
		t.Fatalf("Len = %d, want 5", got)
	}
	if got := b.Cap(); got != 8 {
		t.Fatalf("Cap = %d, want 8", got)
	}

	out := b.Drain(3)
	if len(out) != 3 {
		t.Fatalf("Drain(3) returned %d items", len(out))
	}
	for i := 0; i < 3; i++ {
		if out[i] != i {
			t.Errorf("Drain[%d] = %v, want %d", i, out[i], i)
		}
	}
	if got := b.Len(); got != 2 {
		t.Errorf("Len after drain = %d, want 2", got)
	}
}

func TestBuffer_DrainEmptyReturnsNil(t *testing.T) {
	b, _ := NewBuffer(4, OverflowDropOldest, nil)
	if out := b.Drain(10); out != nil {
		t.Errorf("Drain on empty: got %v, want nil", out)
	}
}

func TestBuffer_DrainZeroOrNegativeReturnsNil(t *testing.T) {
	b, _ := NewBuffer(4, OverflowDropOldest, nil)
	b.Append("x")
	if out := b.Drain(0); out != nil {
		t.Errorf("Drain(0): got %v, want nil", out)
	}
	if out := b.Drain(-1); out != nil {
		t.Errorf("Drain(-1): got %v, want nil", out)
	}
	if b.Len() != 1 {
		t.Errorf("Len after no-op drain = %d, want 1", b.Len())
	}
}

func TestBuffer_DrainMoreThanLen(t *testing.T) {
	b, _ := NewBuffer(4, OverflowDropOldest, nil)
	b.Append("a")
	b.Append("b")
	out := b.Drain(10)
	if len(out) != 2 || out[0] != "a" || out[1] != "b" {
		t.Errorf("Drain: got %v, want [a b]", out)
	}
	if b.Len() != 0 {
		t.Errorf("Len after full drain = %d, want 0", b.Len())
	}
}

func TestBuffer_DropOldestEvictsAndCallsCallback(t *testing.T) {
	var dropped int64
	b, err := NewBuffer(3, OverflowDropOldest, func() {
		atomic.AddInt64(&dropped, 1)
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	b.Append("a")
	b.Append("b")
	b.Append("c") // full
	b.Append("d") // evicts "a"
	b.Append("e") // evicts "b"

	if got := atomic.LoadInt64(&dropped); got != 2 {
		t.Errorf("dropped = %d, want 2", got)
	}
	if b.Len() != 3 {
		t.Errorf("Len = %d, want 3", b.Len())
	}
	out := b.Drain(3)
	want := []any{"c", "d", "e"}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("Drain[%d] = %v, want %v", i, out[i], want[i])
		}
	}
}

func TestBuffer_DropOldestNilCallbackOK(t *testing.T) {
	b, _ := NewBuffer(2, OverflowDropOldest, nil)
	b.Append(1)
	b.Append(2)
	b.Append(3) // would normally invoke nil callback
	if b.Len() != 2 {
		t.Errorf("Len = %d, want 2", b.Len())
	}
}

func TestBuffer_BlockUnblocksOnDrain(t *testing.T) {
	b, _ := NewBuffer(2, OverflowBlock, nil)
	b.Append(1)
	b.Append(2)

	done := make(chan struct{})
	go func() {
		b.Append(3) // should block until Drain frees a slot
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Append returned before Drain made room")
	case <-time.After(50 * time.Millisecond):
	}

	if got := b.Drain(1); len(got) != 1 || got[0] != 1 {
		t.Errorf("Drain: %v, want [1]", got)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Append did not return after Drain freed a slot")
	}

	if b.Len() != 2 {
		t.Errorf("Len after re-fill = %d, want 2", b.Len())
	}
}

func TestBuffer_Close_ReleasesBlockedAppenders(t *testing.T) {
	// OverflowBlock + full buffer + Close should release the blocked Append
	// goroutines without deadlocking. Without Close they would wait forever
	// for a Drain that never happens during shutdown.
	var dropped int64
	b, err := NewBuffer(1, OverflowBlock, func() {
		atomic.AddInt64(&dropped, 1)
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	b.Append(0)

	const blocked = 3
	done := make(chan struct{}, blocked)
	for i := 0; i < blocked; i++ {
		go func(v int) {
			b.Append(v)
			done <- struct{}{}
		}(i + 1)
	}

	// Give the goroutines a chance to enter cond.Wait.
	time.Sleep(10 * time.Millisecond)

	b.Close()

	for i := 0; i < blocked; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("blocked Append #%d did not return after Close", i)
		}
	}
	if got := atomic.LoadInt64(&dropped); got != int64(blocked) {
		t.Errorf("dropped count = %d, want %d (one per blocked appender)", got, blocked)
	}
	// Already-queued row is still drainable; new rows are dropped.
	if got := b.Drain(10); len(got) != 1 || got[0] != 0 {
		t.Errorf("Drain after Close: %v, want [0]", got)
	}
	b.Append(99)
	if got := atomic.LoadInt64(&dropped); got != int64(blocked+1) {
		t.Errorf("Append after Close did not increment drop count: %d", got)
	}
	// Close is idempotent.
	b.Close()
}

func TestBuffer_BlockMultipleAppendersWakeFairly(t *testing.T) {
	b, _ := NewBuffer(1, OverflowBlock, nil)
	b.Append(0)

	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 1; i <= n; i++ {
		i := i
		go func() {
			defer wg.Done()
			b.Append(i)
		}()
	}

	got := []any{}
	for i := 0; i <= n; i++ {
		// Drain one at a time to release one blocked appender per cycle.
		for {
			out := b.Drain(1)
			if len(out) == 1 {
				got = append(got, out[0])
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	wg.Wait()

	if len(got) != n+1 {
		t.Fatalf("collected %d, want %d", len(got), n+1)
	}
	// Order between blocked appenders is not guaranteed, but every value 0..n
	// must appear exactly once.
	seen := make(map[int]bool, n+1)
	for _, v := range got {
		seen[v.(int)] = true
	}
	for i := 0; i <= n; i++ {
		if !seen[i] {
			t.Errorf("missing value %d in drained set %v", i, got)
		}
	}
}

func TestBuffer_RingWrapAround(t *testing.T) {
	// Exercise head/tail wrapping by repeatedly appending and draining past
	// capacity boundaries.
	b, _ := NewBuffer(4, OverflowDropOldest, nil)
	for i := 0; i < 4; i++ {
		b.Append(i)
	}
	_ = b.Drain(2) // head moves to 2
	b.Append(100)  // tail wraps
	b.Append(101)  // tail at 2

	out := b.Drain(4)
	want := []any{2, 3, 100, 101}
	if len(out) != len(want) {
		t.Fatalf("Drain returned %d, want %d", len(out), len(want))
	}
	for i := range want {
		if out[i] != want[i] {
			t.Errorf("Drain[%d] = %v, want %v", i, out[i], want[i])
		}
	}
}

func TestBuffer_ConcurrentAppendDrain(t *testing.T) {
	const (
		producers   = 4
		perProducer = 500
		capacity    = 64
	)
	var dropped int64
	b, _ := NewBuffer(capacity, OverflowDropOldest, func() {
		atomic.AddInt64(&dropped, 1)
	})

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				b.Append([2]int{p, i})
			}
		}(p)
	}

	var consumed int64
	stop := make(chan struct{})
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			out := b.Drain(16)
			atomic.AddInt64(&consumed, int64(len(out)))
			if len(out) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Wait()
	// Drain anything still in the buffer before signalling the consumer to
	// stop.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&consumed)+atomic.LoadInt64(&dropped) >= producers*perProducer {
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	<-consumerDone

	consumedFinal := atomic.LoadInt64(&consumed)
	droppedFinal := atomic.LoadInt64(&dropped)
	total := consumedFinal + droppedFinal + int64(b.Len())
	if total != producers*perProducer {
		t.Errorf("accounting mismatch: consumed=%d dropped=%d remaining=%d total=%d want=%d",
			consumedFinal, droppedFinal, b.Len(), total, producers*perProducer)
	}
}
