package output

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// counterReader emits an endless ramp of samples so a consumer can check
// that what it receives is contiguous.
type counterReader struct {
	next  int32
	reads atomic.Int64
}

const counterPeriod = 30000

func (r *counterReader) Read(p []float32) (int, error) {
	r.reads.Add(1)
	for i := range p {
		p[i] = float32(r.next) / 32768
		r.next = (r.next + 1) % counterPeriod
	}
	// Keep the ramp slow enough that the test can interleave reader
	// restarts with writes without racing through the kernel buffer.
	time.Sleep(time.Millisecond)
	return len(p), nil
}

func newTestPipe(t *testing.T) (string, *counterReader, *pipeOutput) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "out.fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("mkfifo: %v", err)
	}

	reader := &counterReader{}
	out, err := newPipeOutput(&NewOutputOptions{
		Log:              &librespot.NullLogger{},
		Reader:           reader,
		OutputPipe:       path,
		OutputPipeFormat: "s16le",
		InitialVolume:    1,
		ExternalVolume:   true,
	})
	if err != nil {
		t.Fatalf("newPipeOutput: %v", err)
	}
	t.Cleanup(func() { _ = out.Close() })

	return path, reader, out
}

// readSamples opens the pipe for reading and returns the first n decoded
// samples, then closes the pipe (as a crashing or restarted consumer would).
func readSamples(t *testing.T, path string, n int) []int16 {
	t.Helper()

	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer f.Close()

	buf := make([]byte, n*2)
	if _, err := io.ReadFull(f, buf); err != nil {
		t.Fatalf("read: %v", err)
	}

	samples := make([]int16, n)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(buf[i*2:]))
	}
	return samples
}

func assertContiguous(t *testing.T, samples []int16) {
	t.Helper()
	for i := 1; i < len(samples); i++ {
		want := int16((int32(samples[i-1]) + 1) % counterPeriod)
		if samples[i] != want {
			t.Fatalf("sample %d: got %d, want %d (gap in stream)", i, samples[i], want)
		}
	}
}

func assertNoError(t *testing.T, out *pipeOutput) {
	t.Helper()
	select {
	case err := <-out.Error():
		t.Fatalf("output reported an error: %v", err)
	default:
	}
}

func TestPipeOutputWaitsForFirstReader(t *testing.T) {
	path, reader, out := newTestPipe(t)

	// No reader yet: the output must not fail and must not chew through
	// the source while nobody is listening.
	time.Sleep(300 * time.Millisecond)
	assertNoError(t, out)
	if reads := reader.reads.Load(); reads > 1 {
		t.Fatalf("source consumed %d chunks with no reader attached", reads)
	}

	samples := readSamples(t, path, 8192)
	assertContiguous(t, samples)
	assertNoError(t, out)
}

func TestPipeOutputSurvivesReaderRestart(t *testing.T) {
	path, reader, out := newTestPipe(t)

	first := readSamples(t, path, 8192)
	assertContiguous(t, first)

	// Reader is gone. The output must notice the broken pipe, stay alive,
	// and stop pulling from the source instead of racing ahead.
	time.Sleep(300 * time.Millisecond)
	assertNoError(t, out)
	readsWhileGone := reader.reads.Load()
	time.Sleep(300 * time.Millisecond)
	if delta := reader.reads.Load() - readsWhileGone; delta > 1 {
		t.Fatalf("source consumed %d chunks while the reader was gone", delta)
	}

	second := readSamples(t, path, 8192)
	assertContiguous(t, second)
	assertNoError(t, out)

	// Whatever sat in the kernel buffer when the first reader died is lost,
	// but the resumed stream must pick up from at most one chunk plus that
	// buffer behind: a tiny gap, not a reset of the source.
	last := int32(first[len(first)-1])
	resumed := int32(second[0])
	gap := (resumed - last - 1 + counterPeriod) % counterPeriod
	const maxGap = 4*1024 + 65536/2 // one chunk plus a 64 KiB pipe buffer
	if gap > maxGap {
		t.Fatalf("resumed %d samples after the last delivered one, want at most %d", gap, maxGap)
	}
}

func TestPipeOutputCloseWhileWaitingForReader(t *testing.T) {
	_, reader, out := newTestPipe(t)

	time.Sleep(200 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		_ = out.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked while waiting for a reader")
	}

	// The loop must have exited: no further reads from the source.
	time.Sleep(200 * time.Millisecond)
	reads := reader.reads.Load()
	time.Sleep(300 * time.Millisecond)
	if reader.reads.Load() != reads {
		t.Fatal("output loop kept reading after Close")
	}
}

func TestPipeOutputRejectsMissingPipe(t *testing.T) {
	_, err := newPipeOutput(&NewOutputOptions{
		Log:              &librespot.NullLogger{},
		Reader:           &counterReader{},
		OutputPipe:       filepath.Join(t.TempDir(), "does-not-exist"),
		OutputPipeFormat: "s16le",
		InitialVolume:    1,
	})
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected a not-exist error for a missing pipe, got %v", err)
	}
}
