package output

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"syscall"
	"time"

	librespot "github.com/devgianlu/go-librespot"
)

// pipeReopenInterval is how often the output looks for a reader while none
// has the FIFO open.
const pipeReopenInterval = 100 * time.Millisecond

// pipeOutput writes PCM to a named pipe. The process on the other end is
// typically an encoder supervised separately, so it can be restarted or
// crash independently of us. Losing the reader must not end playback: the
// output holds the audio source where it is and reattaches to the pipe as
// soon as a reader is back, so a reader restart is heard as a pause rather
// than as the track (and the session) stopping.
type pipeOutput struct {
	log    librespot.Logger
	reader librespot.Float32Reader
	path   string

	// file is nil while no reader has the pipe open.
	file *os.File

	lock sync.Mutex
	cond *sync.Cond

	externalVolume bool

	volume float32
	paused bool
	closed bool

	volumeUpdate chan float32
	err          chan error

	transform func([]float32, []byte) int
}

func newPipeOutput(opts *NewOutputOptions) (out *pipeOutput, err error) {
	out = &pipeOutput{
		log:            opts.Log,
		reader:         opts.Reader,
		path:           opts.OutputPipe,
		volume:         opts.InitialVolume,
		err:            make(chan error, 2),
		externalVolume: opts.ExternalVolume,
		volumeUpdate:   opts.VolumeUpdate,
	}

	out.cond = sync.NewCond(&out.lock)

	switch opts.OutputPipeFormat {
	case "s16le":
		out.transform = func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := int16(in[i] * 32768)
				binary.LittleEndian.PutUint16(out[i*2:], uint16(sample))
			}
			return len(in) * 2
		}
	case "s32le":
		out.transform = func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := int32(in[i] * 2147483648)
				binary.LittleEndian.PutUint32(out[i*4:], uint32(sample))
			}
			return len(in) * 4
		}
	case "f32le":
		out.transform = func(in []float32, out []byte) int {
			for i := 0; i < len(in); i++ {
				sample := math.Float32bits(in[i])
				binary.LittleEndian.PutUint32(out[i*4:], sample)
			}
			return len(in) * 4
		}
	default:
		return nil, fmt.Errorf("unknown output pipe format: %s", opts.OutputPipeFormat)
	}

	// Attach now so configuration mistakes (missing pipe, permissions) fail
	// the request loudly. A pipe that merely has no reader yet is fine: the
	// output loop keeps trying until one shows up.
	if err := out.open(); err != nil && !errors.Is(err, syscall.ENXIO) {
		return nil, fmt.Errorf("failed to open fifo: %w", err)
	}

	go out.outputLoop()

	return out, nil
}

// open attaches to the pipe. It returns ENXIO when no process has the pipe
// open for reading; any other error is a real failure.
func (out *pipeOutput) open() error {
	// Open non-blocking so a missing reader is reported instead of hanging.
	f, err := os.OpenFile(out.path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}

	// Restore blocking mode now that we are sure we have a reader: writes
	// then pace against the reader instead of failing with EAGAIN.
	if err := syscall.SetNonblock(int(f.Fd()), false); err != nil {
		_ = f.Close()
		return err
	}

	out.file = f
	return nil
}

// write delivers one chunk to the reader. While no reader has the pipe open
// it waits, reattaching as soon as one appears, so audio pauses instead of
// playback ending. A chunk interrupted by the reader going away is written
// again to the next reader. Must be called with out.lock held; the lock is
// released while waiting so Pause and Close stay responsive.
func (out *pipeOutput) write(b []byte) error {
	for !out.closed {
		if out.file == nil {
			if err := out.open(); err != nil {
				if !errors.Is(err, syscall.ENXIO) {
					return err
				}

				out.lock.Unlock()
				time.Sleep(pipeReopenInterval)
				out.lock.Lock()
				continue
			}

			out.log.Infof("output pipe reader attached")
		}

		_, err := out.file.Write(b)
		if err == nil {
			return nil
		} else if !errors.Is(err, syscall.EPIPE) {
			return err
		}

		// The reader went away mid-stream. Drop the dead descriptor and
		// wait for the next one.
		out.log.Warnf("output pipe reader gone, waiting for a new one")
		_ = out.file.Close()
		out.file = nil
	}

	return nil
}

// fail reports a fatal error and shuts the output down. Must be called with
// out.lock held.
func (out *pipeOutput) fail(err error) {
	out.err <- err
	out.closed = true

	if out.file != nil {
		_ = out.file.Close()
		out.file = nil
	}

	out.cond.Signal()
}

func (out *pipeOutput) outputLoop() {
	floats := make([]float32, 4*1024)
	bytes := make([]byte, 4*len(floats)) // times four is the biggest we can get

	for {
		out.lock.Lock()

		for out.paused && !out.closed {
			out.cond.Wait()
		}

		if out.closed {
			out.lock.Unlock()
			break
		}

		n, err := out.reader.Read(floats)

		// Apply volume.
		if !out.externalVolume {
			// Map volume (in percent) to what is perceived as linear by
			// humans. This is the same as math.Pow(out.volume, 2) but simpler.
			volume := out.volume * out.volume

			for i := 0; i < n; i++ {
				floats[i] *= volume
			}
		}

		if n > 0 {
			nn := out.transform(floats[:n], bytes)
			if werr := out.write(bytes[:nn]); werr != nil {
				out.fail(werr)
				out.lock.Unlock()
				break
			}
		}

		if errors.Is(err, io.EOF) {
			// Reached EOF, move to a "paused" state.
			out.paused = true
		} else if err != nil {
			// Got some other error. Close the output and report the error.
			out.fail(err)
			out.lock.Unlock()
			break
		}

		out.lock.Unlock()
	}

	_ = out.Close()
}

func (out *pipeOutput) Pause() error {
	out.lock.Lock()
	defer out.lock.Unlock()

	if out.closed {
		return nil
	}

	out.paused = true
	out.cond.Signal()
	return nil
}

func (out *pipeOutput) Resume() error {
	out.lock.Lock()
	defer out.lock.Unlock()

	if out.closed {
		return nil
	}

	out.paused = false
	out.cond.Signal()
	return nil
}

func (out *pipeOutput) Drop() error {
	return nil
}

func (out *pipeOutput) DelayMs() (int64, error) {
	return 0, nil
}

func (out *pipeOutput) SetVolume(vol float32) {
	if vol < 0 || vol > 1 {
		panic(fmt.Sprintf("invalid volume value: %0.2f", vol))
	}

	out.volume = vol
	sendVolumeUpdate(out.volumeUpdate, vol)
}

func (out *pipeOutput) Error() <-chan error {
	// No need to lock here (out.err is only set in newOutput).
	return out.err
}

func (out *pipeOutput) Close() error {
	out.lock.Lock()
	defer out.lock.Unlock()

	if out.closed {
		return nil
	}

	if out.file != nil {
		_ = out.file.Close()
		out.file = nil
	}

	out.closed = true
	out.cond.Signal()

	return nil
}
