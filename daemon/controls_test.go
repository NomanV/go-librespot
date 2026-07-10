package daemon

import (
	"testing"
	"time"
)

func TestUnplayableSkipDelay(t *testing.T) {
	cases := []struct {
		skips int
		want  time.Duration
	}{
		{1, 0},
		{2, 0},
		{3, 10 * time.Second},
		{4, 10 * time.Second},
		{5, 30 * time.Second},
		{6, 30 * time.Second},
		{7, time.Minute},
		{25, time.Minute},
		{maxConsecutiveUnplayableSkips, time.Minute},
	}
	for _, c := range cases {
		if got := unplayableSkipDelay(c.skips); got != c.want {
			t.Errorf("unplayableSkipDelay(%d) = %s, want %s", c.skips, got, c.want)
		}
	}
}

// A full run of key refusals must be dominated by waiting, not requests: the
// 2026-07-10 incident was 51 key requests in ~90 seconds. With pacing, walking
// the full cap of 50 skips accumulates over 40 minutes of delay, i.e. roughly
// one key request per minute at the steady state instead of one per ~2s.
func TestUnplayableSkipRunIsPaced(t *testing.T) {
	var total time.Duration
	for n := 1; n <= maxConsecutiveUnplayableSkips; n++ {
		total += unplayableSkipDelay(n)
	}
	if total < 40*time.Minute {
		t.Errorf("total delay across a full run = %s, want >= 40m", total)
	}
}
