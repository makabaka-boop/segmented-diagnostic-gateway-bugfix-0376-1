package diaggate

import (
	"sort"
	"sync"
	"time"
)

// Clock and Timer allow deterministic tests.  Production code uses RealClock;
// tests use FakeClock and explicitly advance time.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

type Timer interface {
	Chan() <-chan time.Time
	Stop() bool
	Reset(d time.Duration) bool
}

type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (t *realTimer) Chan() <-chan time.Time     { return t.t.C }
func (t *realTimer) Stop() bool                 { return t.t.Stop() }
func (t *realTimer) Reset(d time.Duration) bool { return t.t.Reset(d) }

type fakeTimer struct {
	c        chan time.Time
	clock    *FakeClock
	mu       sync.Mutex
	deadline time.Time
	stopped  bool
	fired    bool
}

func (t *fakeTimer) Chan() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	active := !t.stopped && !t.fired
	if active {
		delete(t.clock.timers, t)
	}
	t.stopped = true
	return active
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	if d < 0 {
		d = 0
	}
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	active := !t.stopped && !t.fired
	if active {
		delete(t.clock.timers, t)
	}
	t.fired = false
	t.stopped = false
	t.deadline = t.clock.now.Add(d)
	t.clock.timers[t] = t.deadline
	return active
}

func (t *fakeTimer) fireLocked(now time.Time) bool {
	if t.stopped || t.fired {
		return false
	}
	t.fired = true
	t.stopped = true
	delete(t.clock.timers, t)
	select {
	case t.c <- now:
	default:
	}
	return true
}

// FakeClock is a manually advanced clock.  Advance fires all timers whose
// deadline is reached, including timers created or reset while earlier timers
// at the same instant fire.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]time.Time
}

func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Unix(1_700_000_000, 0)
	}
	return &FakeClock{now: start, timers: make(map[*fakeTimer]time.Time)}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// WaitForTimers blocks (using real time) until at least n timers are armed.
// It lets tests advance the clock only after the code under test has
// registered its deadline, avoiding an advance-before-register race.
func (c *FakeClock) WaitForTimers(n int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		count := len(c.timers)
		c.mu.Unlock()
		if count >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func (c *FakeClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if d < 0 {
		d = 0
	}
	t := &fakeTimer{c: make(chan time.Time, 1), clock: c, deadline: c.now.Add(d), stopped: true}
	// A newly created timer starts active.  Initialize stopped=false after
	// constructing it so zero-duration timers fire on the next Advance.
	t.stopped = false
	c.timers[t] = t.deadline
	return t
}

func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	target := c.now.Add(d)
	c.now = target

	for {
		type due struct {
			t  *fakeTimer
			at time.Time
		}
		var pending []due
		for t, deadline := range c.timers {
			if !deadline.After(target) {
				pending = append(pending, due{t, deadline})
			}
		}
		if len(pending) == 0 {
			break
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].at.Before(pending[j].at) })
		firedAt := pending[0].at
		if firedAt.Before(c.now) {
			c.now = firedAt
		}
		for _, item := range pending {
			if !item.at.Equal(firedAt) {
				continue
			}
			item.t.fireLocked(c.now)
		}
	}
	c.mu.Unlock()

	// Give goroutines receiving timer events a deterministic chance to run.
	time.Sleep(time.Millisecond)
}
