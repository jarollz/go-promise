package gopromise

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCallAndCallContextInputValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tests := []struct {
		name string
		call func() Promise[int]
		want string
	}{
		{
			name: "call nil producer",
			call: func() Promise[int] {
				var producer ProducerFunc[int]
				return Call(producer)
			},
			want: "nil producer",
		},
		{
			name: "callcontext nil context",
			call: func() Promise[int] {
				return CallContext[int](nil, func(context.Context) (int, error) {
					return 1, nil
				})
			},
			want: "nil context",
		},
		{
			name: "callcontext nil producer",
			call: func() Promise[int] {
				var producer ProducerContextFunc[int]
				return CallContext(ctx, producer)
			},
			want: "nil producer",
		},
		{
			name: "callcontext nil context and producer",
			call: func() Promise[int] {
				var producer ProducerContextFunc[int]
				return CallContext[int](nil, producer)
			},
			want: "nil context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := tt.call()
			if p == nil {
				t.Fatal("expected non-nil promise")
			}

			v, err := p.Resolve()
			if err == nil || err.Error() != tt.want {
				t.Fatalf("expected error %q, got %v", tt.want, err)
			}
			if v != 0 {
				t.Fatalf("expected zero value, got %d", v)
			}
		})
	}
}

func TestResolveAndResolveContextGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func() (int, error)
		want string
	}{
		{
			name: "resolve nil receiver",
			call: func() (int, error) {
				var p *promiseImpl[int]
				return p.Resolve()
			},
			want: "empty promise",
		},
		{
			name: "resolvecontext nil receiver",
			call: func() (int, error) {
				var p *promiseImpl[int]
				return p.ResolveContext(context.Background())
			},
			want: "empty promise",
		},
		{
			name: "resolve uninitialized promise",
			call: func() (int, error) {
				p := &promiseImpl[int]{}
				return p.Resolve()
			},
			want: "uninitialized promise",
		},
		{
			name: "resolvecontext uninitialized promise",
			call: func() (int, error) {
				p := &promiseImpl[int]{}
				return p.ResolveContext(context.Background())
			},
			want: "uninitialized promise",
		},
		{
			name: "resolvecontext nil context",
			call: func() (int, error) {
				p := Call(func() (int, error) {
					return 1, nil
				})
				return p.ResolveContext(nil)
			},
			want: "nil context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, err := tt.call()
			if err == nil || err.Error() != tt.want {
				t.Fatalf("expected error %q, got %v", tt.want, err)
			}
			if v != 0 {
				t.Fatalf("expected zero value, got %d", v)
			}
		})
	}
}

func TestProducerFailureModes(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("producer failed")
	tests := []struct {
		name   string
		call   func() Promise[int]
		panic  bool
		errChk func(error) bool
	}{
		{
			name: "call producer returns error",
			call: func() Promise[int] {
				return Call(func() (int, error) {
					return 0, expectedErr
				})
			},
			errChk: func(err error) bool { return errors.Is(err, expectedErr) },
		},
		{
			name: "callcontext producer returns error",
			call: func() Promise[int] {
				return CallContext(context.Background(), func(context.Context) (int, error) {
					return 0, expectedErr
				})
			},
			errChk: func(err error) bool { return errors.Is(err, expectedErr) },
		},
		{
			name: "call producer panic",
			call: func() Promise[int] {
				return Call(func() (int, error) {
					panic("boom")
				})
			},
			errChk: func(err error) bool { return err != nil && strings.Contains(err.Error(), "producer panic: boom") },
		},
		{
			name: "callcontext producer panic",
			call: func() Promise[int] {
				return CallContext(context.Background(), func(context.Context) (int, error) {
					panic("boom")
				})
			},
			errChk: func(err error) bool { return err != nil && strings.Contains(err.Error(), "producer panic: boom") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := tt.call()
			v, err := p.Resolve()
			if !tt.errChk(err) {
				t.Fatalf("unexpected error: %v", err)
			}
			if v != 0 {
				t.Fatalf("expected zero value, got %d", v)
			}
		})
	}
}

func TestResolveContextCancellationOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resolve  func(Promise[int]) (int, error)
		wantErr  error
		wantVal  int
		producer func() (int, error)
	}{
		{
			name: "timeout before done",
			producer: func() (int, error) {
				time.Sleep(120 * time.Millisecond)
				return 9, nil
			},
			resolve: func(p Promise[int]) (int, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				return p.ResolveContext(ctx)
			},
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "manual cancel before done",
			producer: func() (int, error) {
				time.Sleep(120 * time.Millisecond)
				return 9, nil
			},
			resolve: func(p Promise[int]) (int, error) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return p.ResolveContext(ctx)
			},
			wantErr: context.Canceled,
		},
		{
			name: "done before timeout",
			producer: func() (int, error) {
				return 11, nil
			},
			resolve: func(p Promise[int]) (int, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cancel()
				return p.ResolveContext(ctx)
			},
			wantVal: 11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := Call(tt.producer)
			v, err := tt.resolve(p)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("expected %v, got %v", tt.wantErr, err)
				}
				if v != 0 {
					t.Fatalf("expected zero value, got %d", v)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if v != tt.wantVal {
				t.Fatalf("expected %d, got %d", tt.wantVal, v)
			}
		})
	}
}

func TestCallAndCallContextValuePaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func() Promise[int]
		want int
	}{
		{
			name: "call value",
			call: func() Promise[int] {
				return Call(func() (int, error) {
					return 42, nil
				})
			},
			want: 42,
		},
		{
			name: "callcontext value",
			call: func() Promise[int] {
				return CallContext(context.Background(), func(context.Context) (int, error) {
					return 42, nil
				})
			},
			want: 42,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := tt.call()
			v, err := p.Resolve()
			if err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
			if v != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, v)
			}
		})
	}
}

func TestResolveMultipleCallsSameResultNoBlock(t *testing.T) {
	t.Parallel()

	p := Call(func() (int, error) {
		return 7, nil
	})

	v1, err1 := p.Resolve()
	if err1 != nil || v1 != 7 {
		t.Fatalf("first resolve expected (7, nil), got (%d, %v)", v1, err1)
	}

	done := make(chan struct{})
	var v2 int
	var err2 error
	go func() {
		defer close(done)
		v2, err2 = p.ResolveContext(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second resolve blocked")
	}

	if err2 != nil || v2 != 7 {
		t.Fatalf("second resolve expected (7, nil), got (%d, %v)", v2, err2)
	}
}

func TestResolveAndResolveContextConcurrentMixedCallers(t *testing.T) {
	t.Parallel()

	p := Call(func() (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 99, nil
	})

	const callers = 40
	vals := make([]int, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				vals[idx], errs[idx] = p.Resolve()
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			vals[idx], errs[idx] = p.ResolveContext(ctx)
		}(i)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent callers blocked")
	}

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d expected nil error, got %v", i, errs[i])
		}
		if vals[i] != 99 {
			t.Fatalf("caller %d expected 99, got %d", i, vals[i])
		}
	}
}

func TestResolveContextTimeoutThenLaterResolveSucceeds(t *testing.T) {
	t.Parallel()

	p := Call(func() (int, error) {
		time.Sleep(80 * time.Millisecond)
		return 5, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	v, err := p.ResolveContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if v != 0 {
		t.Fatalf("expected zero value, got %d", v)
	}

	v2, err2 := p.Resolve()
	if err2 != nil {
		t.Fatalf("expected nil error, got %v", err2)
	}
	if v2 != 5 {
		t.Fatalf("expected 5, got %d", v2)
	}
}

func TestCallContextProducerGetsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})

	p := CallContext(ctx, func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		return 0, ctx.Err()
	})

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("producer did not start")
	}

	cancel()

	v, err := p.Resolve()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if v != 0 {
		t.Fatalf("expected zero value, got %d", v)
	}
}

func TestResolveWaitsUntilDone(t *testing.T) {
	t.Parallel()

	p := Call(func() (int, error) {
		time.Sleep(120 * time.Millisecond)
		return 1, nil
	})

	start := time.Now()
	v, err := p.Resolve()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("resolve returned too early, elapsed %v", elapsed)
	}
}
