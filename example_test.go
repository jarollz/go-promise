package gopromise_test

import (
	"context"
	"fmt"
	"time"

	gopromise "github.com/jarollz/go-promise"
)

func ExampleCall() {
	p := gopromise.Call(func() (string, error) {
		return "done", nil
	})

	v, err := p.Resolve()
	fmt.Println(v)
	fmt.Println(err == nil)

	// Output:
	// done
	// true
}

func ExampleCall_concurrentResolve() {
	p := gopromise.Call(func() (int, error) {
		return 42, nil
	})

	left := make(chan int, 1)
	go func() {
		v, _ := p.Resolve()
		left <- v
	}()

	right, _ := p.Resolve()
	fmt.Println(<-left, right)

	// Output:
	// 42 42
}

func ExampleCallContext() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := gopromise.CallContext(ctx, func(ctx context.Context) (string, error) {
		select {
		case <-time.After(10 * time.Millisecond):
			return "done", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})

	v, err := p.Resolve()
	fmt.Println(v)
	fmt.Println(err == nil)

	// Output:
	// done
	// true
}

func ExamplePromise_ResolveContext() {
	p := gopromise.Call(func() (int, error) {
		time.Sleep(60 * time.Millisecond)
		return 5, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	_, err := p.ResolveContext(ctx)
	fmt.Println(err == context.DeadlineExceeded)

	v, err := p.Resolve()
	fmt.Println(v, err == nil)

	// Output:
	// true
	// 5 true
}
