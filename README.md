# go-promise

`go-promise` is tiny Go library that runs producer asynchronously and lets you resolve same result safely from anywhere, including multiple goroutines.

## Why this is useful

Go has goroutines/channels, but app code often repeats boilerplate for:

- running work in background,
- waiting for completion in many places,
- returning exactly one shared result/error,
- handling panics from background work safely.

This library gives you a focused abstraction for that pattern.

It also supports context-aware producer execution and context-aware waiting.

## Features

- Start work immediately with `Call` (non-blocking).
- Start context-aware work with `CallContext`.
- Resolve result with `p.Resolve()`.
- Resolve with wait control using `p.ResolveContext(ctx)`.
- Resolve many times; every call returns same cached value/error.
- Resolve concurrently from multiple goroutines safely.
- Safely mix `Resolve()` and `ResolveContext(ctx)` across goroutines on same promise.
- `ResolveContext` caller timeouts do not affect other callers or final cached result.
- Panic in producer is recovered and returned as error.
- Nil producer returns deterministic error (`nil producer`).
- Nil context returns deterministic error (`nil context`).

## Installation

```bash
go get github.com/jarollz/go-promise
```

## Quick start

This snippet is also executable as doc example in `example_test.go` (`ExampleCall`).

```go
package main

import (
	"fmt"
	"time"

	gopromise "github.com/jarollz/go-promise"
)

func main() {
	p := gopromise.Call(func() (string, error) {
		time.Sleep(200 * time.Millisecond)
		return "done", nil
	})

	// do other work...
	value, err := p.Resolve()
	fmt.Println(value, err)
}
```

## Concurrent resolve example

This snippet is also executable as doc example in `example_test.go` (`ExampleCall_concurrentResolve`).

```go
p := gopromise.Call(func() (int, error) {
	return 42, nil
})

go func() {
	v, err := p.Resolve()
	_ = v
	_ = err
}()

v, err := p.Resolve()
_ = v
_ = err
```

Both callers receive the same `(42, nil)` result.

You can also safely mix callers that use `Resolve()` with callers that use `ResolveContext(ctx)` on the same promise.
If some `ResolveContext` callers time out, other callers can still resolve successfully, and later `Resolve()` still returns the same cached producer result.

## Context-aware producer (`CallContext`)

This snippet is also executable as doc example in `example_test.go` (`ExampleCallContext`).

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

p := gopromise.CallContext(ctx, func(ctx context.Context) (string, error) {
	select {
	case <-time.After(50 * time.Millisecond):
		return "ok", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
})

v, err := p.Resolve()
_ = v
_ = err
```

`CallContext` passes context into producer. Producer must cooperate by checking `ctx.Done()`.

## Context-aware waiting (`ResolveContext`)

This snippet is also executable as doc example in `example_test.go` (`ExamplePromise_ResolveContext`).

```go
p := gopromise.Call(func() (int, error) {
	time.Sleep(100 * time.Millisecond)
	return 5, nil
})

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
defer cancel()

_, err := p.ResolveContext(ctx) // context deadline exceeded
v, err2 := p.Resolve()          // later succeeds with cached result
_ = err
_ = v
_ = err2
```

`ResolveContext` controls only caller wait. Promise computation can still finish later.

## Error behavior

- `Call(nil)` resolves with `nil producer`.
- `CallContext(nil, producer)` resolves with `nil context`.
- `ResolveContext(nil)` returns `nil context`.
- panic inside producer resolves with `producer panic: <panic value>`.
- internal nil receiver guard returns `empty promise`.
- internal uninitialized guard returns `uninitialized promise`.

## API overview

```go
type ProducerFunc[T any] func() (T, error)
type ProducerContextFunc[T any] func(context.Context) (T, error)

type Promise[T any] interface {
	Resolve() (T, error)
	ResolveContext(ctx context.Context) (T, error)
}

func Call[T any](producer ProducerFunc[T]) Promise[T]
func CallContext[T any](ctx context.Context, producer ProducerContextFunc[T]) Promise[T]
```

## Testing

Race detection is mandatory in this repo.

Doc examples run as part of normal `go test` via `example_test.go`.

```bash
make test
```

This runs:

```bash
go test -race ./...
```

## CI

GitHub Actions workflow at `.github/workflows/ci.yml` runs `make test` on push and pull request.

## Project notes

- Concrete promise implementation is intentionally unexported to protect invariants.
- Exported `Promise[T]` interface makes integration and mocking easier for library users.
