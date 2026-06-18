// Package gopromise provides a small promise-like abstraction for Go.
//
// A producer function starts asynchronously via Call, and callers can safely
// resolve the same result from multiple goroutines by calling Resolve.
//
// For context-aware flows, CallContext lets producer observe cancellation and
// ResolveContext lets callers bound wait time for the same shared result.
package gopromise

import (
	"context"
	"fmt"
)

const errorPrefix = "gopromise: "

// ProducerFunc is function that computes a value or an error.
//
// It runs asynchronously when passed to Call.
type ProducerFunc[T any] func() (T, error)

// ProducerContextFunc is function that computes a value or an error using context.
//
// The provided context comes from CallContext and can be used by producer for
// cooperative cancellation and timeout handling.
type ProducerContextFunc[T any] func(context.Context) (T, error)

// result stores resolved value and error for one promise instance.
type result[T any] struct {
	Value T
	Err   error
}

// Promise represents a single asynchronous computation result.
//
// Resolve blocks until producer finishes, then always returns same cached
// value and error on subsequent calls.
//
// ResolveContext is like Resolve, but caller can stop waiting by canceling
// context or setting a deadline.
//
// Example usage:
//
//	p := gopromise.Call(func() (int, error) {
//		return 42, nil
//	})
//	v, err := p.Resolve()
//	_ = v
//	_ = err
type Promise[T any] interface {
	Resolve() (T, error)
	ResolveContext(ctx context.Context) (T, error)
}

// promiseImpl is internal implementation of Promise.
type promiseImpl[T any] struct {
	done chan struct{}
	res  result[T]
}

// Call starts producer in new goroutine and returns a Promise immediately.
//
// If producer is nil, returned promise resolves with error "gopromise: nil producer".
// If producer panics, panic is recovered and converted to
// "gopromise: producer panic: <panic value>" error.
//
// Example concurrent usage:
//
//	p := gopromise.Call(func() (string, error) {
//		return "ready", nil
//	})
//	go func() {
//		_, _ = p.Resolve()
//	}()
//	_, _ = p.Resolve()
func Call[T any](producer ProducerFunc[T]) Promise[T] {
	if producer == nil {
		promise := &promiseImpl[T]{done: make(chan struct{})}
		promise.res.Err = fmt.Errorf(errorPrefix + "nil producer")
		close(promise.done)
		return promise
	}

	return CallContext(context.Background(), func(context.Context) (T, error) {
		return producer()
	})
}

// CallContext starts producer in new goroutine and returns a Promise immediately.
//
// The producer receives ctx and can observe cancellation via ctx.Done().
// If ctx is nil, returned promise resolves with error "gopromise: nil context".
// If producer is nil, returned promise resolves with error "gopromise: nil producer".
// If producer panics, panic is recovered and converted to
// "gopromise: producer panic: <panic value>" error.
func CallContext[T any](ctx context.Context, producer ProducerContextFunc[T]) Promise[T] {
	promise := &promiseImpl[T]{done: make(chan struct{})}

	if ctx == nil {
		promise.res.Err = fmt.Errorf(errorPrefix + "nil context")
		close(promise.done)
		return promise
	}

	if producer == nil {
		promise.res.Err = fmt.Errorf(errorPrefix + "nil producer")
		close(promise.done)
		return promise
	}

	go func() {
		defer close(promise.done)
		defer func() {
			if recovered := recover(); recovered != nil {
				promise.res.Err = fmt.Errorf(errorPrefix+"producer panic: %v", recovered)
			}
		}()

		value, err := producer(ctx)
		promise.res = result[T]{Value: value, Err: err}
	}()

	return promise
}

// Resolve waits for completion and returns cached result.
//
// Resolve is safe to call many times and from multiple goroutines. Every call
// returns same value/error tuple after producer completes.
//
// Resolve returns "gopromise: empty promise" when receiver is nil and
// "gopromise: uninitialized promise" when internal done channel is nil.
func (promise *promiseImpl[T]) Resolve() (T, error) {
	return promise.ResolveContext(context.Background())
}

// ResolveContext waits for completion and returns cached result.
//
// If ctx is done before promise completes, ResolveContext returns ctx.Err().
// Promise computation still continues and can be resolved later.
//
// ResolveContext returns "gopromise: empty promise" when receiver is nil,
// "gopromise: uninitialized promise" when internal done channel is nil,
// and "gopromise: nil context" when ctx is nil.
func (promise *promiseImpl[T]) ResolveContext(ctx context.Context) (T, error) {
	var nilT T
	if promise == nil {
		return nilT, fmt.Errorf(errorPrefix + "empty promise")
	}
	if promise.done == nil {
		return nilT, fmt.Errorf(errorPrefix + "uninitialized promise")
	}
	if ctx == nil {
		return nilT, fmt.Errorf(errorPrefix + "nil context")
	}

	select {
	case <-promise.done:
	case <-ctx.Done():
		return nilT, ctx.Err()
	}

	val, err := promise.res.Value, promise.res.Err

	return val, err
}
