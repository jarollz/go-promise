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

func ExampleCallContext_featureFlagsABTest() {
	type ABFlags struct {
		UseNewRanking bool
	}

	type Profile struct {
		Name string
	}

	callABService := func(ctx context.Context, userID, device string, experiments []string) (ABFlags, error) {
		_ = userID
		_ = device
		_ = experiments

		select {
		case <-time.After(20 * time.Millisecond):
			return ABFlags{UseNewRanking: true}, nil
		case <-ctx.Done():
			return ABFlags{}, ctx.Err()
		}
	}

	fetchProfile := func(ctx context.Context, userID string) (Profile, error) {
		_ = userID

		select {
		case <-time.After(5 * time.Millisecond):
			return Profile{Name: "alice"}, nil
		case <-ctx.Done():
			return Profile{}, ctx.Err()
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	abPromise := gopromise.CallContext(ctx, func(ctx context.Context) (ABFlags, error) {
		return callABService(ctx, "u-42", "ios", []string{"ranking_exp"})
	})

	profilePromise := gopromise.CallContext(ctx, func(ctx context.Context) (Profile, error) {
		return fetchProfile(ctx, "u-42")
	})

	profile, _ := profilePromise.Resolve()

	// Resolve AB flags only when branch decision is needed.
	rankingVersion := "default"
	needRankingDecision := true
	if needRankingDecision {
		flags, err := abPromise.ResolveContext(ctx)
		if err == nil && flags.UseNewRanking {
			rankingVersion = "new"
		}
	}

	fmt.Println(profile.Name, rankingVersion)

	// Output:
	// alice new
}
