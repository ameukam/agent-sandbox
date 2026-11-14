// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package wake implements the router's alpha, experimental wake path for
// Sandboxes that use spec.desiredLifecycle. A request asks for a wake
// explicitly with the X-Sandbox-Wake header. The router then holds the request
// in a bounded per-Sandbox and global queue, sets spec.desiredLifecycle to
// Active, and routes only to the current ready execution Pod whose UID and
// placement epoch match the Sandbox status. Plain traffic is never a wake.
package wake

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/agent-sandbox/sandbox-router/proxy"
)

// HeaderWake marks a request as an explicit wake request when set to "true".
const HeaderWake = "X-Sandbox-Wake"

var (
	// ErrOverloaded reports that the per-Sandbox or global wake queue is full.
	// Callers may retry after the advertised Retry-After interval.
	ErrOverloaded = errors.New("wake queue is full")
	// ErrDeadlineExceeded reports that no current ready endpoint appeared
	// before the wake deadline. Callers may retry.
	ErrDeadlineExceeded = errors.New("sandbox wake did not complete before the deadline")
	// ErrSandboxGone reports that the requested Sandbox incarnation no longer
	// exists. A wake for a deleted or recreated Sandbox never succeeds.
	ErrSandboxGone = errors.New("sandbox incarnation no longer exists")
	// ErrNotLifecycleManaged reports that the Sandbox does not use
	// spec.desiredLifecycle, so it has no wake contract.
	ErrNotLifecycleManaged = errors.New("sandbox does not use desiredLifecycle")
)

// Target names the Sandbox incarnation a wake is for.
type Target struct {
	Namespace string
	Name      string
	UID       types.UID
}

func (t Target) key() string { return t.Namespace + "/" + t.Name }

// Endpoint is a routable execution of a Sandbox.
type Endpoint struct {
	SandboxUID     types.UID
	PodUID         types.UID
	PlacementEpoch int64
	PodIP          string
}

// Lifecycle is the router's interface to the Sandbox lifecycle owner.
type Lifecycle interface {
	// RequestWake records the desire for an Active execution of target.
	RequestWake(ctx context.Context, target Target) error
	// CurrentEndpoint returns the ready execution that holds target's current
	// placement epoch. It returns ok=false while no such execution exists.
	CurrentEndpoint(ctx context.Context, target Target) (endpoint Endpoint, ok bool, err error)
}

// Queue bounds the number of requests waiting for wakes, per Sandbox and in
// total. A request that does not fit is rejected immediately rather than
// queued without bound.
type Queue struct {
	maxPerSandbox int
	maxGlobal     int

	mu         sync.Mutex
	global     int
	perSandbox map[string]int
}

// NewQueue returns a Queue that admits at most maxPerSandbox waiting requests
// for one Sandbox and maxGlobal waiting requests in total.
func NewQueue(maxPerSandbox, maxGlobal int) *Queue {
	return &Queue{maxPerSandbox: maxPerSandbox, maxGlobal: maxGlobal, perSandbox: map[string]int{}}
}

// acquire reserves a waiting slot for key and returns the function that
// releases it.
func (q *Queue) acquire(key string) (func(), error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.global >= q.maxGlobal || q.perSandbox[key] >= q.maxPerSandbox {
		return nil, ErrOverloaded
	}
	q.global++
	q.perSandbox[key]++
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			defer q.mu.Unlock()
			q.global--
			if q.perSandbox[key]--; q.perSandbox[key] == 0 {
				delete(q.perSandbox, key)
			}
		})
	}, nil
}

// Waiting returns the number of requests currently holding a slot.
func (q *Queue) Waiting() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.global
}

// Waker requests wakes and waits for their current endpoints.
type Waker struct {
	Lifecycle Lifecycle
	Queue     *Queue
	// Deadline bounds how long one request waits for a current endpoint.
	Deadline time.Duration
	// PollInterval is how often the Waker checks for the current endpoint.
	PollInterval time.Duration
	// RetryAfter is the delay advertised to callers that receive a
	// retryable overload or deadline response.
	RetryAfter time.Duration
}

// Wake waits for a current ready endpoint of target. It returns
// ErrOverloaded when no queue slot is free, ErrDeadlineExceeded when the
// deadline passes first, and the context error when the caller cancels.
func (w *Waker) Wake(ctx context.Context, target Target) (Endpoint, error) {
	release, err := w.Queue.acquire(target.key())
	if err != nil {
		return Endpoint{}, err
	}
	defer release()

	ctx, cancel := context.WithTimeoutCause(ctx, w.Deadline, ErrDeadlineExceeded)
	defer cancel()

	if err := w.Lifecycle.RequestWake(ctx, target); err != nil {
		return Endpoint{}, waitError(ctx, err)
	}
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()
	for {
		endpoint, ok, err := w.Lifecycle.CurrentEndpoint(ctx, target)
		if err != nil {
			return Endpoint{}, waitError(ctx, err)
		}
		if ok {
			return endpoint, nil
		}
		select {
		case <-ctx.Done():
			return Endpoint{}, waitError(ctx, ctx.Err())
		case <-ticker.C:
		}
	}
}

// waitError prefers the deadline or cancellation cause over the error a
// lifecycle call returned because its context ended.
func waitError(ctx context.Context, err error) error {
	if ctx.Err() == nil {
		return err
	}
	if cause := context.Cause(ctx); errors.Is(cause, ErrDeadlineExceeded) {
		return ErrDeadlineExceeded
	}
	return ctx.Err()
}

// RouteFunc forwards a request to a verified current endpoint.
type RouteFunc func(w http.ResponseWriter, r *http.Request, endpoint Endpoint)

// Handler serves explicit wake requests with route and passes every other
// request to next unchanged. Callers must place it behind request
// authorization: a wake changes the Sandbox's desired lifecycle.
func (w *Waker) Handler(route RouteFunc, next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Header.Get(HeaderWake) != "true" {
			next.ServeHTTP(rw, r)
			return
		}
		parsed, perr := proxy.ParseSandboxHeaders(r.Header, proxy.ParseOptions{})
		if perr != nil {
			proxy.WriteJSONError(rw, perr)
			return
		}
		if parsed.UID == "" {
			proxy.WriteJSONError(rw, &proxy.Error{Status: http.StatusBadRequest, Detail: "X-Sandbox-UID header is required for a wake request."})
			return
		}
		if parsed.PodIP != "" {
			proxy.WriteJSONError(rw, &proxy.Error{Status: http.StatusBadRequest, Detail: "X-Sandbox-Pod-IP is not allowed on a wake request."})
			return
		}
		target := Target{Namespace: parsed.Namespace, Name: parsed.ID, UID: types.UID(parsed.UID)}
		endpoint, err := w.Wake(r.Context(), target)
		switch {
		case err == nil:
			route(rw, r, endpoint)
		case errors.Is(err, ErrOverloaded):
			w.writeRetryable(rw, "Sandbox wake queue is full.")
		case errors.Is(err, ErrDeadlineExceeded):
			w.writeRetryable(rw, "Sandbox wake did not complete before the deadline.")
		case errors.Is(err, context.Canceled):
			// The caller went away; nobody reads a response.
		case errors.Is(err, ErrSandboxGone):
			proxy.WriteJSONError(rw, &proxy.Error{Status: http.StatusNotFound, Detail: "Sandbox not found."})
		case errors.Is(err, ErrNotLifecycleManaged):
			proxy.WriteJSONError(rw, &proxy.Error{Status: http.StatusConflict, Detail: "Sandbox does not support wake."})
		default:
			proxy.WriteJSONError(rw, &proxy.Error{Status: http.StatusBadGateway, Detail: "Sandbox wake failed."})
		}
	})
}

func (w *Waker) writeRetryable(rw http.ResponseWriter, detail string) {
	seconds := max(int64(math.Ceil(w.RetryAfter.Seconds())), 1)
	rw.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	proxy.WriteJSONError(rw, &proxy.Error{Status: http.StatusServiceUnavailable, Detail: detail})
}
