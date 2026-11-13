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

package wake

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"sigs.k8s.io/agent-sandbox/sandbox-router/proxy"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeLifecycle reports a current endpoint once ready is closed.
type fakeLifecycle struct {
	ready      chan struct{}
	endpoint   Endpoint
	wakeErr    error
	wakes      atomic.Int32
	inEndpoint chan struct{}
	signalOnce sync.Once
}

func newFakeLifecycle() *fakeLifecycle {
	return &fakeLifecycle{
		ready:      make(chan struct{}),
		inEndpoint: make(chan struct{}),
		endpoint:   Endpoint{SandboxUID: "sb-uid", PodUID: "pod-2", PlacementEpoch: 2, PodIP: "10.0.0.2"},
	}
}

func (f *fakeLifecycle) RequestWake(context.Context, Target) error {
	f.wakes.Add(1)
	return f.wakeErr
}

func (f *fakeLifecycle) CurrentEndpoint(context.Context, Target) (Endpoint, bool, error) {
	f.signalOnce.Do(func() { close(f.inEndpoint) })
	select {
	case <-f.ready:
		return f.endpoint, true, nil
	default:
		return Endpoint{}, false, nil
	}
}

// newTestWaker admits one waiting request, so a second concurrent wake is
// always an overload.
func newTestWaker(lc Lifecycle, deadline time.Duration) *Waker {
	return &Waker{
		Lifecycle:    lc,
		Queue:        NewQueue(1, 1),
		Deadline:     deadline,
		PollInterval: time.Millisecond,
		RetryAfter:   1500 * time.Millisecond,
	}
}

var testTarget = Target{Namespace: "default", Name: "sb", UID: "sb-uid"}

func TestQueueBoundsPerSandboxAndGlobal(t *testing.T) {
	q := NewQueue(2, 3)

	releaseA1, err := q.acquire("ns/a")
	require.NoError(t, err)
	releaseA2, err := q.acquire("ns/a")
	require.NoError(t, err)
	_, err = q.acquire("ns/a")
	require.ErrorIs(t, err, ErrOverloaded, "per-Sandbox bound")

	releaseB, err := q.acquire("ns/b")
	require.NoError(t, err)
	_, err = q.acquire("ns/c")
	require.ErrorIs(t, err, ErrOverloaded, "global bound")
	require.Equal(t, 3, q.Waiting())

	releaseA1()
	releaseA1() // Releasing twice must not free a second slot.
	require.Equal(t, 2, q.Waiting())
	releaseC, err := q.acquire("ns/c")
	require.NoError(t, err)

	releaseA2()
	releaseB()
	releaseC()
	require.Equal(t, 0, q.Waiting())
	require.Empty(t, q.perSandbox)
}

func TestWakeReturnsCurrentEndpoint(t *testing.T) {
	lc := newFakeLifecycle()
	w := newTestWaker(lc, time.Minute)
	go func() {
		<-lc.inEndpoint
		close(lc.ready)
	}()

	endpoint, err := w.Wake(t.Context(), testTarget)

	require.NoError(t, err)
	require.Equal(t, lc.endpoint, endpoint)
	require.Equal(t, int32(1), lc.wakes.Load())
	require.Equal(t, 0, w.Queue.Waiting())
}

func TestWakeDeadlineExpires(t *testing.T) {
	w := newTestWaker(newFakeLifecycle(), 20*time.Millisecond)

	_, err := w.Wake(t.Context(), testTarget)

	require.ErrorIs(t, err, ErrDeadlineExceeded)
	require.Equal(t, 0, w.Queue.Waiting(), "an expired wake releases its slot")
}

func TestWakeCancellationReleasesSlot(t *testing.T) {
	lc := newFakeLifecycle()
	w := newTestWaker(lc, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		<-lc.inEndpoint
		cancel()
	}()

	_, err := w.Wake(ctx, testTarget)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, w.Queue.Waiting())
}

func TestWakeRequestErrorIsReturned(t *testing.T) {
	lc := newFakeLifecycle()
	lc.wakeErr = ErrSandboxGone
	w := newTestWaker(lc, time.Minute)

	_, err := w.Wake(t.Context(), testTarget)

	require.ErrorIs(t, err, ErrSandboxGone)
}

func wakeRequest(ctx context.Context) *http.Request {
	r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/run", nil)
	r.Header.Set(HeaderWake, "true")
	r.Header.Set(proxy.HeaderSandboxID, testTarget.Name)
	r.Header.Set(proxy.HeaderSandboxNamespace, testTarget.Namespace)
	r.Header.Set(proxy.HeaderSandboxUID, string(testTarget.UID))
	return r
}

func failNext(t *testing.T) http.Handler {
	return http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("wake request reached the plain-traffic handler")
	})
}

func failRoute(t *testing.T) RouteFunc {
	return func(http.ResponseWriter, *http.Request, Endpoint) {
		t.Error("request was routed")
	}
}

func TestHandlerOverloadReturnsRetryable503(t *testing.T) {
	lc := newFakeLifecycle()
	w := newTestWaker(lc, time.Minute)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Hold the only slot with a wake that is still waiting.
	held := make(chan error, 1)
	go func() {
		_, err := w.Wake(ctx, testTarget)
		held <- err
	}()
	<-lc.inEndpoint

	rec := httptest.NewRecorder()
	w.Handler(failRoute(t), failNext(t)).ServeHTTP(rec, wakeRequest(t.Context()))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "2", rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"detail":"Sandbox wake queue is full."}`, rec.Body.String())
	cancel()
	require.ErrorIs(t, <-held, context.Canceled)
}

func TestHandlerDeadlineReturnsRetryable503(t *testing.T) {
	w := newTestWaker(newFakeLifecycle(), 10*time.Millisecond)

	rec := httptest.NewRecorder()
	w.Handler(failRoute(t), failNext(t)).ServeHTTP(rec, wakeRequest(t.Context()))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.NotEmpty(t, rec.Header().Get("Retry-After"))
	require.JSONEq(t, `{"detail":"Sandbox wake did not complete before the deadline."}`, rec.Body.String())
}

func TestHandlerRoutesToCurrentEndpoint(t *testing.T) {
	lc := newFakeLifecycle()
	close(lc.ready)
	w := newTestWaker(lc, time.Minute)
	var routed Endpoint

	rec := httptest.NewRecorder()
	w.Handler(func(rw http.ResponseWriter, _ *http.Request, e Endpoint) {
		routed = e
		rw.WriteHeader(http.StatusNoContent)
	}, failNext(t)).ServeHTTP(rec, wakeRequest(t.Context()))

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, lc.endpoint, routed)
}

func TestHandlerPlainTrafficIsNotAWake(t *testing.T) {
	lc := newFakeLifecycle()
	w := newTestWaker(lc, time.Minute)
	r := wakeRequest(t.Context())
	r.Header.Del(HeaderWake)
	passed := false

	w.Handler(failRoute(t), http.HandlerFunc(func(http.ResponseWriter, *http.Request) { passed = true })).
		ServeHTTP(httptest.NewRecorder(), r)

	require.True(t, passed)
	require.Zero(t, lc.wakes.Load(), "plain traffic must not request a wake")
}

func TestHandlerRejectsInvalidWakeRequests(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate     func(*http.Request)
		wakeErr    error
		wantStatus int
	}{
		"missing sandbox UID": {
			mutate:     func(r *http.Request) { r.Header.Del(proxy.HeaderSandboxUID) },
			wantStatus: http.StatusBadRequest,
		},
		"pod IP override": {
			mutate:     func(r *http.Request) { r.Header.Set(proxy.HeaderSandboxPodIP, "10.0.0.9") },
			wantStatus: http.StatusBadRequest,
		},
		"invalid sandbox ID": {
			mutate:     func(r *http.Request) { r.Header.Set(proxy.HeaderSandboxID, "a.b") },
			wantStatus: http.StatusBadRequest,
		},
		"deleted sandbox": {
			wakeErr:    ErrSandboxGone,
			wantStatus: http.StatusNotFound,
		},
		"sandbox without desiredLifecycle": {
			wakeErr:    ErrNotLifecycleManaged,
			wantStatus: http.StatusConflict,
		},
		"lifecycle API failure": {
			wakeErr:    errors.New("apiserver unavailable"),
			wantStatus: http.StatusBadGateway,
		},
	} {
		t.Run(name, func(t *testing.T) {
			lc := newFakeLifecycle()
			lc.wakeErr = tc.wakeErr
			w := newTestWaker(lc, time.Minute)
			r := wakeRequest(t.Context())
			if tc.mutate != nil {
				tc.mutate(r)
			}

			rec := httptest.NewRecorder()
			w.Handler(failRoute(t), failNext(t)).ServeHTTP(rec, r)

			require.Equal(t, tc.wantStatus, rec.Code)
			require.Empty(t, rec.Header().Get("Retry-After"))
		})
	}
}
