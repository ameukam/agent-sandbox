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

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/agent-sandbox/sandbox-router/authz"
	"sigs.k8s.io/agent-sandbox/sandbox-router/config"
	"sigs.k8s.io/agent-sandbox/sandbox-router/proxy"
	"sigs.k8s.io/agent-sandbox/sandbox-router/wake"
)

type fakeAuthorizer struct {
	err   error
	calls atomic.Int32
}

func (a *fakeAuthorizer) Authorize(_ context.Context, _ *http.Request, _ authz.AuthorizationTarget) error {
	a.calls.Add(1)
	return a.err
}

// fakeLifecycle reports endpoint as current once ready is closed.
type fakeLifecycle struct {
	endpoint     wake.Endpoint
	ready        chan struct{}
	wakeRequests atomic.Int32
}

func (l *fakeLifecycle) RequestWake(context.Context, wake.Target) error {
	l.wakeRequests.Add(1)
	return nil
}

func (l *fakeLifecycle) CurrentEndpoint(context.Context, wake.Target) (wake.Endpoint, bool, error) {
	select {
	case <-l.ready:
		return l.endpoint, true, nil
	default:
		return wake.Endpoint{}, false, nil
	}
}

func wakeTestConfig() *config.Config {
	cfg := config.Defaults()
	cfg.WakeEnabled = true
	cfg.AuthzMode = config.AuthzTokenReview
	cfg.WakeMaxWaitingPerSandbox = 1
	cfg.WakeMaxWaiting = 1
	cfg.WakeDeadline = 5 * time.Second
	cfg.WakeRetryAfter = 7 * time.Second
	return &cfg
}

func wakeRequest(wakeHeader bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/work", nil)
	r.Header.Set(proxy.HeaderSandboxID, "sb")
	r.Header.Set(proxy.HeaderSandboxNamespace, "ns")
	r.Header.Set(proxy.HeaderSandboxUID, "sandbox-uid")
	if wakeHeader {
		r.Header.Set(wake.HeaderWake, "true")
	}
	return r
}

// recordingHandler records the headers of the request that reached the proxy.
type recordingHandler struct {
	mu      sync.Mutex
	headers []http.Header
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.headers = append(h.headers, r.Header.Clone())
	h.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func TestWakeHandlerPassesPlainRequestsThrough(t *testing.T) {
	authorizer := &fakeAuthorizer{}
	lifecycle := &fakeLifecycle{ready: make(chan struct{})}
	next := &recordingHandler{}
	h := newWakeHandler(wakeTestConfig(), authorizer, lifecycle, next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, wakeRequest(false))

	if rec.Code != http.StatusOK || len(next.headers) != 1 {
		t.Fatalf("plain request: code %d, forwarded %d", rec.Code, len(next.headers))
	}
	if next.headers[0].Get(proxy.HeaderSandboxPodIP) != "" {
		t.Fatalf("plain request was pinned to a Pod IP")
	}
	if authorizer.calls.Load() != 0 || lifecycle.wakeRequests.Load() != 0 {
		t.Fatalf("plain request triggered wake authorization or a wake")
	}
}

func TestWakeHandlerAuthorizesBeforeWaking(t *testing.T) {
	authorizer := &fakeAuthorizer{err: authz.ErrForbidden}
	lifecycle := &fakeLifecycle{ready: make(chan struct{})}
	next := &recordingHandler{}
	h := newWakeHandler(wakeTestConfig(), authorizer, lifecycle, next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, wakeRequest(true))

	if rec.Code != http.StatusForbidden {
		t.Fatalf("code = %d, want 403", rec.Code)
	}
	if lifecycle.wakeRequests.Load() != 0 || len(next.headers) != 0 {
		t.Fatalf("a denied request must not wake or reach the proxy")
	}
}

func TestWakeHandlerRoutesToCurrentEndpoint(t *testing.T) {
	authorizer := &fakeAuthorizer{}
	lifecycle := &fakeLifecycle{ready: make(chan struct{}), endpoint: wake.Endpoint{PodIP: "10.1.2.3"}}
	close(lifecycle.ready)
	next := &recordingHandler{}
	h := newWakeHandler(wakeTestConfig(), authorizer, lifecycle, next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, wakeRequest(true))

	if rec.Code != http.StatusOK || len(next.headers) != 1 {
		t.Fatalf("code %d, forwarded %d", rec.Code, len(next.headers))
	}
	if got := next.headers[0].Get(proxy.HeaderSandboxPodIP); got != "10.1.2.3" {
		t.Fatalf("forwarded Pod IP = %q, want the current endpoint", got)
	}
	if next.headers[0].Get(wake.HeaderWake) != "" {
		t.Fatalf("the wake header must not reach the proxy")
	}
	if lifecycle.wakeRequests.Load() != 1 {
		t.Fatalf("wake requests = %d, want 1", lifecycle.wakeRequests.Load())
	}
}

func TestWakeHandlerQueueBoundAnswersRetryAfter(t *testing.T) {
	lifecycle := &fakeLifecycle{ready: make(chan struct{}), endpoint: wake.Endpoint{PodIP: "10.1.2.3"}}
	next := &recordingHandler{}
	h := newWakeHandler(wakeTestConfig(), &fakeAuthorizer{}, lifecycle, next)

	// The first request holds the only queue slot until ready is closed.
	firstDone := make(chan int)
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, wakeRequest(true))
		firstDone <- rec.Code
	}()
	deadline := time.Now().Add(5 * time.Second)
	for lifecycle.wakeRequests.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first wake request never started")
		}
		time.Sleep(time.Millisecond)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, wakeRequest(true))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("Retry-After = %q, want the --wake-retry-after value 7", got)
	}

	close(lifecycle.ready)
	if code := <-firstDone; code != http.StatusOK {
		t.Fatalf("first request code = %d, want 200", code)
	}
}

func TestWakeHandlerDeadlineAnswersRetryAfter(t *testing.T) {
	cfg := wakeTestConfig()
	cfg.WakeDeadline = 50 * time.Millisecond
	lifecycle := &fakeLifecycle{ready: make(chan struct{})}
	next := &recordingHandler{}
	h := newWakeHandler(cfg, &fakeAuthorizer{}, lifecycle, next)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, wakeRequest(true))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("Retry-After = %q, want 7", got)
	}
	if len(next.headers) != 0 {
		t.Fatalf("a request without a current endpoint must not reach the proxy")
	}
}

func TestWakeDisabledByDefault(t *testing.T) {
	if config.Defaults().WakeEnabled {
		t.Fatal("the experimental wake path must be off unless --wake-enabled is set")
	}
}
