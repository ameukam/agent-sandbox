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
	"net/http"
	"time"

	"sigs.k8s.io/agent-sandbox/sandbox-router/authz"
	"sigs.k8s.io/agent-sandbox/sandbox-router/config"
	"sigs.k8s.io/agent-sandbox/sandbox-router/proxy"
	"sigs.k8s.io/agent-sandbox/sandbox-router/wake"
)

// wakePollInterval is how often a waiting wake request checks for the
// current execution Pod.
const wakePollInterval = 500 * time.Millisecond

// newWakeHandler serves X-Sandbox-Wake: true requests and passes every other
// request to next unchanged. It authorizes a wake request before the wake
// changes the Sandbox spec. Once the current execution Pod is ready, it
// forwards the request to next pinned to that Pod's IP, so next authorizes
// and proxies it like any other request.
func newWakeHandler(cfg *config.Config, authorizer authz.Authorizer, lifecycle wake.Lifecycle, next http.Handler) http.Handler {
	waker := &wake.Waker{
		Lifecycle:    lifecycle,
		Queue:        wake.NewQueue(cfg.WakeMaxWaitingPerSandbox, cfg.WakeMaxWaiting),
		Deadline:     cfg.WakeDeadline,
		PollInterval: wakePollInterval,
		RetryAfter:   cfg.WakeRetryAfter,
	}
	route := func(w http.ResponseWriter, r *http.Request, endpoint wake.Endpoint) {
		routed := r.Clone(r.Context())
		routed.Header.Del(wake.HeaderWake)
		routed.Header.Set(proxy.HeaderSandboxPodIP, endpoint.PodIP)
		next.ServeHTTP(w, routed)
	}
	wakeHandler := waker.Handler(route, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(wake.HeaderWake) != "true" {
			next.ServeHTTP(w, r)
			return
		}
		parsed, perr := proxy.ParseSandboxHeaders(r.Header, proxy.ParseOptions{})
		if perr != nil {
			proxy.WriteJSONError(w, perr)
			return
		}
		target, err := authz.NormalizeAuthorizationTarget(authz.AuthorizationTarget{
			Namespace:   parsed.Namespace,
			SandboxName: parsed.ID,
			SandboxUID:  parsed.UID,
			Port:        parsed.Port,
			Method:      r.Method,
			Path:        r.URL.EscapedPath(),
		})
		if err != nil {
			proxy.WriteJSONError(w, &proxy.Error{Status: http.StatusBadRequest, Detail: err.Error()})
			return
		}
		if err := authorizer.Authorize(r.Context(), r, target); err != nil {
			proxy.WriteJSONError(w, &proxy.Error{Status: authz.HTTPStatusFor(err), Detail: err.Error()})
			return
		}
		wakeHandler.ServeHTTP(w, r)
	})
}
