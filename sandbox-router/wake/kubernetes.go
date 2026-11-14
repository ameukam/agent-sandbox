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
	"encoding/json"
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxclientset "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned"
)

// KubernetesLifecycle implements Lifecycle with the Sandbox and Pod APIs.
type KubernetesLifecycle struct {
	Sandboxes sandboxclientset.Interface
	Pods      kubernetes.Interface
}

var _ Lifecycle = (*KubernetesLifecycle)(nil)

func (k *KubernetesLifecycle) getSandbox(ctx context.Context, target Target) (*sandboxv1beta1.Sandbox, error) {
	sandbox, err := k.Sandboxes.AgentsV1beta1().Sandboxes(target.Namespace).Get(ctx, target.Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return nil, ErrSandboxGone
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get sandbox %s: %w", target.key(), err)
	}
	if sandbox.UID != target.UID || !sandbox.DeletionTimestamp.IsZero() {
		return nil, ErrSandboxGone
	}
	if sandbox.Spec.DesiredLifecycle == "" {
		return nil, ErrNotLifecycleManaged
	}
	return sandbox, nil
}

// wakePatchAttempts bounds how often RequestWake re-reads the Sandbox after
// a concurrent writer wins the optimistic-lock race.
const wakePatchAttempts = 3

// RequestWake sets spec.desiredLifecycle to Active. The merge patch carries
// the UID and resourceVersion that were checked, so it cannot wake a
// recreated Sandbox or overwrite a concurrent change.
func (k *KubernetesLifecycle) RequestWake(ctx context.Context, target Target) error {
	var err error
	for range wakePatchAttempts {
		var sandbox *sandboxv1beta1.Sandbox
		sandbox, err = k.getSandbox(ctx, target)
		if err != nil {
			return err
		}
		if sandbox.Spec.DesiredLifecycle == sandboxv1beta1.SandboxDesiredLifecycleActive {
			return nil
		}
		var patch []byte
		patch, err = json.Marshal(map[string]any{
			"metadata": map[string]any{"uid": sandbox.UID, "resourceVersion": sandbox.ResourceVersion},
			"spec":     map[string]any{"desiredLifecycle": sandboxv1beta1.SandboxDesiredLifecycleActive},
		})
		if err != nil {
			return fmt.Errorf("failed to build wake patch: %w", err)
		}
		_, err = k.Sandboxes.AgentsV1beta1().Sandboxes(target.Namespace).Patch(ctx, target.Name, types.MergePatchType, patch, metav1.PatchOptions{})
		switch {
		case err == nil:
			return nil
		case k8serrors.IsNotFound(err):
			return ErrSandboxGone
		case !k8serrors.IsConflict(err):
			return fmt.Errorf("failed to request wake for sandbox %s: %w", target.key(), err)
		}
		// Another writer changed the Sandbox first. Re-read it so a deletion,
		// a recreation, or an existing Active request is classified correctly.
	}
	return fmt.Errorf("failed to request wake for sandbox %s: %w", target.key(), err)
}

// CurrentEndpoint returns the execution Pod only when the Sandbox is Active
// and Ready and the Pod's UID, placement epoch, owner, and readiness all match
// the Sandbox status. A cached or stale endpoint is never returned.
func (k *KubernetesLifecycle) CurrentEndpoint(ctx context.Context, target Target) (Endpoint, bool, error) {
	sandbox, err := k.getSandbox(ctx, target)
	if err != nil {
		return Endpoint{}, false, err
	}
	status := &sandbox.Status
	if sandbox.Spec.DesiredLifecycle != sandboxv1beta1.SandboxDesiredLifecycleActive ||
		!meta.IsStatusConditionTrue(status.Conditions, string(sandboxv1beta1.SandboxConditionReady)) ||
		status.ExecutionPodUID == "" || status.PlacementEpoch < 1 {
		return Endpoint{}, false, nil
	}
	pod, err := k.Pods.CoreV1().Pods(target.Namespace).Get(ctx, target.Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		return Endpoint{}, false, nil
	}
	if err != nil {
		return Endpoint{}, false, fmt.Errorf("failed to get execution pod for sandbox %s: %w", target.key(), err)
	}
	if !podIsCurrentExecution(sandbox, pod) {
		return Endpoint{}, false, nil
	}
	return Endpoint{
		SandboxUID:     sandbox.UID,
		PodUID:         pod.UID,
		PlacementEpoch: status.PlacementEpoch,
		PodIP:          pod.Status.PodIP,
	}, true, nil
}

func podIsCurrentExecution(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if string(pod.UID) != sandbox.Status.ExecutionPodUID || !pod.DeletionTimestamp.IsZero() || pod.Status.PodIP == "" {
		return false
	}
	epoch, err := strconv.ParseInt(pod.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation], 10, 64)
	if err != nil || epoch != sandbox.Status.PlacementEpoch {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil || owner.UID != sandbox.UID {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}
