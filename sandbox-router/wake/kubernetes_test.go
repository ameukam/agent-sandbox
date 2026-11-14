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
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	sandboxfake "sigs.k8s.io/agent-sandbox/clients/k8s/clientset/versioned/fake"
)

func wakeSandbox(desired sandboxv1beta1.SandboxDesiredLifecycle, ready bool) *sandboxv1beta1.Sandbox {
	sb := &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{Name: testTarget.Name, Namespace: testTarget.Namespace, UID: testTarget.UID},
		Spec:       sandboxv1beta1.SandboxSpec{DesiredLifecycle: desired},
		Status: sandboxv1beta1.SandboxStatus{
			PlacementEpoch:  2,
			ExecutionPodUID: "pod-2",
		},
	}
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	sb.Status.Conditions = []metav1.Condition{{Type: string(sandboxv1beta1.SandboxConditionReady), Status: status, Reason: "Test"}}
	return sb
}

func wakePod(uid string, epoch string, ready bool) *corev1.Pod {
	readyStatus := corev1.ConditionFalse
	if ready {
		readyStatus = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testTarget.Name,
			Namespace:   testTarget.Namespace,
			UID:         types.UID(uid),
			Annotations: map[string]string{sandboxv1beta1.SandboxPlacementEpochAnnotation: epoch},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "agents.x-k8s.io/v1beta1", Kind: "Sandbox", Name: testTarget.Name,
				UID: testTarget.UID, Controller: new(true),
			}},
		},
		Status: corev1.PodStatus{
			PodIP:      "10.0.0.2",
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: readyStatus}},
		},
	}
}

func newKubernetesLifecycle(t *testing.T, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) *KubernetesLifecycle {
	t.Helper()
	// The generated NewClientset tracker cannot serve CRDs, and seeding objects
	// into a tracker guesses the plural "sandboxs", so the Sandbox fake uses a
	// plain object tracker and objects are created through the typed clients.
	scheme := runtime.NewScheme()
	require.NoError(t, sandboxfake.AddToScheme(scheme))
	tracker := k8stesting.NewObjectTracker(scheme, serializer.NewCodecFactory(scheme).UniversalDecoder())
	sandboxes := &sandboxfake.Clientset{}
	sandboxes.AddReactor("*", "*", k8stesting.ObjectReaction(tracker))
	lc := &KubernetesLifecycle{Sandboxes: sandboxes, Pods: k8sfake.NewClientset()}
	if sandbox != nil {
		_, err := lc.Sandboxes.AgentsV1beta1().Sandboxes(sandbox.Namespace).Create(t.Context(), sandbox, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	if pod != nil {
		_, err := lc.Pods.CoreV1().Pods(pod.Namespace).Create(t.Context(), pod, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	return lc
}

func TestKubernetesLifecycleCurrentEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		sandbox *sandboxv1beta1.Sandbox
		pod     *corev1.Pod
		wantOK  bool
		wantErr error
	}{
		{
			name:    "current ready execution",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true),
			pod:     wakePod("pod-2", "2", true),
			wantOK:  true,
		},
		{
			name:    "stale endpoint from the previous execution",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true),
			pod:     wakePod("pod-1", "2", true),
		},
		{
			name:    "stale placement epoch",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true),
			pod:     wakePod("pod-2", "1", true),
		},
		{
			name:    "execution pod not ready",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true),
			pod:     wakePod("pod-2", "2", false),
		},
		{
			name:    "sandbox not ready",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, false),
			pod:     wakePod("pod-2", "2", true),
		},
		{
			name:    "sandbox hibernating",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated, true),
			pod:     wakePod("pod-2", "2", true),
		},
		{
			name:    "pod absent",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true),
		},
		{
			name:    "pod owned by another sandbox incarnation",
			sandbox: wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true),
			pod: func() *corev1.Pod {
				p := wakePod("pod-2", "2", true)
				p.OwnerReferences[0].UID = "old-sandbox-uid"
				return p
			}(),
		},
		{
			name: "recreated sandbox",
			sandbox: func() *sandboxv1beta1.Sandbox {
				sb := wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, true)
				sb.UID = "recreated-uid"
				return sb
			}(),
			pod:     wakePod("pod-2", "2", true),
			wantErr: ErrSandboxGone,
		},
		{
			name:    "deleted sandbox",
			wantErr: ErrSandboxGone,
		},
		{
			name:    "sandbox without desiredLifecycle",
			sandbox: wakeSandbox("", true),
			pod:     wakePod("pod-2", "2", true),
			wantErr: ErrNotLifecycleManaged,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lc := newKubernetesLifecycle(t, tc.sandbox, tc.pod)

			endpoint, ok, err := lc.CurrentEndpoint(t.Context(), testTarget)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, Endpoint{SandboxUID: testTarget.UID, PodUID: "pod-2", PlacementEpoch: 2, PodIP: "10.0.0.2"}, endpoint)
			}
		})
	}
}

func TestKubernetesLifecycleRequestWakeSetsDesiredLifecycle(t *testing.T) {
	lc := newKubernetesLifecycle(t, wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated, false), nil)

	require.NoError(t, lc.RequestWake(t.Context(), testTarget))

	got, err := lc.Sandboxes.AgentsV1beta1().Sandboxes(testTarget.Namespace).Get(t.Context(), testTarget.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, sandboxv1beta1.SandboxDesiredLifecycleActive, got.Spec.DesiredLifecycle)
}

func TestKubernetesLifecycleRequestWakeRefusesOtherIncarnations(t *testing.T) {
	recreated := wakeSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated, false)
	recreated.UID = "recreated-uid"
	lc := newKubernetesLifecycle(t, recreated, nil)

	require.ErrorIs(t, lc.RequestWake(t.Context(), testTarget), ErrSandboxGone)

	got, err := lc.Sandboxes.AgentsV1beta1().Sandboxes(testTarget.Namespace).Get(t.Context(), testTarget.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, sandboxv1beta1.SandboxDesiredLifecycleHibernated, got.Spec.DesiredLifecycle)
}

func TestKubernetesLifecycleRequestWakeRefusesUnmanagedSandbox(t *testing.T) {
	lc := newKubernetesLifecycle(t, wakeSandbox("", false), nil)

	require.ErrorIs(t, lc.RequestWake(t.Context(), testTarget), ErrNotLifecycleManaged)
}
