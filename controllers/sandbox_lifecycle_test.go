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

package controllers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	asmetrics "sigs.k8s.io/agent-sandbox/internal/metrics"
)

const (
	lifecycleSandboxName = "lifecycle-sandbox"
	lifecycleNamespace   = "default"
)

var lifecycleKey = types.NamespacedName{Name: lifecycleSandboxName, Namespace: lifecycleNamespace}

func lifecycleSandbox(desired sandboxv1beta1.SandboxDesiredLifecycle) *sandboxv1beta1.Sandbox {
	return &sandboxv1beta1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:       lifecycleSandboxName,
			Namespace:  lifecycleNamespace,
			UID:        sandboxUID,
			Generation: 1,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{
				PodTemplate: sandboxv1beta1.PodTemplate{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}},
				},
			},
			OperatingMode:    sandboxv1beta1.SandboxOperatingModeRunning,
			DesiredLifecycle: desired,
		},
	}
}

func lifecycleCheckpoint(epoch int64, podUID string) *sandboxv1beta1.SandboxCheckpointReference {
	return &sandboxv1beta1.SandboxCheckpointReference{
		ManifestURI:        "gs://bucket/" + podUID + "/manifest.json",
		ManifestGeneration: 1700000000000001,
		OperationID:        fmt.Sprintf("op-%s-%d", podUID, epoch),
		PlacementEpoch:     epoch,
		SourcePodUID:       podUID,
	}
}

// hibernatedSandbox returns a Sandbox whose execution podUID at epoch was
// checkpointed and removed.
func hibernatedSandbox(desired sandboxv1beta1.SandboxDesiredLifecycle, podUID string, epoch int64) *sandboxv1beta1.Sandbox {
	sb := lifecycleSandbox(desired)
	sb.Status.PlacementEpoch = epoch
	sb.Status.ExecutionPodUID = podUID
	sb.Status.CommittedCheckpoint = lifecycleCheckpoint(epoch, podUID)
	return sb
}

func setFence(sb *sandboxv1beta1.Sandbox, podUID string, epoch int64) {
	sb.Status.FencedExecution = &sandboxv1beta1.SandboxExecutionIdentity{PodUID: podUID, PlacementEpoch: epoch}
	meta.SetStatusCondition(&sb.Status.Conditions, metav1.Condition{
		Type:   string(sandboxv1beta1.SandboxConditionExecutionFenced),
		Status: metav1.ConditionTrue,
		Reason: "FenceVerified",
	})
}

func executionPod(podUID string, epoch int64, gated bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            lifecycleSandboxName,
			Namespace:       lifecycleNamespace,
			UID:             types.UID(podUID),
			Labels:          map[string]string{sandboxv1beta1.SandboxNameHashLabel: NameHash(lifecycleSandboxName)},
			Annotations:     map[string]string{sandboxv1beta1.SandboxPlacementEpochAnnotation: fmt.Sprint(epoch)},
			OwnerReferences: []metav1.OwnerReference{sandboxControllerRef(lifecycleSandboxName)},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c"}}},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIPs:     []corev1.PodIP{{IP: "10.0.0.7"}},
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if gated {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: sandboxv1beta1.SandboxPlacementSchedulingGate}}
		pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
	}
	return pod
}

// uidAssigningCreate gives created Pods distinct UIDs, as the apiserver does;
// the fake client leaves UIDs empty.
func uidAssigningCreate(next func(ctx context.Context, c client.WithWatch, obj client.Object) error) func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
	count := 0
	return func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*corev1.Pod); ok && obj.GetUID() == "" {
			count++
			obj.SetUID(types.UID(fmt.Sprintf("created-pod-uid-%d", count)))
		}
		if err := c.Create(ctx, obj, opts...); err != nil {
			return err
		}
		if next != nil {
			return next(ctx, c, obj)
		}
		return nil
	}
}

func newLifecycleReconciler(t *testing.T, afterPodCreate func(ctx context.Context, c client.WithWatch, obj client.Object) error, objs ...client.Object) (*SandboxReconciler, *events.FakeRecorder) {
	t.Helper()
	initial := make([]client.Object, 0, len(objs))
	initial = append(initial, objs...)
	c := newFakeClientWithInterceptor(interceptor.Funcs{Create: uidAssigningCreate(afterPodCreate)})
	for _, obj := range initial {
		require.NoError(t, c.Create(t.Context(), obj))
		if sb, ok := obj.(*sandboxv1beta1.Sandbox); ok {
			status := sb.Status.DeepCopy()
			live := &sandboxv1beta1.Sandbox{}
			require.NoError(t, c.Get(t.Context(), lifecycleKey, live))
			live.Status = *status
			require.NoError(t, c.Status().Update(t.Context(), live))
		}
	}
	recorder := events.NewFakeRecorder(10)
	return &SandboxReconciler{Client: c, Scheme: Scheme, Recorder: recorder, Tracer: asmetrics.NewNoOp(), ClusterDomain: "cluster.local"}, recorder
}

func reconcileLifecycle(t *testing.T, r *SandboxReconciler) error {
	t.Helper()
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: lifecycleKey})
	return err
}

func getLifecycleSandbox(t *testing.T, r *SandboxReconciler) *sandboxv1beta1.Sandbox {
	t.Helper()
	sb := &sandboxv1beta1.Sandbox{}
	require.NoError(t, r.Get(t.Context(), lifecycleKey, sb))
	return sb
}

func getLifecyclePod(t *testing.T, r *SandboxReconciler) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	err := r.Get(t.Context(), lifecycleKey, pod)
	if k8serrors.IsNotFound(err) {
		return nil
	}
	require.NoError(t, err)
	return pod
}

// drainEvents returns every event recorded so far, joined in order.
func drainEvents(recorder *events.FakeRecorder) string {
	var all []string
	for {
		select {
		case event := <-recorder.Events:
			all = append(all, event)
		default:
			return strings.Join(all, "\n")
		}
	}
}

func requireCondition(t *testing.T, sb *sandboxv1beta1.Sandbox, condType sandboxv1beta1.ConditionType, status metav1.ConditionStatus, reason string) {
	t.Helper()
	cond := meta.FindStatusCondition(sb.Status.Conditions, string(condType))
	require.NotNil(t, cond, "condition %s missing", condType)
	require.Equal(t, status, cond.Status, "condition %s", condType)
	require.Equal(t, reason, cond.Reason, "condition %s", condType)
}

func TestLifecycleFirstPlacementGatesPodUntilEpochIsRecorded(t *testing.T) {
	r, _ := newLifecycleReconciler(t, nil, lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive))

	require.NoError(t, reconcileLifecycle(t, r))

	pod := getLifecyclePod(t, r)
	require.NotNil(t, pod)
	require.Equal(t, "1", pod.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation])
	require.True(t, hasPlacementSchedulingGate(pod))
	require.NotContains(t, pod.Annotations, sandboxv1beta1.SandboxCheckpointManifestURIAnnotation)
	sb := getLifecycleSandbox(t, r)
	require.Equal(t, int64(1), sb.Status.PlacementEpoch)
	require.Equal(t, string(pod.UID), sb.Status.ExecutionPodUID)

	// The next pass sees the recorded placement and releases the gate.
	require.NoError(t, reconcileLifecycle(t, r))
	pod = getLifecyclePod(t, r)
	require.False(t, hasPlacementSchedulingGate(pod))
	sb = getLifecycleSandbox(t, r)
	requireCondition(t, sb, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonNotHibernated)
	requireCondition(t, sb, sandboxv1beta1.SandboxConditionRecoveryRequired, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonNoRecoveryRequired)
}

func TestLifecycleHibernateKeepsPodUntilCheckpointCommitted(t *testing.T) {
	sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated)
	sb.Status.PlacementEpoch = 1
	sb.Status.ExecutionPodUID = "pod-1"
	r, _ := newLifecycleReconciler(t, nil, sb, executionPod("pod-1", 1, false))

	require.NoError(t, reconcileLifecycle(t, r))
	require.NoError(t, reconcileLifecycle(t, r))

	pod := getLifecyclePod(t, r)
	require.NotNil(t, pod)
	require.True(t, pod.DeletionTimestamp.IsZero())
	require.NotContains(t, pod.Annotations, sandboxv1beta1.SandboxShutdownIntentAnnotation)
	requireCondition(t, getLifecycleSandbox(t, r), sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonAwaitingCheckpoint)
}

func TestLifecycleHibernateRecordsShutdownIntentBeforeDeletingPod(t *testing.T) {
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated, "pod-1", 1)
	r, recorder := newLifecycleReconciler(t, nil, sb, executionPod("pod-1", 1, false))

	// First pass records the intent and keeps the Pod.
	require.NoError(t, reconcileLifecycle(t, r))
	pod := getLifecyclePod(t, r)
	require.NotNil(t, pod)
	require.Equal(t, sandboxv1beta1.SandboxShutdownIntentHibernate, pod.Annotations[sandboxv1beta1.SandboxShutdownIntentAnnotation])

	// Second pass observes the persisted intent and deletes the Pod.
	require.NoError(t, reconcileLifecycle(t, r))
	require.Nil(t, getLifecyclePod(t, r))
	require.Contains(t, drainEvents(recorder), eventReasonExecutionPodRemoved)

	require.NoError(t, reconcileLifecycle(t, r))
	got := getLifecycleSandbox(t, r)
	requireCondition(t, got, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionTrue, sandboxv1beta1.SandboxReasonCheckpointCommitted)
	requireCondition(t, got, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonHibernated)
	require.Equal(t, "pod-1", got.Status.ExecutionPodUID, "the removed execution stays recorded for fencing")
	require.Empty(t, got.Status.PodIPs)
}

func TestLifecycleWakeBlockedWithoutFence(t *testing.T) {
	for name, fence := range map[string]func(*sandboxv1beta1.Sandbox){
		"no fence condition": func(*sandboxv1beta1.Sandbox) {},
		"fence for an older epoch": func(sb *sandboxv1beta1.Sandbox) {
			setFence(sb, "pod-1", 1)
		},
		"fence for another pod": func(sb *sandboxv1beta1.Sandbox) {
			setFence(sb, "pod-other", 2)
		},
		"fence condition false": func(sb *sandboxv1beta1.Sandbox) {
			setFence(sb, "pod-2", 2)
			meta.SetStatusCondition(&sb.Status.Conditions, metav1.Condition{
				Type: string(sandboxv1beta1.SandboxConditionExecutionFenced), Status: metav1.ConditionFalse, Reason: "FenceUnavailable",
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
			fence(sb)
			r, recorder := newLifecycleReconciler(t, nil, sb)

			require.NoError(t, reconcileLifecycle(t, r))
			require.NoError(t, reconcileLifecycle(t, r))

			require.Nil(t, getLifecyclePod(t, r), "no successor before the fence")
			got := getLifecycleSandbox(t, r)
			require.Equal(t, int64(2), got.Status.PlacementEpoch)
			require.Equal(t, "pod-2", got.Status.ExecutionPodUID)
			requireCondition(t, got, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonAwaitingExecutionFence)
			require.Contains(t, drainEvent(t, recorder), eventReasonAwaitingExecutionFence)
			assertNoEvent(t, recorder)
		})
	}
}

func TestLifecycleWakeCreatesReplacementFromCheckpointAfterFence(t *testing.T) {
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
	setFence(sb, "pod-2", 2)
	r, _ := newLifecycleReconciler(t, nil, sb)

	require.NoError(t, reconcileLifecycle(t, r))

	pod := getLifecyclePod(t, r)
	require.NotNil(t, pod)
	require.NotEqual(t, types.UID("pod-2"), pod.UID)
	require.True(t, hasPlacementSchedulingGate(pod))
	cp := sb.Status.CommittedCheckpoint
	require.Equal(t, map[string]string{
		sandboxv1beta1.SandboxPlacementEpochAnnotation:               "3",
		sandboxv1beta1.SandboxCheckpointManifestURIAnnotation:        cp.ManifestURI,
		sandboxv1beta1.SandboxCheckpointManifestGenerationAnnotation: fmt.Sprint(cp.ManifestGeneration),
		sandboxv1beta1.SandboxCheckpointOperationIDAnnotation:        cp.OperationID,
		sandboxv1beta1.SandboxCheckpointPlacementEpochAnnotation:     "2",
	}, map[string]string{
		sandboxv1beta1.SandboxPlacementEpochAnnotation:               pod.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation],
		sandboxv1beta1.SandboxCheckpointManifestURIAnnotation:        pod.Annotations[sandboxv1beta1.SandboxCheckpointManifestURIAnnotation],
		sandboxv1beta1.SandboxCheckpointManifestGenerationAnnotation: pod.Annotations[sandboxv1beta1.SandboxCheckpointManifestGenerationAnnotation],
		sandboxv1beta1.SandboxCheckpointOperationIDAnnotation:        pod.Annotations[sandboxv1beta1.SandboxCheckpointOperationIDAnnotation],
		sandboxv1beta1.SandboxCheckpointPlacementEpochAnnotation:     pod.Annotations[sandboxv1beta1.SandboxCheckpointPlacementEpochAnnotation],
	})
	got := getLifecycleSandbox(t, r)
	require.Equal(t, int64(3), got.Status.PlacementEpoch)
	require.Equal(t, string(pod.UID), got.Status.ExecutionPodUID)

	// The previous fence no longer matches, so losing this execution would
	// again require a new fence.
	require.False(t, previousExecutionFenced(got))
}

func TestLifecycleWakeDeletionDuringWakeLeavesNoSuccessor(t *testing.T) {
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
	setFence(sb, "pod-2", 2)
	deleteSandboxAfterPodCreate := func(ctx context.Context, c client.WithWatch, obj client.Object) error {
		if _, ok := obj.(*corev1.Pod); !ok {
			return nil
		}
		return c.Delete(ctx, &sandboxv1beta1.Sandbox{ObjectMeta: metav1.ObjectMeta{Name: lifecycleSandboxName, Namespace: lifecycleNamespace}})
	}
	r, _ := newLifecycleReconciler(t, deleteSandboxAfterPodCreate, sb)

	err := reconcileLifecycle(t, r)
	require.Error(t, err)
	require.Nil(t, getLifecyclePod(t, r), "the gated Pod is deleted when its epoch cannot be recorded")
}

func TestLifecycleWakeConcurrentSandboxChangeRejectsPlacement(t *testing.T) {
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
	setFence(sb, "pod-2", 2)
	hibernateAgainAfterPodCreate := func(ctx context.Context, c client.WithWatch, obj client.Object) error {
		if _, ok := obj.(*corev1.Pod); !ok {
			return nil
		}
		live := &sandboxv1beta1.Sandbox{}
		if err := c.Get(ctx, lifecycleKey, live); err != nil {
			return err
		}
		live.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleHibernated
		return c.Update(ctx, live)
	}
	r, _ := newLifecycleReconciler(t, hibernateAgainAfterPodCreate, sb)

	err := reconcileLifecycle(t, r)
	require.Error(t, err)
	require.True(t, k8serrors.IsConflict(errors.Unwrap(err)) || k8serrors.IsConflict(err), "got %v", err)
	require.Nil(t, getLifecyclePod(t, r))
	got := getLifecycleSandbox(t, r)
	require.Equal(t, int64(2), got.Status.PlacementEpoch, "epoch only changes through the conditional update")
	require.Equal(t, "pod-2", got.Status.ExecutionPodUID)
}

func TestLifecycleGatedPodWithUnrecordedPlacementIsDeleted(t *testing.T) {
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
	r, _ := newLifecycleReconciler(t, nil, sb, executionPod("pod-orphan", 3, true))

	require.NoError(t, reconcileLifecycle(t, r))

	require.Nil(t, getLifecyclePod(t, r))
}

func TestLifecycleGatedPodFromSupersededPlacementIsDeletedImmediately(t *testing.T) {
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
	pod := executionPod("pod-stale", 2, true)
	pod.CreationTimestamp = metav1.Now()
	r, _ := newLifecycleReconciler(t, nil, sb, pod)

	require.NoError(t, reconcileLifecycle(t, r))

	require.Nil(t, getLifecyclePod(t, r))
}

func TestLifecycleGatedPodNewerThanCachedStatusIsKept(t *testing.T) {
	// The Pod informer can observe a new execution Pod before the Sandbox
	// informer observes the status patch that recorded it.
	sb := hibernatedSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive, "pod-2", 2)
	pod := executionPod("pod-3", 3, true)
	pod.CreationTimestamp = metav1.Now()
	r, _ := newLifecycleReconciler(t, nil, sb, pod)

	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: lifecycleKey})
	require.NoError(t, err)

	require.NotNil(t, getLifecyclePod(t, r))
	require.Greater(t, result.RequeueAfter, time.Duration(0))
	require.LessOrEqual(t, result.RequeueAfter, unrecordedPlacementGracePeriod)
	got := getLifecycleSandbox(t, r)
	requireCondition(t, got, sandboxv1beta1.SandboxConditionRecoveryRequired, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonNoRecoveryRequired)
}

func TestLifecycleStaleEpochPodReportsRecoveryRequired(t *testing.T) {
	sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive)
	sb.Status.PlacementEpoch = 3
	sb.Status.ExecutionPodUID = "pod-1"
	// The Pod UID matches, but it carries an older epoch than the Sandbox records.
	r, _ := newLifecycleReconciler(t, nil, sb, executionPod("pod-1", 2, false))

	require.NoError(t, reconcileLifecycle(t, r))

	pod := getLifecyclePod(t, r)
	require.NotNil(t, pod, "evidence is preserved")
	require.True(t, pod.DeletionTimestamp.IsZero())
	got := getLifecycleSandbox(t, r)
	requireCondition(t, got, sandboxv1beta1.SandboxConditionRecoveryRequired, metav1.ConditionTrue, sandboxv1beta1.SandboxReasonUnexpectedExecutionPod)
	requireCondition(t, got, sandboxv1beta1.SandboxConditionReady, metav1.ConditionFalse, sandboxv1beta1.SandboxReasonRecoveryRequired)
	require.Empty(t, got.Status.PodIPs, "a stale execution is not published")
}

func TestLifecycleLostExecutionWithoutCheckpointRequiresRecovery(t *testing.T) {
	sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive)
	sb.Status.PlacementEpoch = 1
	sb.Status.ExecutionPodUID = "pod-1"
	setFence(sb, "pod-1", 1)
	r, _ := newLifecycleReconciler(t, nil, sb)

	require.NoError(t, reconcileLifecycle(t, r))

	require.Nil(t, getLifecyclePod(t, r))
	requireCondition(t, getLifecycleSandbox(t, r), sandboxv1beta1.SandboxConditionRecoveryRequired, metav1.ConditionTrue, sandboxv1beta1.SandboxReasonNoCommittedCheckpoint)
}

func TestLifecycleDoesNotAdoptUnownedPod(t *testing.T) {
	sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive)
	pod := executionPod("pod-unowned", 1, false)
	pod.OwnerReferences = nil
	r, _ := newLifecycleReconciler(t, nil, sb, pod)

	require.Error(t, reconcileLifecycle(t, r))

	got := getLifecyclePod(t, r)
	require.Empty(t, got.OwnerReferences)
	require.Zero(t, getLifecycleSandbox(t, r).Status.PlacementEpoch)
}

func TestLifecycleConditionsAbsentWhenNotManaged(t *testing.T) {
	sb := lifecycleSandbox("")
	r, _ := newLifecycleReconciler(t, nil, sb)

	require.NoError(t, reconcileLifecycle(t, r))

	got := getLifecycleSandbox(t, r)
	require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, string(sandboxv1beta1.SandboxConditionHibernated)))
	require.Nil(t, meta.FindStatusCondition(got.Status.Conditions, string(sandboxv1beta1.SandboxConditionRecoveryRequired)))
	pod := getLifecyclePod(t, r)
	require.NotNil(t, pod)
	require.NotContains(t, pod.Annotations, sandboxv1beta1.SandboxPlacementEpochAnnotation)
	require.Empty(t, pod.Spec.SchedulingGates)
	require.Zero(t, got.Status.PlacementEpoch)
}

func TestSandboxRecreatesFailedPodsDisabledForLifecycle(t *testing.T) {
	sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleActive)
	sb.Spec.FailedPodPolicy = sandboxv1beta1.FailedPodPolicyRecreate
	require.False(t, sandboxRecreatesFailedPods(sb))
}

func TestCommitCheckpoint(t *testing.T) {
	current := func() *sandboxv1beta1.Sandbox {
		sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated)
		sb.Status.PlacementEpoch = 2
		sb.Status.ExecutionPodUID = "pod-2"
		return sb
	}
	validCommit := CheckpointCommit{SandboxUID: sandboxUID, Checkpoint: *lifecycleCheckpoint(2, "pod-2")}

	tests := []struct {
		name         string
		sandbox      *sandboxv1beta1.Sandbox
		commit       CheckpointCommit
		wantRejected bool
		wantRef      *sandboxv1beta1.SandboxCheckpointReference
	}{
		{
			name:    "adopts checkpoint of the current execution",
			sandbox: current(),
			commit:  validCommit,
			wantRef: lifecycleCheckpoint(2, "pod-2"),
		},
		{
			name: "late upload after the sandbox was woken",
			sandbox: func() *sandboxv1beta1.Sandbox {
				sb := current()
				sb.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
				return sb
			}(),
			commit:       validCommit,
			wantRejected: true,
		},
		{
			name:         "late upload for a deleted sandbox",
			commit:       validCommit,
			wantRejected: true,
		},
		{
			name: "late upload for a recreated sandbox with the same name",
			sandbox: func() *sandboxv1beta1.Sandbox {
				sb := current()
				sb.UID = "recreated-uid"
				return sb
			}(),
			commit:       validCommit,
			wantRejected: true,
		},
		{
			name:         "stale epoch",
			sandbox:      current(),
			commit:       CheckpointCommit{SandboxUID: sandboxUID, Checkpoint: *lifecycleCheckpoint(1, "pod-2")},
			wantRejected: true,
		},
		{
			name:         "different source pod",
			sandbox:      current(),
			commit:       CheckpointCommit{SandboxUID: sandboxUID, Checkpoint: *lifecycleCheckpoint(2, "pod-1")},
			wantRejected: true,
		},
		{
			name: "same commit retried is idempotent",
			sandbox: func() *sandboxv1beta1.Sandbox {
				sb := current()
				sb.Status.CommittedCheckpoint = lifecycleCheckpoint(2, "pod-2")
				return sb
			}(),
			commit:  validCommit,
			wantRef: lifecycleCheckpoint(2, "pod-2"),
		},
		{
			name: "second operation for an already committed epoch",
			sandbox: func() *sandboxv1beta1.Sandbox {
				sb := current()
				sb.Status.CommittedCheckpoint = lifecycleCheckpoint(2, "pod-2")
				sb.Status.CommittedCheckpoint.OperationID = "op-earlier"
				return sb
			}(),
			commit:       validCommit,
			wantRejected: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var objs []client.Object
			if tc.sandbox != nil {
				objs = append(objs, tc.sandbox)
			}
			r, _ := newLifecycleReconciler(t, nil, objs...)

			err := CommitCheckpoint(t.Context(), r.Client, lifecycleKey, tc.commit)

			if tc.wantRejected {
				require.ErrorIs(t, err, ErrCheckpointRejected)
				if tc.sandbox != nil {
					require.Equal(t, tc.sandbox.Status.CommittedCheckpoint, getLifecycleSandbox(t, r).Status.CommittedCheckpoint)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRef, getLifecycleSandbox(t, r).Status.CommittedCheckpoint)
		})
	}
}

func TestCommitCheckpointConflictIsRetryableAndRevalidated(t *testing.T) {
	sb := lifecycleSandbox(sandboxv1beta1.SandboxDesiredLifecycleHibernated)
	sb.Status.PlacementEpoch = 2
	sb.Status.ExecutionPodUID = "pod-2"
	base := newFakeClient()
	require.NoError(t, base.Create(t.Context(), sb))
	live := &sandboxv1beta1.Sandbox{}
	require.NoError(t, base.Get(t.Context(), lifecycleKey, live))
	live.Status = sb.Status
	require.NoError(t, base.Status().Update(t.Context(), live))

	// A wake lands between the commit's read and its conditional write.
	woken := false
	c := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if !woken {
				woken = true
				current := &sandboxv1beta1.Sandbox{}
				if err := c.Get(ctx, key, current); err != nil {
					return err
				}
				current.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
				return c.Update(ctx, current)
			}
			return nil
		},
	})
	commit := CheckpointCommit{SandboxUID: sandboxUID, Checkpoint: *lifecycleCheckpoint(2, "pod-2")}

	err := CommitCheckpoint(t.Context(), c, lifecycleKey, commit)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCheckpointRejected)
	require.True(t, k8serrors.IsConflict(errors.Unwrap(err)), "got %v", err)

	// The retry re-reads the Sandbox and rejects the now-late upload.
	require.ErrorIs(t, CommitCheckpoint(t.Context(), c, lifecycleKey, commit), ErrCheckpointRejected)
	require.NoError(t, base.Get(t.Context(), lifecycleKey, live))
	require.Nil(t, live.Status.CommittedCheckpoint)
}
