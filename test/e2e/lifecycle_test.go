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

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	"sigs.k8s.io/agent-sandbox/controllers"
	"sigs.k8s.io/agent-sandbox/test/e2e/framework"
	"sigs.k8s.io/agent-sandbox/test/e2e/framework/predicates"
)

func lifecycleTestNamespace(t *testing.T, tc *framework.TestContext) string {
	t.Helper()
	ns := &corev1.Namespace{}
	ns.Name = fmt.Sprintf("lifecycle-ns-%d", time.Now().UnixNano())
	require.NoError(t, tc.CreateWithCleanup(t.Context(), ns))
	return ns.Name
}

func TestSandboxDesiredLifecycleValidation(t *testing.T) {
	tc := framework.NewTestContext(t)
	ns := lifecycleTestNamespace(t, tc)

	suspended := simpleSandbox(ns)
	suspended.Name = "suspended"
	suspended.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
	suspended.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
	err := tc.CreateWithCleanup(t.Context(), suspended)
	require.True(t, apierrors.IsInvalid(err), "operatingMode Suspended must be rejected, got %v", err)

	recreate := simpleSandbox(ns)
	recreate.Name = "recreate"
	recreate.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
	recreate.Spec.FailedPodPolicy = sandboxv1beta1.FailedPodPolicyRecreate
	err = tc.CreateWithCleanup(t.Context(), recreate)
	require.True(t, apierrors.IsInvalid(err), "failedPodPolicy Recreate must be rejected, got %v", err)

	existing := simpleSandbox(ns)
	existing.Name = "existing"
	require.NoError(t, tc.CreateWithCleanup(t.Context(), existing))
	require.NoError(t, tc.Get(t.Context(), types.NamespacedName{Namespace: ns, Name: existing.Name}, existing))
	existing.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
	err = tc.Update(t.Context(), existing)
	require.True(t, apierrors.IsInvalid(err), "desiredLifecycle must not be added after creation, got %v", err)
}

// TestSandboxHibernateAndFencedWake drives the experimental lifecycle through
// a real apiserver. The test plays the runtime (checkpoint commit) and the
// external fencing owner, which have no implementation in this repository.
func TestSandboxHibernateAndFencedWake(t *testing.T) {
	tc := framework.NewTestContext(t)
	ns := lifecycleTestNamespace(t, tc)
	ctx := t.Context()
	c := tc.ControllerClient()

	sandbox := simpleSandbox(ns)
	sandbox.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
	require.NoError(t, tc.CreateWithCleanup(ctx, sandbox))
	key := types.NamespacedName{Namespace: ns, Name: sandbox.Name}
	tc.MustWaitForObject(sandbox, predicates.ReadyConditionIsTrue)

	require.NoError(t, tc.Get(ctx, key, sandbox))
	firstPod := &corev1.Pod{}
	require.NoError(t, tc.Get(ctx, key, firstPod))
	require.Equal(t, int64(1), sandbox.Status.PlacementEpoch)
	require.Equal(t, string(firstPod.UID), sandbox.Status.ExecutionPodUID)
	require.Equal(t, "1", firstPod.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation])
	require.Empty(t, firstPod.Spec.SchedulingGates)

	// Hibernation waits for a committed checkpoint.
	framework.MustUpdateObject(tc.ClusterClient, sandbox, func(obj *sandboxv1beta1.Sandbox) {
		obj.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleHibernated
	})
	tc.MustWaitForObject(sandbox, predicates.ConditionReasonEquals(
		string(sandboxv1beta1.SandboxConditionHibernated), sandboxv1beta1.SandboxReasonAwaitingCheckpoint))
	tc.MustExist(firstPod)
	// The runtime accepts the suspension request at this Sandbox generation.
	require.NoError(t, tc.Get(ctx, key, sandbox))

	checkpoint := sandboxv1beta1.SandboxCheckpointReference{
		ManifestURI:        "gs://e2e-unused/" + string(firstPod.UID) + "/manifest.json",
		ManifestGeneration: 1,
		OperationID:        "e2e-op-1",
		PlacementEpoch:     1,
		SourcePodUID:       string(firstPod.UID),
	}
	stale := checkpoint
	stale.PlacementEpoch = 7
	require.ErrorIs(t, controllers.CommitCheckpoint(ctx, c, key, controllers.CheckpointCommit{SandboxUID: sandbox.UID, SandboxGeneration: sandbox.Generation, Checkpoint: stale}),
		controllers.ErrCheckpointRejected)
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return controllers.CommitCheckpoint(ctx, c, key, controllers.CheckpointCommit{SandboxUID: sandbox.UID, SandboxGeneration: sandbox.Generation, Checkpoint: checkpoint})
	}))

	// Pod absence alone does not establish Hibernated.
	require.NoError(t, tc.WaitForObjectNotFound(ctx, firstPod))
	tc.MustWaitForObject(sandbox, predicates.ConditionReasonEquals(
		string(sandboxv1beta1.SandboxConditionHibernated), sandboxv1beta1.SandboxReasonAwaitingExecutionTermination))

	// Wake without a fence stays blocked and creates no successor.
	framework.MustUpdateObject(tc.ClusterClient, sandbox, func(obj *sandboxv1beta1.Sandbox) {
		obj.Spec.DesiredLifecycle = sandboxv1beta1.SandboxDesiredLifecycleActive
	})
	tc.MustWaitForObject(sandbox, predicates.ConditionReasonEquals(
		string(sandboxv1beta1.SandboxConditionReady), sandboxv1beta1.SandboxReasonAwaitingExecutionFence))
	time.Sleep(3 * time.Second)
	require.NoError(t, tc.ValidateObjectNotFound(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}))

	// The fencing owner reports the previous execution as fenced.
	require.NoError(t, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &sandboxv1beta1.Sandbox{}
		if err := c.Get(ctx, key, latest); err != nil {
			return err
		}
		latest.Status.FencedExecution = &sandboxv1beta1.SandboxExecutionIdentity{PodUID: string(firstPod.UID), PlacementEpoch: 1}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:   string(sandboxv1beta1.SandboxConditionExecutionFenced),
			Status: metav1.ConditionTrue,
			Reason: "E2EFenceVerified",
		})
		return c.Status().Update(ctx, latest)
	}))

	tc.MustWaitForObject(sandbox, predicates.ReadyConditionIsTrue)
	require.NoError(t, tc.Get(ctx, key, sandbox))
	replacement := &corev1.Pod{}
	require.NoError(t, tc.Get(ctx, key, replacement))
	require.NotEqual(t, firstPod.UID, replacement.UID)
	require.Equal(t, int64(2), sandbox.Status.PlacementEpoch)
	require.Equal(t, string(replacement.UID), sandbox.Status.ExecutionPodUID)
	require.Equal(t, "2", replacement.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation])
	require.Equal(t, checkpoint.ManifestURI, replacement.Annotations[sandboxv1beta1.SandboxCheckpointManifestURIAnnotation])
	require.Equal(t, checkpoint.OperationID, replacement.Annotations[sandboxv1beta1.SandboxCheckpointOperationIDAnnotation])
	require.Empty(t, replacement.Spec.SchedulingGates)
}
