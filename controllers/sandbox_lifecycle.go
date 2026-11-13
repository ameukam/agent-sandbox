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

// This file holds the alpha, experimental execution lifecycle that a Sandbox
// opts into with spec.desiredLifecycle: placement epochs, committed checkpoint
// references, deep hibernation, and fenced replacement of an execution Pod.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	// unrecordedPlacementGracePeriod is how long the controller keeps a gated
	// Pod whose placement epoch is newer than the cached Sandbox status. The
	// Pod and Sandbox informers are independent, so the Pod can be observed
	// before the status patch that recorded its placement.
	unrecordedPlacementGracePeriod = time.Minute

	eventReasonAwaitingExecutionFence = sandboxv1beta1.SandboxReasonAwaitingExecutionFence
	eventReasonExecutionPodRemoved    = "ExecutionPodRemovedForHibernation"
)

// lifecycleManaged reports whether the Sandbox opted into the experimental
// execution lifecycle. Every lifecycle rule in this file applies only then, so
// Sandboxes without spec.desiredLifecycle keep their existing behavior.
func lifecycleManaged(sandbox *sandboxv1beta1.Sandbox) bool {
	return sandbox != nil && sandbox.Spec.DesiredLifecycle != ""
}

// podPlacementEpoch returns the placement epoch the controller stamped on pod.
func podPlacementEpoch(pod *corev1.Pod) (int64, bool) {
	value, ok := pod.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation]
	if !ok {
		return 0, false
	}
	epoch, err := strconv.ParseInt(value, 10, 64)
	if err != nil || epoch < 1 {
		return 0, false
	}
	return epoch, true
}

// hasPlacementSchedulingGate reports whether pod is still held back by the
// gate the controller adds before it records the Pod's placement epoch.
func hasPlacementSchedulingGate(pod *corev1.Pod) bool {
	return slices.ContainsFunc(pod.Spec.SchedulingGates, func(g corev1.PodSchedulingGate) bool {
		return g.Name == sandboxv1beta1.SandboxPlacementSchedulingGate
	})
}

// isCurrentExecutionPod reports whether pod is the execution that holds the
// Sandbox's current placement epoch.
func isCurrentExecutionPod(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) bool {
	if pod == nil || sandbox.Status.ExecutionPodUID == "" || string(pod.UID) != sandbox.Status.ExecutionPodUID {
		return false
	}
	epoch, ok := podPlacementEpoch(pod)
	return ok && epoch == sandbox.Status.PlacementEpoch && isOwnedBySandbox(pod, sandbox)
}

// previousExecutionFenced reports whether the external fencing owner verified
// that the execution recorded in status can no longer run or write. Both the
// ExecutionFenced condition and the fenced identity must match, so a fence
// recorded for an older execution never authorizes a newer replacement.
func previousExecutionFenced(sandbox *sandboxv1beta1.Sandbox) bool {
	if !meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionExecutionFenced)) {
		return false
	}
	fenced := sandbox.Status.FencedExecution
	return fenced != nil &&
		fenced.PodUID == sandbox.Status.ExecutionPodUID &&
		fenced.PlacementEpoch == sandbox.Status.PlacementEpoch
}

// checkpointOfCurrentExecution reports whether the committed checkpoint was
// produced by the execution that holds the current placement epoch.
func checkpointOfCurrentExecution(sandbox *sandboxv1beta1.Sandbox) bool {
	cp := sandbox.Status.CommittedCheckpoint
	return cp != nil &&
		sandbox.Status.ExecutionPodUID != "" &&
		cp.SourcePodUID == sandbox.Status.ExecutionPodUID &&
		cp.PlacementEpoch == sandbox.Status.PlacementEpoch
}

// executionPlacement is an authorized, not yet recorded, new execution.
type executionPlacement struct {
	epoch      int64
	checkpoint *sandboxv1beta1.SandboxCheckpointReference
}

// applyTo stamps the placement on a Pod that is about to be created. The
// scheduling gate keeps the Pod from running until recordExecutionPlacement
// has stored its UID and epoch in the Sandbox status. A Pod with
// spec.nodeName set is bound without the scheduler, so a scheduling gate
// cannot hold it back; applyTo refuses such a Pod even if validation of the
// Sandbox was bypassed.
func (p *executionPlacement) applyTo(pod *corev1.Pod) error {
	if pod.Spec.NodeName != "" {
		return fmt.Errorf("execution pod %q sets spec.nodeName %q, which bypasses the placement scheduling gate", pod.Name, pod.Spec.NodeName)
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[sandboxv1beta1.SandboxPlacementEpochAnnotation] = strconv.FormatInt(p.epoch, 10)
	if p.checkpoint != nil {
		pod.Annotations[sandboxv1beta1.SandboxCheckpointManifestURIAnnotation] = p.checkpoint.ManifestURI
		pod.Annotations[sandboxv1beta1.SandboxCheckpointManifestGenerationAnnotation] = strconv.FormatInt(p.checkpoint.ManifestGeneration, 10)
		pod.Annotations[sandboxv1beta1.SandboxCheckpointOperationIDAnnotation] = p.checkpoint.OperationID
		pod.Annotations[sandboxv1beta1.SandboxCheckpointPlacementEpochAnnotation] = strconv.FormatInt(p.checkpoint.PlacementEpoch, 10)
	}
	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: sandboxv1beta1.SandboxPlacementSchedulingGate})
	return nil
}

// authorizeExecutionPlacement decides whether the controller may create a new
// execution Pod. It returns nil when creation must wait. A replacement for an
// earlier execution requires a verified fence for that execution and a
// committed checkpoint to restore from (DESIGN 5.3): the controller never
// creates a successor early.
func (r *SandboxReconciler) authorizeExecutionPlacement(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) *executionPlacement {
	logger := log.FromContext(ctx)
	if sandbox.Spec.DesiredLifecycle != sandboxv1beta1.SandboxDesiredLifecycleActive {
		return nil
	}
	status := &sandbox.Status
	if status.ExecutionPodUID == "" {
		if status.PlacementEpoch != 0 {
			logger.Info("Refusing to place execution: placement epoch has no recorded execution Pod",
				"placementEpoch", status.PlacementEpoch)
			return nil
		}
		return &executionPlacement{epoch: 1}
	}
	if !previousExecutionFenced(sandbox) {
		logger.Info("Waiting for a verified fence of the previous execution before creating a replacement",
			"executionPodUID", status.ExecutionPodUID, "placementEpoch", status.PlacementEpoch)
		r.recordAwaitingFenceEvent(sandbox)
		return nil
	}
	if status.CommittedCheckpoint == nil {
		logger.Info("Refusing to place execution: no committed checkpoint to restore from",
			"executionPodUID", status.ExecutionPodUID, "placementEpoch", status.PlacementEpoch)
		return nil
	}
	return &executionPlacement{epoch: status.PlacementEpoch + 1, checkpoint: status.CommittedCheckpoint.DeepCopy()}
}

func (r *SandboxReconciler) recordAwaitingFenceEvent(sandbox *sandboxv1beta1.Sandbox) {
	if r.Recorder == nil {
		return
	}
	ready := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
	if ready != nil && ready.Reason == sandboxv1beta1.SandboxReasonAwaitingExecutionFence {
		return
	}
	r.Recorder.Eventf(sandbox, nil, corev1.EventTypeWarning, eventReasonAwaitingExecutionFence, "Waking",
		"Replacement execution blocked until execution Pod %q at placement epoch %d is fenced",
		sandbox.Status.ExecutionPodUID, sandbox.Status.PlacementEpoch)
}

// recordExecutionPlacement stores the new epoch and execution Pod UID with a
// resourceVersion-conditional status patch. If the Sandbox changed or was
// deleted since the controller read it, the patch fails and the controller
// deletes the still-gated Pod, which has never run.
func (r *SandboxReconciler) recordExecutionPlacement(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod, placement *executionPlacement) error {
	base := sandbox.DeepCopy()
	updated := sandbox.DeepCopy()
	updated.Status.PlacementEpoch = placement.epoch
	updated.Status.ExecutionPodUID = string(pod.UID)
	patch := client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})
	if err := r.Status().Patch(ctx, updated, patch); err != nil {
		log.FromContext(ctx).Info("Placement epoch was not recorded; deleting the gated execution Pod",
			"Pod.Name", pod.Name, "Pod.UID", pod.UID, "placementEpoch", placement.epoch, "error", err.Error())
		if delErr := r.deleteExecutionPod(ctx, pod); delErr != nil {
			return errors.Join(fmt.Errorf("failed to record placement epoch %d: %w", placement.epoch, err), delErr)
		}
		return fmt.Errorf("failed to record placement epoch %d: %w", placement.epoch, err)
	}
	sandbox.ResourceVersion = updated.ResourceVersion
	sandbox.Status.PlacementEpoch = updated.Status.PlacementEpoch
	sandbox.Status.ExecutionPodUID = updated.Status.ExecutionPodUID
	return nil
}

// deleteExecutionPod deletes exactly the given Pod incarnation.
func (r *SandboxReconciler) deleteExecutionPod(ctx context.Context, pod *corev1.Pod) error {
	uid := pod.UID
	if err := r.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil && !k8serrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete execution pod %q: %w", pod.Name, err)
	}
	return nil
}

// reconcileManagedExistingPod applies the execution lifecycle to an owned Pod.
// It returns handled=false only for the current execution while the Sandbox is
// Active, so the caller continues with ordinary Pod metadata reconciliation.
// A non-zero requeue asks the caller to reconcile again after that delay.
func (r *SandboxReconciler) reconcileManagedExistingPod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) (handled bool, requeue time.Duration, err error) {
	logger := log.FromContext(ctx)
	if !isOwnedBySandbox(pod, sandbox) {
		// Adopting a Pod would make an execution that never held a placement
		// epoch, so a lifecycle-managed Sandbox never adopts one.
		return true, 0, fmt.Errorf("pod %q is not controlled by sandbox %q, and lifecycle-managed sandboxes do not adopt pods", pod.Name, sandbox.Name)
	}

	if !isCurrentExecutionPod(sandbox, pod) {
		if hasPlacementSchedulingGate(pod) && pod.DeletionTimestamp.IsZero() {
			// Deleting a gated Pod is always safe because it never ran. A Pod
			// with a newer epoch may still be recorded, though, and deleting it
			// early would leave the Sandbox waiting for a fence of an execution
			// that never ran.
			if epoch, ok := podPlacementEpoch(pod); ok && epoch > sandbox.Status.PlacementEpoch {
				if wait := unrecordedPlacementGracePeriod - time.Since(pod.CreationTimestamp.Time); wait > 0 {
					logger.V(4).Info("Waiting for the placement of a gated Pod to be observed",
						"Pod.Name", pod.Name, "Pod.UID", pod.UID, "wait", wait)
					return true, wait, nil
				}
			}
			logger.Info("Deleting gated Pod whose placement was not recorded",
				"Pod.Name", pod.Name, "Pod.UID", pod.UID)
			return true, 0, r.deleteExecutionPod(ctx, pod)
		}
		// An unrecorded Pod that may have run is evidence; keep it and report
		// RecoveryRequired instead of deleting or replacing it.
		return true, 0, nil
	}

	if hasPlacementSchedulingGate(pod) {
		patch := client.MergeFromWithOptions(pod.DeepCopy(), client.MergeFromWithOptimisticLock{})
		pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(g corev1.PodSchedulingGate) bool {
			return g.Name == sandboxv1beta1.SandboxPlacementSchedulingGate
		})
		if err := r.Patch(ctx, pod, patch); err != nil {
			return true, 0, fmt.Errorf("failed to remove placement scheduling gate: %w", err)
		}
		logger.Info("Released execution Pod for scheduling", "Pod.Name", pod.Name, "placementEpoch", sandbox.Status.PlacementEpoch)
	}

	if sandbox.Spec.DesiredLifecycle != sandboxv1beta1.SandboxDesiredLifecycleHibernated {
		return false, 0, nil
	}

	// Hibernation deletes the execution only after its checkpoint is committed
	// and the shutdown intent is persisted on the Pod (DESIGN 4.1, 5.2 step 6).
	if !checkpointOfCurrentExecution(sandbox) {
		logger.V(4).Info("Keeping execution Pod until its checkpoint is committed", "Pod.Name", pod.Name)
		return true, 0, nil
	}
	if pod.Annotations[sandboxv1beta1.SandboxShutdownIntentAnnotation] != sandboxv1beta1.SandboxShutdownIntentHibernate {
		patch := client.MergeFromWithOptions(pod.DeepCopy(), client.MergeFromWithOptimisticLock{})
		if pod.Annotations == nil {
			pod.Annotations = map[string]string{}
		}
		pod.Annotations[sandboxv1beta1.SandboxShutdownIntentAnnotation] = sandboxv1beta1.SandboxShutdownIntentHibernate
		if err := r.Patch(ctx, pod, patch); err != nil {
			return true, 0, fmt.Errorf("failed to record shutdown intent: %w", err)
		}
		logger.Info("Recorded hibernation shutdown intent on execution Pod", "Pod.Name", pod.Name)
		// Delete in a later pass, after the cache reflects the persisted intent.
		return true, 0, nil
	}
	if pod.DeletionTimestamp.IsZero() {
		if err := r.deleteExecutionPod(ctx, pod); err != nil {
			return true, 0, err
		}
		logger.Info("Deleted execution Pod for hibernation", "Pod.Name", pod.Name, "placementEpoch", sandbox.Status.PlacementEpoch)
		if r.Recorder != nil {
			r.Recorder.Eventf(sandbox, pod, corev1.EventTypeNormal, eventReasonExecutionPodRemoved, "Hibernating",
				"Deleted execution Pod %q after checkpoint %q was committed", pod.Name, sandbox.Status.CommittedCheckpoint.OperationID)
		}
	}
	return true, 0, nil
}

// lifecycleConditions computes the Hibernated and RecoveryRequired conditions
// and, when the lifecycle explains why the Sandbox cannot serve, a Ready
// condition that replaces the one computed from the Pod alone.
func lifecycleConditions(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) (hibernated, recovery metav1.Condition, ready *metav1.Condition) {
	status := &sandbox.Status
	hibernated = metav1.Condition{
		Type:               string(sandboxv1beta1.SandboxConditionHibernated),
		ObservedGeneration: sandbox.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             sandboxv1beta1.SandboxReasonNotHibernated,
	}
	recovery = metav1.Condition{
		Type:               string(sandboxv1beta1.SandboxConditionRecoveryRequired),
		ObservedGeneration: sandbox.Generation,
		Status:             metav1.ConditionFalse,
		Reason:             sandboxv1beta1.SandboxReasonNoRecoveryRequired,
	}
	desired := sandbox.Spec.DesiredLifecycle
	ownedPod := pod != nil && isOwnedBySandbox(pod, sandbox)

	switch {
	case ownedPod && !isCurrentExecutionPod(sandbox, pod) && !hasPlacementSchedulingGate(pod):
		recovery.Status = metav1.ConditionTrue
		recovery.Reason = sandboxv1beta1.SandboxReasonUnexpectedExecutionPod
		recovery.Message = fmt.Sprintf("Pod %q (UID %s) is not the execution Pod %q at placement epoch %d",
			pod.Name, pod.UID, status.ExecutionPodUID, status.PlacementEpoch)
	case pod == nil && status.ExecutionPodUID == "" && status.PlacementEpoch != 0:
		recovery.Status = metav1.ConditionTrue
		recovery.Reason = sandboxv1beta1.SandboxReasonExecutionIdentityUnknown
		recovery.Message = fmt.Sprintf("placement epoch %d has no recorded execution Pod", status.PlacementEpoch)
	case pod == nil && desired == sandboxv1beta1.SandboxDesiredLifecycleActive &&
		status.ExecutionPodUID != "" && status.CommittedCheckpoint == nil:
		recovery.Status = metav1.ConditionTrue
		recovery.Reason = sandboxv1beta1.SandboxReasonNoCommittedCheckpoint
		recovery.Message = fmt.Sprintf("execution Pod %q at placement epoch %d is gone and no checkpoint is committed",
			status.ExecutionPodUID, status.PlacementEpoch)
	}

	if desired == sandboxv1beta1.SandboxDesiredLifecycleHibernated {
		switch {
		case ownedPod && !pod.DeletionTimestamp.IsZero():
			hibernated.Reason = sandboxv1beta1.SandboxReasonPodTerminating
		case ownedPod || !checkpointOfCurrentExecution(sandbox):
			hibernated.Reason = sandboxv1beta1.SandboxReasonAwaitingCheckpoint
		default:
			hibernated.Status = metav1.ConditionTrue
			hibernated.Reason = sandboxv1beta1.SandboxReasonCheckpointCommitted
			hibernated.Message = fmt.Sprintf("checkpoint operation %q is committed", status.CommittedCheckpoint.OperationID)
		}
	}

	readyOverride := func(reason, message string) *metav1.Condition {
		return &metav1.Condition{
			Type:               string(sandboxv1beta1.SandboxConditionReady),
			ObservedGeneration: sandbox.Generation,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
		}
	}
	switch {
	case recovery.Status == metav1.ConditionTrue:
		ready = readyOverride(sandboxv1beta1.SandboxReasonRecoveryRequired, recovery.Message)
	case hibernated.Status == metav1.ConditionTrue:
		ready = readyOverride(sandboxv1beta1.SandboxReasonHibernated, "")
	case pod == nil && desired == sandboxv1beta1.SandboxDesiredLifecycleActive &&
		status.ExecutionPodUID != "" && !previousExecutionFenced(sandbox):
		ready = readyOverride(sandboxv1beta1.SandboxReasonAwaitingExecutionFence,
			fmt.Sprintf("waiting for the ExecutionFenced condition for execution Pod %q at placement epoch %d",
				status.ExecutionPodUID, status.PlacementEpoch))
	}
	return hibernated, recovery, ready
}

// CheckpointCommit identifies a checkpoint that a runtime operation uploaded
// and published as an immutable manifest.
type CheckpointCommit struct {
	// SandboxUID is the UID of the Sandbox the checkpoint operation started for.
	SandboxUID types.UID
	// Checkpoint is the reference to adopt. Its PlacementEpoch and SourcePodUID
	// must still name the Sandbox's current execution.
	Checkpoint sandboxv1beta1.SandboxCheckpointReference
}

// ErrCheckpointRejected reports that a checkpoint can never be adopted for the
// Sandbox as it exists now. The caller must leave the uploaded artifacts
// unreferenced for operation-aware garbage collection.
var ErrCheckpointRejected = errors.New("checkpoint rejected")

// CommitCheckpoint adopts a committed checkpoint reference into the Sandbox
// status (DESIGN 5.2 step 5). It succeeds only while the Sandbox UID, placement
// epoch, execution Pod UID, and desired lifecycle Hibernated still match the
// operation, so a late upload for a changed or deleted Sandbox is rejected.
// The status patch carries the resourceVersion that the checks read. A
// conflict returns an error that is not ErrCheckpointRejected; the caller
// retries, and the retry repeats every check against the newer Sandbox.
func CommitCheckpoint(ctx context.Context, c client.Client, key types.NamespacedName, commit CheckpointCommit) error {
	sandbox := &sandboxv1beta1.Sandbox{}
	if err := c.Get(ctx, key, sandbox); err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("%w: sandbox %s no longer exists", ErrCheckpointRejected, key)
		}
		return fmt.Errorf("failed to get sandbox %s: %w", key, err)
	}
	cp := commit.Checkpoint
	switch {
	case sandbox.UID != commit.SandboxUID:
		return fmt.Errorf("%w: sandbox %s has UID %s, not %s", ErrCheckpointRejected, key, sandbox.UID, commit.SandboxUID)
	case !sandbox.DeletionTimestamp.IsZero():
		return fmt.Errorf("%w: sandbox %s is being deleted", ErrCheckpointRejected, key)
	case sandbox.Spec.DesiredLifecycle != sandboxv1beta1.SandboxDesiredLifecycleHibernated:
		return fmt.Errorf("%w: sandbox %s desired lifecycle is %q, not Hibernated", ErrCheckpointRejected, key, sandbox.Spec.DesiredLifecycle)
	case cp.PlacementEpoch != sandbox.Status.PlacementEpoch:
		return fmt.Errorf("%w: checkpoint placement epoch %d is not the current epoch %d", ErrCheckpointRejected, cp.PlacementEpoch, sandbox.Status.PlacementEpoch)
	case cp.SourcePodUID == "" || cp.SourcePodUID != sandbox.Status.ExecutionPodUID:
		return fmt.Errorf("%w: checkpoint source Pod %q is not the execution Pod %q", ErrCheckpointRejected, cp.SourcePodUID, sandbox.Status.ExecutionPodUID)
	case cp.ManifestURI == "" || cp.ManifestGeneration < 1 || cp.OperationID == "":
		return fmt.Errorf("%w: checkpoint reference is incomplete", ErrCheckpointRejected)
	}
	if existing := sandbox.Status.CommittedCheckpoint; existing != nil && existing.PlacementEpoch == cp.PlacementEpoch {
		if *existing == cp {
			return nil
		}
		// A committed checkpoint may already have authorized the shutdown
		// intent, so it is never replaced within the same placement epoch.
		return fmt.Errorf("%w: operation %q is already committed for placement epoch %d", ErrCheckpointRejected, existing.OperationID, cp.PlacementEpoch)
	}
	patch := client.MergeFromWithOptions(sandbox.DeepCopy(), client.MergeFromWithOptimisticLock{})
	sandbox.Status.CommittedCheckpoint = cp.DeepCopy()
	if err := c.Status().Patch(ctx, sandbox, patch); err != nil {
		if k8serrors.IsNotFound(err) {
			return fmt.Errorf("%w: sandbox %s no longer exists", ErrCheckpointRejected, key)
		}
		return fmt.Errorf("failed to commit checkpoint for sandbox %s: %w", key, err)
	}
	return nil
}
