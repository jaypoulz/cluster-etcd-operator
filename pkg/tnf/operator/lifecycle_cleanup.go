package operator

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/operator/ceohelpers"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
)

const (
// Cleanup runs every 30 seconds to delete orphaned pods and jobs
// No constants needed - we delete all orphaned resources
)

// CleanupOrphanedJobs cleans up TNF jobs for nodes that no longer exist in K8s.
// This is called periodically from sync() to catch missed delete events.
func (c *PacemakerLifecycleManager) CleanupOrphanedJobs(ctx context.Context) error {
	// Check if node informer has synced
	if c.nodeInformer == nil || !c.nodeInformer.HasSynced() {
		klog.V(4).Infof("Skipping orphaned job cleanup - node informer not synced yet")
		return nil
	}

	// Get K8s control plane nodes
	k8sNodes, err := ceohelpers.ListNodesFromInformer(c.nodeInformer)
	if err != nil {
		return fmt.Errorf("failed to list control plane nodes: %w", err)
	}

	// Call helpers to perform cleanup
	var errs []error
	if err := c.cleanupOrphanedJobs(ctx, k8sNodes); err != nil {
		errs = append(errs, err)
	}
	if err := c.cleanupOrphanedPods(ctx); err != nil {
		errs = append(errs, err)
	}
	return errors.NewAggregate(errs)
}

// cleanupOrphanedJobs deletes TNF jobs for nodes that no longer exist in K8s.
// This is called periodically during reconciliation to catch missed delete events.
func (c *PacemakerLifecycleManager) cleanupOrphanedJobs(ctx context.Context, k8sNodes []*corev1.Node) error {
	// Build set of current node UIDs
	currentNodeUIDs := make(map[string]bool)
	for _, node := range k8sNodes {
		currentNodeUIDs[string(node.UID)] = true
	}

	// List all node-specific TNF jobs in openshift-etcd namespace
	// Note: update-setup, setup, and fencing jobs are cluster-wide (no node label), so we don't clean them up here
	jobList, err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name in (tnf-auth-job,tnf-after-setup-job)",
	})
	if err != nil {
		return fmt.Errorf("failed to list TNF jobs: %w", err)
	}

	// Check each job to see if its node still exists
	orphanedCount := 0
	var errs []error
	for _, job := range jobList.Items {
		// Get node UID from job label (not node name - UIDs are stable across node replacements).
		// If a node is deleted and re-added with the same name but different UID,
		// jobs labeled with the old UID should be cleaned up.
		jobNodeUID, ok := job.Labels["node"]
		if !ok {
			// Jobs without node label are not node-specific (e.g., setup/fencing jobs)
			klog.V(4).Infof("Job %s has no node label, skipping", job.Name)
			continue
		}

		// Check if the node UID still exists in current node set
		if !currentNodeUIDs[jobNodeUID] {
			// Node doesn't exist - delete orphaned job
			klog.Infof("Deleting orphaned TNF job %s for deleted/replaced node (UID: %s)", job.Name, jobNodeUID)

			// Delete the orphaned job
			deletePolicy := metav1.DeletePropagationBackground
			err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).Delete(ctx, job.Name, metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			})
			if err != nil && !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to delete orphaned job %s: %v", job.Name, err)
				errs = append(errs, fmt.Errorf("failed to delete job %s: %w", job.Name, err))
				continue
			}
			orphanedCount++
		}
	}

	if orphanedCount > 0 {
		klog.Infof("Cleaned up %d orphaned TNF jobs", orphanedCount)
	} else {
		klog.V(4).Infof("No orphaned TNF jobs found")
	}

	return errors.NewAggregate(errs)
}

// cleanupOrphanedPods deletes pods whose owner Jobs no longer exist.
// This handles cases where Job deletion with propagationPolicy:Background doesn't clean up pods,
// or where pods get stuck after Job deletion.
func (c *PacemakerLifecycleManager) cleanupOrphanedPods(ctx context.Context) error {
	// List all TNF pods (both node-specific and cluster-wide)
	podList, err := c.kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=two-node-fencing-setup",
	})
	if err != nil {
		return fmt.Errorf("failed to list TNF pods for cleanup: %w", err)
	}

	// Build set of existing TNF jobs
	jobList, err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=two-node-fencing-setup",
	})
	if err != nil {
		return fmt.Errorf("failed to list TNF jobs for pod cleanup: %w", err)
	}

	existingJobs := make(map[string]bool)
	for _, job := range jobList.Items {
		existingJobs[job.Name] = true
	}

	// Delete pods whose owner Job no longer exists
	deletedCount := 0
	var errs []error
	for _, pod := range podList.Items {
		// Check if pod has ownerReferences
		if len(pod.OwnerReferences) == 0 {
			klog.V(4).Infof("Pod %s has no owner references, skipping", pod.Name)
			continue
		}

		// Check if owner Job still exists
		ownerJob := pod.OwnerReferences[0].Name
		if !existingJobs[ownerJob] {
			// Owner Job doesn't exist - delete orphaned pod
			klog.V(2).Infof("Deleting orphaned TNF pod %s (owner job %s no longer exists)", pod.Name, ownerJob)

			err := c.kubeClient.CoreV1().Pods(operatorclient.TargetNamespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to delete orphaned pod %s: %v", pod.Name, err)
				errs = append(errs, fmt.Errorf("failed to delete pod %s: %w", pod.Name, err))
				continue
			}
			deletedCount++
		}
	}

	if deletedCount > 0 {
		klog.Infof("Cleaned up %d orphaned TNF pods", deletedCount)
	} else {
		klog.V(4).Infof("No orphaned pods to clean up")
	}

	return errors.NewAggregate(errs)
}
