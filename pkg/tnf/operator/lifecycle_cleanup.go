package operator

import (
	"context"
	"fmt"
	"sort"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/operator/ceohelpers"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/jobs"
)

const (
	// maxFinishedJobsPerType is the number of completed/failed jobs to keep per job type for debugging
	maxFinishedJobsPerType = 5
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
	if err := c.cleanupOldFinishedJobs(ctx); err != nil {
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

// cleanupOldFinishedJobs keeps only the most recent N finished jobs per job type for debugging.
// This prevents accumulation of completed/failed jobs while preserving recent history.
func (c *PacemakerLifecycleManager) cleanupOldFinishedJobs(ctx context.Context) error {
	// List all TNF jobs (both node-specific and cluster-wide)
	jobList, err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=two-node-fencing-setup",
	})
	if err != nil {
		return fmt.Errorf("failed to list TNF jobs for cleanup: %w", err)
	}

	// Group finished jobs by job name pattern (strip node-specific suffix to group by type)
	// e.g., "tnf-auth-job-master-0-abc123" -> "tnf-auth-job"
	jobsByType := make(map[string][]batchv1.Job)
	for _, job := range jobList.Items {
		if !jobs.IsStopped(job) {
			// Skip running jobs
			continue
		}

		// Get job type from label
		jobType := job.Labels["app.kubernetes.io/name"]
		if jobType == "" {
			klog.V(4).Infof("Job %s has no app.kubernetes.io/name label, skipping cleanup", job.Name)
			continue
		}

		jobsByType[jobType] = append(jobsByType[jobType], job)
	}

	// For each job type, keep only the most recent N finished jobs
	deletedCount := 0
	var errs []error
	for jobType, jobsOfType := range jobsByType {
		if len(jobsOfType) <= maxFinishedJobsPerType {
			continue // No cleanup needed
		}

		// Sort by completion time (most recent first)
		sort.Slice(jobsOfType, func(i, j int) bool {
			timeI := getJobFinishTime(&jobsOfType[i])
			timeJ := getJobFinishTime(&jobsOfType[j])
			return timeI.After(timeJ) // Descending order (newest first)
		})

		// Delete jobs beyond the limit
		for i := maxFinishedJobsPerType; i < len(jobsOfType); i++ {
			job := jobsOfType[i]
			klog.V(2).Infof("Deleting old finished job %s (type: %s, finished: %v)",
				job.Name, jobType, getJobFinishTime(&job))

			deletePolicy := metav1.DeletePropagationBackground
			err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).Delete(ctx, job.Name, metav1.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			})
			if err != nil && !apierrors.IsNotFound(err) {
				klog.Errorf("Failed to delete old job %s: %v", job.Name, err)
				errs = append(errs, fmt.Errorf("failed to delete job %s: %w", job.Name, err))
				continue
			}
			deletedCount++
		}
	}

	if deletedCount > 0 {
		klog.Infof("Cleaned up %d old finished TNF jobs (keeping %d per type)", deletedCount, maxFinishedJobsPerType)
	} else {
		klog.V(4).Infof("No old finished jobs to clean up")
	}

	return errors.NewAggregate(errs)
}

// getJobFinishTime returns the completion or failure time of a job
func getJobFinishTime(job *batchv1.Job) metav1.Time {
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed {
			return condition.LastTransitionTime
		}
	}
	// Fallback to creation time if no completion condition found
	return job.CreationTimestamp
}
