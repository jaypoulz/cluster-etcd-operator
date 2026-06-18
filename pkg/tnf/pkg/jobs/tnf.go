package jobs

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"github.com/openshift/cluster-etcd-operator/bindata"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/tools"
)

// NodeTarget identifies a specific node for job scheduling and lifecycle management.
// When set, the job is tied to this node's identity (named with node suffix, labeled with UID for cleanup).
type NodeTarget struct {
	Name string // Node name for scheduling and job naming
	UID  string // Node UID for job labeling (enables cleanup on node deletion/replacement)
}

// ValidNodeFunc calculates the list of valid target nodes for a job.
// Called by job controller before each attempt to get fresh node state.
type ValidNodeFunc func() ([]*corev1.Node, error)

var (
	// runningControllers tracks which controllers are already running to prevent duplicates
	runningControllers = make(map[string]bool)
	// runningControllersMutex protects the runningControllers map
	runningControllersMutex sync.Mutex

	// restartJobLocks tracks in-flight RestartJobOrRunController calls to prevent parallel execution
	restartJobLocks = make(map[string]*sync.Mutex)
	// restartJobLocksMutex protects the restartJobLocks map
	restartJobLocksMutex sync.Mutex

	// retryState tracks multi-node retry state for jobs using ValidNodeFunc
	// Map key is job name, value tracks current attempt and node index
	// No mutex needed: each job has only one controller (enforced by runningControllers)
	retryState = make(map[string]*JobRetryState)
)

// JobRetryState tracks retry progress for multi-node jobs
type JobRetryState struct {
	AttemptNumber    int       // Current attempt (1-N)
	NodeIndex        int       // Index of node to try in current attempt
	ValidNodes       []string  // Cached node names from last validNodeFunc call
	MaxRetryAttempts int       // Maximum attempts before degrading
	LastFailTime     time.Time // When last failure occurred
}

// configureMultiNodeJob handles multi-node retry logic for cluster-wide jobs.
// Uses validNodeFunc to get current valid nodes, tracks retry state, and selects
// which node to schedule the job on based on previous failures.
// This function is called by the job hook on every sync, so it must be idempotent.
func configureMultiNodeJob(ctx context.Context, job *batchv1.Job, validNodeFunc ValidNodeFunc, maxRetryAttempts int, kubeClient kubernetes.Interface) error {
	jobName := job.Name

	// Get current valid nodes from function
	// Note: validNodeFunc should return nodes in deterministic order (sorted)
	validNodes, err := validNodeFunc()
	if err != nil {
		return fmt.Errorf("failed to get valid nodes: %w", err)
	}
	if len(validNodes) == 0 {
		return fmt.Errorf("no valid nodes available for job")
	}

	// Get or create retry state for this job
	state, exists := retryState[jobName]
	if !exists {
		// First attempt - initialize state
		state = &JobRetryState{
			AttemptNumber:    1,
			NodeIndex:        0,
			ValidNodes:       getNodeNames(validNodes),
			MaxRetryAttempts: maxRetryAttempts,
		}
		retryState[jobName] = state
		klog.Infof("Starting job %s - attempt %d/%d, will try nodes: %v",
			jobName, state.AttemptNumber, state.MaxRetryAttempts, state.ValidNodes)
	} else {
		// Existing state - check if we need to increment
		// The hook is called on every sync to build desired spec, not just after failures.
		// Only increment if the existing job failed (was deleted by controller).
		// Check if a job currently exists in the cluster with our current state.
		existingJob, getErr := kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).Get(ctx, jobName, v1.GetOptions{})
		shouldIncrement := false
		if getErr != nil {
			if !apierrors.IsNotFound(getErr) {
				// Unexpected error - log but don't fail
				klog.Warningf("Failed to check existing job %s: %v - assuming no job exists", jobName, getErr)
			}
			// Job doesn't exist - previous job was deleted (failed or succeeded)
			// Increment to try next node
			shouldIncrement = true
		} else {
			// Job exists - check if it matches our current state
			currentNodeIndexLabel := existingJob.Labels["tnf.etcd.openshift.io/node-index"]
			expectedNodeIndexLabel := fmt.Sprintf("%d", state.NodeIndex)
			if currentNodeIndexLabel != expectedNodeIndexLabel {
				// Existing job has different node-index - shouldn't happen but log it
				klog.Warningf("Job %s exists but has unexpected node-index label: %s (expected %s)",
					jobName, currentNodeIndexLabel, expectedNodeIndexLabel)
			}
			// Job exists with current state - don't increment (this is an idempotent call)
			shouldIncrement = false
		}

		if shouldIncrement {
			state.NodeIndex++
			klog.V(2).Infof("Job %s retry - moving to next node (index %d)", jobName, state.NodeIndex)
		}
	}

	// Select node to try
	if state.NodeIndex >= len(validNodes) {
		// Exhausted all nodes in this attempt
		if state.AttemptNumber >= state.MaxRetryAttempts {
			// Exceeded max attempts - reset to attempt 1 and continue trying
			// Controller will be marked degraded but keep retrying
			klog.Warningf("Job %s exhausted all %d attempts (tried %d nodes each), resetting to attempt 1 and continuing",
				jobName, state.MaxRetryAttempts, len(validNodes))
			state.AttemptNumber = 1
			state.NodeIndex = 0
			state.ValidNodes = getNodeNames(validNodes)
		} else {
			// Start new attempt
			state.AttemptNumber++
			state.NodeIndex = 0
			state.ValidNodes = getNodeNames(validNodes)
			klog.Infof("Job %s exhausted all nodes in attempt %d, starting attempt %d/%d with nodes: %v",
				jobName, state.AttemptNumber-1, state.AttemptNumber, state.MaxRetryAttempts, state.ValidNodes)
		}
	}

	selectedNode := validNodes[state.NodeIndex]
	klog.Infof("Job %s attempt %d/%d: scheduling on node %s (index %d/%d)",
		jobName, state.AttemptNumber, state.MaxRetryAttempts, selectedNode.Name,
		state.NodeIndex+1, len(validNodes))

	// Configure job to run on selected node
	job.Spec.Template.Spec.NodeName = selectedNode.Name
	job.Labels["tnf.etcd.openshift.io/attempt"] = fmt.Sprintf("%d", state.AttemptNumber)
	job.Labels["tnf.etcd.openshift.io/node-index"] = fmt.Sprintf("%d", state.NodeIndex)

	return nil
}

// getNodeNames extracts node names from a slice of nodes
func getNodeNames(nodes []*corev1.Node) []string {
	names := make([]string, len(nodes))
	for i, node := range nodes {
		names[i] = node.Name
	}
	return names
}

// resetJobRetryState clears the retry state for a job (called on success or when starting fresh)
func resetJobRetryState(jobName string) {
	delete(retryState, jobName)
	klog.V(2).Infof("Reset retry state for job %s", jobName)
}

// RunTNFJobController starts a job controller for the specified job type.
//
// Parameters:
//   - nodeTarget: If non-nil, ties the job to this specific node (job name includes node suffix,
//     job is labeled with node UID for cleanup, and pod is scheduled on this node).
//     Use for node-specific jobs like auth and after-setup.
//   - validNodeFunc: Optional function that returns list of valid nodes to try (for multi-node retry logic).
//     Job controller will try nodes serially until one succeeds, calling this function before each attempt.
//     Only used when nodeTarget is nil. Use for cluster-wide jobs like update-setup.
//   - retries: Number of retries before marking degraded. Meaning depends on job type:
//   - Single-node (nodeTarget): sets backoffLimit (Kubernetes retries on same node)
//   - Multi-node (validNodeFunc): sets maxRetryAttempts (loop across different nodes, backoffLimit=0)
//   - Pass 0 for no retries
func RunTNFJobController(ctx context.Context, jobType tools.JobType, nodeTarget *NodeTarget, validNodeFunc ValidNodeFunc, retries int, controllerContext *controllercmd.ControllerContext, operatorClient v1helpers.StaticPodOperatorClient, kubeClient kubernetes.Interface, kubeInformersForNamespaces v1helpers.KubeInformersForNamespaces, conditions []string) {
	nodeNameForLogs := "any"
	var jobNodeName *string
	if nodeTarget != nil {
		nodeNameForLogs = nodeTarget.Name
		jobNodeName = &nodeTarget.Name
	}

	// Check if a controller for this jobType and node is already running
	controllerKey := jobType.GetJobName(jobNodeName)
	runningControllersMutex.Lock()
	if runningControllers[controllerKey] {
		runningControllersMutex.Unlock()
		klog.Infof("Two Node Fencing job controller for command %q on node %q is already running, skipping duplicate start", jobType.GetSubCommand(), nodeNameForLogs)
		return
	}
	// Mark this controller as running
	runningControllers[controllerKey] = true
	runningControllersMutex.Unlock()

	klog.Infof("starting Two Node Fencing job controller for command %q on node %q", jobType.GetSubCommand(), nodeNameForLogs)
	tnfJobController := NewJobController(
		jobType.GetJobName(jobNodeName),
		bindata.MustAsset("tnfdeployment/job.yaml"),
		controllerContext.EventRecorder,
		operatorClient,
		kubeClient,
		kubeInformersForNamespaces.InformersFor(operatorclient.TargetNamespace).Batch().V1().Jobs(),
		conditions,
		[]factory.Informer{},
		[]JobHookFunc{
			func(_ *operatorv1.OperatorSpec, job *batchv1.Job) error {
				// Configure job based on node target and retry strategy
				if nodeTarget != nil {
					// Single-node job: schedule on node, label with UID, set backoffLimit for retries
					job.Spec.Template.Spec.NodeName = nodeTarget.Name
					job.Labels["node"] = nodeTarget.UID
					job.Spec.BackoffLimit = ptr.To(int32(retries))
				} else if validNodeFunc != nil {
					// Multi-node job: use validNodeFunc and retry state, backoffLimit=0 (no Kubernetes retries)
					job.Spec.BackoffLimit = ptr.To(int32(0))
					if err := configureMultiNodeJob(ctx, job, validNodeFunc, retries, kubeClient); err != nil {
						return err
					}
				} else {
					// No specific targeting - use default backoffLimit from yaml
					if retries > 0 {
						job.Spec.BackoffLimit = ptr.To(int32(retries))
					}
				}
				job.SetName(jobType.GetJobName(jobNodeName))
				job.Labels["app.kubernetes.io/name"] = jobType.GetNameLabelValue()
				job.Spec.Template.Spec.Containers[0].Image = os.Getenv("OPERATOR_IMAGE")
				job.Spec.Template.Spec.Containers[0].Command[1] = jobType.GetSubCommand()
				return nil
			}}...,
	)
	go func() {
		defer func() {
			runningControllersMutex.Lock()
			delete(runningControllers, controllerKey)
			runningControllersMutex.Unlock()
			klog.Infof("Two Node Fencing job controller for command %q on node %q stopped", jobType.GetSubCommand(), nodeNameForLogs)
		}()
		tnfJobController.Run(ctx, 1)
	}()
}

// RestartJobOrRunController ensures a job controller is running, restarting the job if it already exists.
//
// Parameters:
//   - nodeTarget: If non-nil, ties the job to this specific node (job name includes node suffix,
//     job is labeled with node UID for cleanup, and pod is scheduled on this node).
//     Use for node-specific jobs like auth and after-setup.
//   - validNodeFunc: Optional function that returns list of valid nodes to try (for multi-node retry logic).
//     Job controller will try nodes serially until one succeeds, calling this function before each attempt.
//     Only used when nodeTarget is nil. Use for cluster-wide jobs like update-setup.
//   - retries: Number of retries before marking degraded. Meaning depends on job type:
//   - Single-node (nodeTarget): sets backoffLimit (Kubernetes retries on same node)
//   - Multi-node (validNodeFunc): sets maxRetryAttempts (loop across different nodes, backoffLimit=0)
//   - Pass 0 for no retries
func RestartJobOrRunController(
	ctx context.Context,
	jobType tools.JobType,
	nodeTarget *NodeTarget,
	validNodeFunc ValidNodeFunc,
	retries int,
	controllerContext *controllercmd.ControllerContext,
	operatorClient v1helpers.StaticPodOperatorClient,
	kubeClient kubernetes.Interface,
	kubeInformersForNamespaces v1helpers.KubeInformersForNamespaces,
	conditions []string,
	existingJobCompletionTimeout time.Duration) error {

	// Determine job name based on node target
	var jobNodeName *string
	if nodeTarget != nil {
		jobNodeName = &nodeTarget.Name
	}
	jobName := jobType.GetJobName(jobNodeName)

	// Acquire a lock for this specific job to prevent parallel execution
	restartJobLocksMutex.Lock()
	jobLock, exists := restartJobLocks[jobName]
	if !exists {
		jobLock = &sync.Mutex{}
		restartJobLocks[jobName] = jobLock
	}
	restartJobLocksMutex.Unlock()

	jobLock.Lock()
	defer jobLock.Unlock()

	// Check if job already exists
	jobExists := true
	_, err := kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).Get(ctx, jobName, v1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to check for existing job %s: %w", jobName, err)
		}
		jobExists = false
	}

	// always try to run the controller, CEO might have been restarted
	RunTNFJobController(ctx, jobType, nodeTarget, validNodeFunc, retries, controllerContext, operatorClient, kubeClient, kubeInformersForNamespaces, conditions)

	if !jobExists {
		// No existing job - reset retry state to start fresh
		resetJobRetryState(jobName)
		return nil
	}

	// Job exists, wait for completion
	klog.Infof("Job %s already exists, waiting for being stopped", jobName)
	if err := WaitForStopped(ctx, kubeClient, jobName, operatorclient.TargetNamespace, existingJobCompletionTimeout); err != nil {
		return fmt.Errorf("failed to wait for update-setup job %s to complete: %w", jobName, err)
	}

	// Delete the job so the controller can recreate it
	klog.Infof("Deleting existing job %s", jobName)
	if err := DeleteAndWait(ctx, kubeClient, jobName, operatorclient.TargetNamespace); err != nil {
		return fmt.Errorf("failed to delete existing update-setup job %s: %w", jobName, err)
	}

	// Reset retry state when starting fresh after deleting old job
	resetJobRetryState(jobName)

	return nil
}
