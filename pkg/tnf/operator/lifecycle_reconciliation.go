package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"github.com/openshift/cluster-etcd-operator/pkg/operator/ceohelpers"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/jobs"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/tools"
)

var (
	// updateSetupGeneration orders update-setup ConfigMaps when events arrive close together (OCPBUGS-84695).
	updateSetupGeneration int64 = 0

	// reconcilePacemakerConfigMutex serializes ReconcilePacemakerConfig to prevent time-of-check-time-of-use
	// races between drift detection and update-setup triggering. Multiple concurrent calls can detect
	// drift simultaneously, but only one should proceed with reconciliation at a time.
	reconcilePacemakerConfigMutex sync.Mutex

	// updateSetupFunc is a variable to allow mocking in tests
	updateSetupFunc = updateSetup
)

// ReconcilePacemakerConfig performs drift detection and reconciliation after external etcd transition completes.
// Compares K8s node state with pacemaker membership and triggers update-setup if drift is detected.
// Also handles orphaned jobs by starting JobController to reconcile operator conditions.
// This method is called:
//  1. Periodically from sync() (every 30s)
//  2. On node Add events
//  3. On node Update events (IP changes while Ready)
//  4. On node Delete events
//
// Returns nil if no action needed or reconciliation triggered successfully.
//
// Concurrency model: Multiple goroutines can enter this function concurrently and perform
// initial checks (informer sync, node readiness, drift detection). Once drift is detected,
// reconcilePacemakerConfigMutex serializes the check-and-trigger path to prevent time-of-check-time-of-use
// races where multiple goroutines would redundantly create ConfigMaps and trigger jobs. The second
// goroutine will either find no drift (first goroutine fixed it) or trigger a new reconciliation
// with a higher generation number. The generation counter ensures correctness (highest wins).
func (c *PacemakerLifecycleManager) ReconcilePacemakerConfig(ctx context.Context) error {
	// Check if node informer has synced
	if c.nodeInformer == nil || !c.nodeInformer.HasSynced() {
		klog.V(4).Infof("Skipping drift reconciliation - node informer not synced yet")
		return nil
	}

	// Get K8s control plane nodes
	k8sNodes, err := ceohelpers.ListNodesFromInformer(c.nodeInformer)
	if err != nil {
		return fmt.Errorf("failed to list control plane nodes: %w", err)
	}

	// Validate node count (pacemaker only supports 2 nodes)
	if len(k8sNodes) > 2 {
		klog.Warningf("Found %d control plane nodes - pacemaker behavior undefined for >2 nodes, no reconciliation action taken", len(k8sNodes))
		return nil
	}

	// Check all K8s nodes are Ready before attempting drift reconciliation
	// This ensures we don't try to reconcile while nodes are in flux (e.g., single-node case after delete)
	for _, node := range k8sNodes {
		if !tools.IsNodeReady(node) {
			klog.V(4).Infof("Node %s is not Ready - skipping drift reconciliation (will retry in next sync)", node.Name)
			return nil
		}
	}

	// Get pacemaker nodes from PacemakerCluster CR
	pacemakerNodes, err := c.getPacemakerNodes()
	if err != nil {
		// CR might not exist yet during initial setup, don't treat as error
		klog.V(4).Infof("Skipping reconciliation check - failed to get pacemaker nodes: %v", err)
		return nil
	}

	// Check for stopped update-setup job from previous run (handles perma-degraded/perma-progressing)
	// This needs to run on every sync to catch jobs that stopped while operator was down
	stoppedJob, err := c.getStoppedUpdateSetupJob(ctx)
	if err != nil {
		klog.Warningf("Failed to check for stopped update-setup job: %v", err)
		// Don't return error - continue with drift detection
	}

	// Detect drift (compares node names and IPs)
	hasDrift := c.detectDrift(k8sNodes, pacemakerNodes)

	// Determine if we need to take action
	needsReconciliation := false
	var reason string

	// Reason 1: Stopped unsuccessful job exists
	if stoppedJob != nil && !jobs.IsComplete(*stoppedJob) {
		needsReconciliation = true
		reason = fmt.Sprintf("stopped unsuccessful job %s exists", stoppedJob.Name)
	}

	// Reason 2: Drift detected
	if hasDrift {
		needsReconciliation = true
		if reason == "" {
			reason = fmt.Sprintf("drift detected between K8s (%d nodes) and pacemaker (%d nodes)", len(k8sNodes), len(pacemakerNodes))
		} else {
			reason = fmt.Sprintf("%s AND drift detected", reason)
		}
	}

	// If no action needed, return early
	if !needsReconciliation {
		klog.V(4).Infof("No reconciliation needed - no drift and no stopped jobs")
		return nil
	}

	klog.Infof("Reconciliation needed: %s", reason)

	// Serialize reconciliation trigger to prevent time-of-check-time-of-use race between drift detection
	// and update-setup start. Multiple concurrent callers can detect drift, but only one should check-and-trigger at a time.
	reconcilePacemakerConfigMutex.Lock()
	defer reconcilePacemakerConfigMutex.Unlock()

	// Re-check drift and stopped job after acquiring lock (another goroutine may have changed state)
	stoppedJob, err = c.getStoppedUpdateSetupJob(ctx)
	if err != nil {
		klog.Warningf("Failed to re-check for stopped update-setup job: %v", err)
	}
	hasDrift = c.detectDrift(k8sNodes, pacemakerNodes)

	// Decision tree:
	// 1. If drift exists → ensure ConfigMap exists and matches desired state, start controller if needed
	// 2. If stopped unsuccessful job but NO drift → delete job (cluster is correct, stale failure)
	// 3. Otherwise → nothing to do

	if hasDrift {
		klog.Infof("Drift detected - ensuring ConfigMap and controller")
		// Continue to ConfigMap creation and controller start below...
	} else if stoppedJob != nil && !jobs.IsComplete(*stoppedJob) {
		// No drift, but stopped unsuccessful job exists - cluster is correct, just delete the stale job
		klog.Infof("No drift detected, but stopped job %s exists - deleting stale job to clear conditions", stoppedJob.Name)
		if err := jobs.DeleteAndWait(ctx, c.kubeClient, stoppedJob.Name, operatorclient.TargetNamespace); err != nil {
			return fmt.Errorf("failed to delete stopped job: %w", err)
		}
		klog.Infof("Successfully deleted stopped job %s - JobController will clear conditions", stoppedJob.Name)
		return nil
	} else {
		klog.V(4).Infof("No action needed after acquiring lock - another goroutine may have handled it")
		return nil
	}

	// Calculate intersection: nodes that exist in BOTH K8s and pacemaker
	intersection := c.getIntersection(k8sNodes, pacemakerNodes)
	if len(intersection) == 0 {
		return fmt.Errorf("no nodes in both K8s and pacemaker - manual intervention may be required")
	}

	klog.Infof("Found %d nodes in intersection (K8s ∩ pacemaker): %v", len(intersection), getNodeNames(intersection))

	// Call update-setup with intersection nodes (creates/updates ConfigMap and ensures controller is running)
	// Use updateSetupFunc to allow mocking in tests
	return updateSetupFunc(
		intersection,
		k8sNodes,
		pacemakerNodes,
		ctx,
		c.controllerContext,
		c.operatorClient,
		c.kubeClient,
		c.kubeInformersForNamespaces,
	)
}

// detectDrift compares K8s nodes with pacemaker nodes and returns true if drift exists.
// Checks both node names and IPs (supports IPv4 and IPv6).
func (c *PacemakerLifecycleManager) detectDrift(k8sNodes []*corev1.Node, pacemakerNodes map[string]string) bool {
	// Check count mismatch
	if len(k8sNodes) != len(pacemakerNodes) {
		klog.V(2).Infof("Drift detected: node count mismatch (K8s: %d, Pacemaker: %d)",
			len(k8sNodes), len(pacemakerNodes))
		return true
	}

	// Check each K8s node exists in pacemaker with matching IP
	for _, k8sNode := range k8sNodes {
		k8sIP, err := tools.GetNodeIPForPacemaker(*k8sNode)
		if err != nil {
			klog.Warningf("Failed to get IP for K8s node %s: %v", k8sNode.Name, err)
			continue
		}

		pmIP, exists := pacemakerNodes[k8sNode.Name]
		if !exists {
			klog.V(2).Infof("Drift detected: node %s exists in K8s but not in pacemaker", k8sNode.Name)
			return true
		}

		if !ceohelpers.IPAddressesEqual(k8sIP, pmIP) {
			klog.V(2).Infof("Drift detected: node %s IP mismatch (K8s: %s, Pacemaker: %s)",
				k8sNode.Name, k8sIP, pmIP)
			return true
		}
	}

	// Check for nodes in pacemaker but not in K8s
	for pmNodeName := range pacemakerNodes {
		found := false
		for _, k8sNode := range k8sNodes {
			if k8sNode.Name == pmNodeName {
				found = true
				break
			}
		}
		if !found {
			klog.V(2).Infof("Drift detected: node %s exists in pacemaker but not in K8s", pmNodeName)
			return true
		}
	}

	return false
}

// getIntersection returns nodes that exist in BOTH K8s and pacemaker.
// Used to determine valid target nodes for update-setup operations.
func (c *PacemakerLifecycleManager) getIntersection(k8sNodes []*corev1.Node, pacemakerNodes map[string]string) []*corev1.Node {
	intersection := []*corev1.Node{}
	for _, k8sNode := range k8sNodes {
		if _, exists := pacemakerNodes[k8sNode.Name]; exists {
			intersection = append(intersection, k8sNode)
		}
	}
	return intersection
}

// isUpdateSetupRunning checks if any update-setup job is currently running.
func (c *PacemakerLifecycleManager) isUpdateSetupRunning(ctx context.Context) (bool, error) {
	// List all update-setup jobs by name label
	jobList, err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).List(ctx, v1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=tnf-update-setup-job",
	})
	if err != nil {
		return false, fmt.Errorf("failed to list update-setup jobs: %w", err)
	}

	// Check if any job is still running (not Complete and not Failed)
	for _, job := range jobList.Items {
		if !jobs.IsStopped(job) {
			klog.V(4).Infof("Update-setup job %s is still running", job.Name)
			return true, nil
		}
	}

	return false, nil
}

// getStoppedUpdateSetupJob returns the stopped update-setup job if one exists, nil otherwise.
// A stopped job is one that has completed (successfully or failed).
func (c *PacemakerLifecycleManager) getStoppedUpdateSetupJob(ctx context.Context) (*batchv1.Job, error) {
	jobName := tools.JobTypeUpdateSetup.GetJobName(nil)
	job, err := c.kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).Get(ctx, jobName, v1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get update-setup job: %w", err)
	}

	if jobs.IsStopped(*job) {
		return job, nil
	}

	return nil, nil
}

// getNextUpdateSetupGeneration increments and returns the next generation counter.
// Caller must hold reconcilePacemakerConfigMutex.
func getNextUpdateSetupGeneration() int64 {
	updateSetupGeneration++
	return updateSetupGeneration
}

func getCurrentUpdateSetupGeneration() int64 {
	return updateSetupGeneration
}

// initUpdateSetupGeneration scans existing update-setup ConfigMaps and initializes the generation
// counter to max(existing)+1 to prevent reusing stale ConfigMaps after operator restart.
// Must be called once during lifecycle manager initialization before any reconciliation loops run.
func (c *PacemakerLifecycleManager) initUpdateSetupGeneration(ctx context.Context) error {
	cmList, err := c.kubeClient.CoreV1().ConfigMaps(operatorclient.TargetNamespace).List(ctx, v1.ListOptions{
		LabelSelector: "app.kubernetes.io/component=" + tools.TnfUpdateSetupComponentValue,
	})
	if err != nil {
		return fmt.Errorf("failed to list existing update-setup ConfigMaps: %w", err)
	}

	var maxGen int64 = 0
	for _, cm := range cmList.Items {
		genStr := cm.Data["generation"]
		if genStr == "" {
			continue
		}
		gen, err := strconv.ParseInt(genStr, 10, 64)
		if err != nil {
			klog.Warningf("Found update-setup ConfigMap %s with invalid generation %q: %v", cm.Name, genStr, err)
			continue
		}
		if gen > maxGen {
			maxGen = gen
		}
	}

	updateSetupGeneration = maxGen
	if maxGen > 0 {
		klog.Infof("Initialized update-setup generation counter to %d (found %d existing ConfigMaps)", maxGen, len(cmList.Items))
	} else {
		klog.V(2).Infof("No existing update-setup ConfigMaps found, starting generation counter at 0")
	}
	return nil
}

// updateSetup writes a snapshot ConfigMap, runs auth on all nodes, update-setup on one target, then after-setup on all.
// validTargetNodes: nodes that can run the update-setup job (intersection of K8s and pacemaker)
// allK8sNodes: all K8s nodes for the ConfigMap snapshot
// pacemakerNodes: current pacemaker membership (name -> IP) from PacemakerCluster CR
func updateSetup(
	validTargetNodes []*corev1.Node,
	allK8sNodes []*corev1.Node,
	pacemakerNodes map[string]string,
	ctx context.Context,
	controllerContext *controllercmd.ControllerContext,
	operatorClient v1helpers.StaticPodOperatorClient,
	kubeClient kubernetes.Interface,
	kubeInformersForNamespaces v1helpers.KubeInformersForNamespaces,
) error {

	// Pick target node from valid nodes (first in list)
	if len(validTargetNodes) == 0 {
		return fmt.Errorf("no valid target nodes for update-setup - manual intervention may be required")
	}
	targetNode := validTargetNodes[0]

	// Encode desired state for comparison
	nodeListData, err := encodeNodeList(allK8sNodes)
	if err != nil {
		return fmt.Errorf("failed to encode node list: %w", err)
	}

	// Check if current generation matches desired state - if so, reuse it instead of creating new one
	currentGeneration := getCurrentUpdateSetupGeneration()
	var generation int64
	var shouldCreateNewConfigMap bool

	if currentGeneration > 0 {
		currentCMName := fmt.Sprintf("tnf-update-setup-%d", currentGeneration)
		currentCM, err := kubeClient.CoreV1().ConfigMaps(operatorclient.TargetNamespace).Get(ctx, currentCMName, v1.GetOptions{})
		if err == nil {
			// Compare desired state with current ConfigMap
			if currentCM.Data["nodes"] == nodeListData && currentCM.Data["targetNode"] == targetNode.Name {
				klog.Infof("Desired state matches current generation %d - reusing existing ConfigMap", currentGeneration)
				generation = currentGeneration
				shouldCreateNewConfigMap = false
			} else {
				klog.Infof("Desired state differs from generation %d - creating new generation", currentGeneration)
				generation = getNextUpdateSetupGeneration()
				shouldCreateNewConfigMap = true
			}
		} else {
			// Current ConfigMap doesn't exist (deleted or error) - create new
			klog.V(2).Infof("Current generation %d ConfigMap not found: %v - creating new generation", currentGeneration, err)
			generation = getNextUpdateSetupGeneration()
			shouldCreateNewConfigMap = true
		}
	} else {
		// First generation
		generation = getNextUpdateSetupGeneration()
		shouldCreateNewConfigMap = true
	}

	klog.Infof("Generation %d: Target=%s, ValidTargets=%v, AllNodes=%v",
		generation, targetNode.Name, getNodeNames(validTargetNodes), getNodeNames(allK8sNodes))

	cmName := fmt.Sprintf("tnf-update-setup-%d", generation)

	// Only create ConfigMap if desired state differs from current generation
	if shouldCreateNewConfigMap {
		cmData := map[string]string{
			"nodes":      nodeListData,    // Desired state: which K8s nodes should be in pacemaker
			"targetNode": targetNode.Name, // Which node should run the update-setup job
			"generation": fmt.Sprintf("%d", generation),
			"timestamp":  time.Now().Format(time.RFC3339),
		}
		cm := &corev1.ConfigMap{
			ObjectMeta: v1.ObjectMeta{
				Name:      cmName,
				Namespace: operatorclient.TargetNamespace,
				Labels: map[string]string{
					"app.kubernetes.io/component":      tools.TnfUpdateSetupComponentValue,
					"tnf.etcd.openshift.io/generation": fmt.Sprintf("%d", generation),
				},
			},
			Data: cmData,
		}

		_, err = kubeClient.CoreV1().ConfigMaps(operatorclient.TargetNamespace).Create(ctx, cm, v1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create ConfigMap %s: %w", cmName, err)
		}
		klog.Infof("Created new ConfigMap %s for generation %d", cmName, generation)
	}

	// Clean up ConfigMap after all jobs complete
	defer func() {
		if err := kubeClient.CoreV1().ConfigMaps(operatorclient.TargetNamespace).Delete(ctx, cmName, v1.DeleteOptions{}); err != nil {
			klog.Warningf("failed to delete ConfigMap %s: %v", cmName, err)
		}
	}()

	// Run auth jobs on all nodes to ensure pacemaker authentication is current
	if err := runJobsOnNodes(ctx, tools.JobTypeAuth, allK8sNodes, controllerContext, operatorClient, kubeClient, kubeInformersForNamespaces); err != nil {
		return err
	}

	// Run update-setup job on target node
	// This is a cluster-wide operation (not tied to node lifecycle), but scheduled on a pacemaker-active node
	timeout := getJobTimeout(tools.JobTypeUpdateSetup)
	if err := jobs.RestartJobOrRunController(ctx, tools.JobTypeUpdateSetup, nil, &targetNode.Name,
		controllerContext, operatorClient, kubeClient, kubeInformersForNamespaces,
		jobs.DefaultConditions, timeout); err != nil {
		return fmt.Errorf("failed to start update-setup job: %w", err)
	}
	if err := jobs.WaitForCompletion(ctx, kubeClient, tools.JobTypeUpdateSetup.GetJobName(nil),
		operatorclient.TargetNamespace, timeout); err != nil {
		return fmt.Errorf("failed to wait for update-setup job: %w", err)
	}

	// Run after-setup jobs on all nodes for post-reconciliation tasks
	if err := runJobsOnNodes(ctx, tools.JobTypeAfterSetup, allK8sNodes, controllerContext, operatorClient, kubeClient, kubeInformersForNamespaces); err != nil {
		return err
	}

	return nil
}

// runJobsOnNodes runs a node-specific job type on the given nodes and waits for completion.
// This is used for auth and after-setup jobs that are tied to individual node lifecycles.
func runJobsOnNodes(
	ctx context.Context,
	jobType tools.JobType,
	nodes []*corev1.Node,
	controllerContext *controllercmd.ControllerContext,
	operatorClient v1helpers.StaticPodOperatorClient,
	kubeClient kubernetes.Interface,
	kubeInformersForNamespaces v1helpers.KubeInformersForNamespaces,
) error {
	timeout := getJobTimeout(jobType)

	for _, node := range nodes {
		nodeTarget := &jobs.NodeTarget{Name: node.Name, UID: string(node.UID)}
		if err := jobs.RestartJobOrRunController(ctx, jobType, nodeTarget, nil,
			controllerContext, operatorClient, kubeClient, kubeInformersForNamespaces,
			jobs.DefaultConditions, timeout); err != nil {
			return fmt.Errorf("failed to start %s job on node %s: %w", jobType.GetSubCommand(), node.Name, err)
		}
	}

	for _, node := range nodes {
		if err := jobs.WaitForCompletion(ctx, kubeClient, jobType.GetJobName(&node.Name),
			operatorclient.TargetNamespace, timeout); err != nil {
			return fmt.Errorf("failed to wait for %s job on node %s: %w", jobType.GetSubCommand(), node.Name, err)
		}
	}

	return nil
}

// buildK8sNodeMap builds a map of node name to IP from K8s nodes.
func buildK8sNodeMap(nodes []*corev1.Node) map[string]string {
	m := make(map[string]string)
	for _, node := range nodes {
		ip, err := tools.GetNodeIPForPacemaker(*node)
		if err != nil {
			klog.Warningf("failed to get IP for node %s: %v - skipping", node.Name, err)
			continue
		}
		m[node.Name] = ip
	}
	return m
}

// getJobTimeout returns the appropriate timeout for a given job type.
func getJobTimeout(jobType tools.JobType) time.Duration {
	switch jobType {
	case tools.JobTypeAuth:
		return tools.AuthJobCompletedTimeout
	case tools.JobTypeUpdateSetup:
		return tools.SetupJobCompletedTimeout
	case tools.JobTypeAfterSetup:
		return tools.AfterSetupJobCompletedTimeout
	default:
		return tools.AllCompletedTimeout
	}
}

// encodeNodeList encodes a list of nodes to JSON.
func encodeNodeList(nodes []*corev1.Node) (string, error) {
	type nodeInfo struct {
		Name              string               `json:"name"`
		CreationTimestamp v1.Time              `json:"creationTimestamp"`
		Labels            map[string]string    `json:"labels"`
		Addresses         []corev1.NodeAddress `json:"addresses"`
	}

	infos := make([]nodeInfo, len(nodes))
	for i, node := range nodes {
		infos[i] = nodeInfo{
			Name:              node.Name,
			CreationTimestamp: node.CreationTimestamp,
			Labels:            node.Labels,
			Addresses:         node.Status.Addresses,
		}
	}

	data, err := json.Marshal(infos)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// encodeStringList encodes a list of strings to JSON.
func encodeStringList(list []string) string {
	if len(list) == 0 {
		return "[]"
	}
	data, err := json.Marshal(list)
	if err != nil {
		// Should never happen for []string
		klog.Errorf("failed to encode string list: %v", err)
		return "[]"
	}
	return string(data)
}
