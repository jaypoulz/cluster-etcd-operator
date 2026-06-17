package operator

import (
	"context"
	"fmt"
	"os"
	"time"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/cluster-etcd-operator/bindata"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/jobs"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/tools"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

const (
	// pacemakerStatusCollectorName is the name of the Pacemaker status collector CronJob
	pacemakerStatusCollectorName = "pacemaker-status-collector"
)

// TargetNodeSelectorFunc is a function type that selects a target node for the status collector.
// It receives the list of control plane nodes and the kubeClient to query state.
type TargetNodeSelectorFunc func(k8sNodes []*corev1.Node, kubeClient kubernetes.Interface) (string, error)

// runPacemakerStatusCollectorCronJob starts the CronJob controller for periodic pacemaker status collection.
// The CronJob runs "tnf-monitor collect" which executes "sudo -n pcs status xml" and updates the PacemakerCluster CR.
func runPacemakerStatusCollectorCronJob(
	ctx context.Context,
	controllerContext *controllercmd.ControllerContext,
	operatorClient v1helpers.StaticPodOperatorClient,
	kubeClient kubernetes.Interface,
	lifecycleManager *PacemakerLifecycleManager,
	nodeInformer cache.SharedIndexInformer,
) {
	// Create target node selector function that will be called on each CronJob sync
	targetNodeSelector := func(k8sNodes []*corev1.Node, client kubernetes.Interface) (string, error) {
		return selectStatusCollectorTargetNode(k8sNodes, client, lifecycleManager)
	}

	// Start the cronjob controller to create a CronJob for periodic status collection.
	statusCronJobController := jobs.NewCronJobController(
		pacemakerStatusCollectorName,
		bindata.MustAsset("tnfdeployment/cronjob.yaml"),
		operatorClient,
		kubeClient,
		controllerContext.EventRecorder,
		func(_ *operatorv1.OperatorSpec, cronJob *batchv1.CronJob) error {
			// Set the name and namespace
			cronJob.SetName(pacemakerStatusCollectorName)
			cronJob.SetNamespace(operatorclient.TargetNamespace)

			// Set the schedule - run every minute
			cronJob.Spec.Schedule = "* * * * *"

			// Initialize labels maps if nil and set labels at all levels
			if cronJob.Labels == nil {
				cronJob.Labels = make(map[string]string)
			}
			cronJob.Labels["app.kubernetes.io/name"] = pacemakerStatusCollectorName

			if cronJob.Spec.JobTemplate.Labels == nil {
				cronJob.Spec.JobTemplate.Labels = make(map[string]string)
			}
			cronJob.Spec.JobTemplate.Labels["app.kubernetes.io/name"] = pacemakerStatusCollectorName

			if cronJob.Spec.JobTemplate.Spec.Template.Labels == nil {
				cronJob.Spec.JobTemplate.Spec.Template.Labels = make(map[string]string)
			}
			cronJob.Spec.JobTemplate.Spec.Template.Labels["app.kubernetes.io/name"] = pacemakerStatusCollectorName

			// Configure the container
			cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image = os.Getenv("OPERATOR_IMAGE")
			cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command = []string{"tnf-monitor", "collect", "-v=4"}

			// Get K8s control plane nodes from informer
			k8sNodes := []*corev1.Node{}
			for _, obj := range nodeInformer.GetStore().List() {
				node, ok := obj.(*corev1.Node)
				if !ok {
					klog.Warningf("Failed to convert object to Node: %+v", obj)
					continue
				}
				// Only consider master nodes
				if _, isMaster := node.Labels["node-role.kubernetes.io/master"]; isMaster {
					k8sNodes = append(k8sNodes, node)
				}
			}

			// Get target node using the selector function
			targetNode, err := targetNodeSelector(k8sNodes, kubeClient)
			if err != nil {
				klog.Warningf("Failed to determine target node for status collector: %v - falling back to nodeSelector only", err)
				// On error, don't set NodeName - let scheduler decide based on nodeSelector/affinity
				return nil
			}

			// Set NodeName to schedule on specific node
			klog.Infof("Status collector will run on node: %s", targetNode)
			cronJob.Spec.JobTemplate.Spec.Template.Spec.NodeName = targetNode

			return nil
		},
	)
	go statusCronJobController.Run(ctx, 1)
}

// selectStatusCollectorTargetNode selects a target node for the status collector CronJob.
// Strategy:
// 1. If PacemakerCluster CR exists, use intersection logic (K8s ∩ pacemaker)
// 2. Otherwise, check for recent failed Jobs and try the other node
// 3. Default to first ready node
func selectStatusCollectorTargetNode(
	k8sNodes []*corev1.Node,
	kubeClient kubernetes.Interface,
	lifecycleManager *PacemakerLifecycleManager,
) (string, error) {
	if len(k8sNodes) == 0 {
		return "", fmt.Errorf("no control plane nodes found")
	}

	// Filter to Ready nodes only
	readyNodes := []*corev1.Node{}
	for _, node := range k8sNodes {
		if tools.IsNodeReady(node) {
			readyNodes = append(readyNodes, node)
		}
	}

	if len(readyNodes) == 0 {
		return "", fmt.Errorf("no ready control plane nodes found")
	}

	// Try to get pacemaker nodes from CR
	pacemakerNodes, err := lifecycleManager.getPacemakerNodes()
	if err == nil {
		// CR exists - use intersection logic
		intersection := []*corev1.Node{}
		for _, k8sNode := range readyNodes {
			if _, exists := pacemakerNodes[k8sNode.Name]; exists {
				intersection = append(intersection, k8sNode)
			}
		}

		if len(intersection) > 0 {
			// Use first node in intersection (both in K8s and running pacemaker)
			targetNode := intersection[0].Name
			klog.V(4).Infof("Scheduling status collector on intersection node: %s (from %d candidate nodes)", targetNode, len(intersection))
			return targetNode, nil
		}
		// No intersection - fall through to failure-based selection
		klog.Warningf("No nodes in intersection (K8s ∩ pacemaker) - checking for failed Jobs")
	}

	// CR doesn't exist or no intersection - check if we have a recent failed Job
	// and try the other node if so
	failedJobNode, err := getRecentFailedJobNode(kubeClient)
	if err != nil {
		klog.V(4).Infof("Failed to check for recent failed Jobs: %v - defaulting to first node", err)
		return readyNodes[0].Name, nil
	}

	if failedJobNode != "" {
		// We have a recent failure - try a different node
		for _, node := range readyNodes {
			if node.Name != failedJobNode {
				klog.Infof("Recent Job failed on %s - trying node %s instead", failedJobNode, node.Name)
				return node.Name, nil
			}
		}
		klog.Warningf("Recent Job failed on %s but no other ready nodes available", failedJobNode)
	}

	// No recent failures or couldn't determine - use first ready node
	targetNode := readyNodes[0].Name
	klog.V(4).Infof("Scheduling status collector on first ready node: %s", targetNode)
	return targetNode, nil
}

// getRecentFailedJobNode returns the node name where a recent failed status collector Job ran.
// Returns empty string if no recent failed Job exists.
// "Recent" means failed Jobs from the last 5 minutes to avoid switching back and forth too quickly.
func getRecentFailedJobNode(kubeClient kubernetes.Interface) (string, error) {
	jobList, err := kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).List(
		context.Background(),
		metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/name=" + pacemakerStatusCollectorName,
		},
	)
	if err != nil {
		return "", fmt.Errorf("failed to list status collector Jobs: %w", err)
	}

	// Look for failed Jobs in the last 5 minutes
	recentFailureThreshold := time.Now().Add(-5 * time.Minute)

	for i := range jobList.Items {
		job := &jobList.Items[i]

		// Check if Job failed
		failed := false
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
				failed = true
				break
			}
		}

		if !failed {
			continue
		}

		// Check if failure is recent
		if job.Status.CompletionTime != nil && job.Status.CompletionTime.Time.After(recentFailureThreshold) {
			nodeName := job.Spec.Template.Spec.NodeName
			if nodeName != "" {
				klog.V(4).Infof("Found recent failed Job %s on node %s (failed at %v)", job.Name, nodeName, job.Status.CompletionTime)
				return nodeName, nil
			}
		}
	}

	return "", nil
}
