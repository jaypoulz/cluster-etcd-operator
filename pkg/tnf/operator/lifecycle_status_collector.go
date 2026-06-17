package operator

import (
	"context"
	"fmt"
	"os"

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
// 1. If PacemakerCluster CR exists and is fresh, use intersection logic (K8s ∩ pacemaker)
// 2. Otherwise, use deterministic pseudo-random selection based on CronJob run count
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

	// Try to get pacemaker nodes from CR (if CR exists and is fresh)
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
		// No intersection - fall through to random selection
		klog.Warningf("No nodes in intersection (K8s ∩ pacemaker) - using deterministic selection")
	}

	// CR doesn't exist or no intersection - use deterministic pseudo-random selection
	// Count existing Jobs created by the CronJob to get run count
	runCount, err := getStatusCollectorRunCount(kubeClient)
	if err != nil {
		klog.Warningf("Failed to get run count: %v - defaulting to first node", err)
		return readyNodes[0].Name, nil
	}

	// Use modulo to select node deterministically (alternates between nodes)
	nodeIndex := runCount % len(readyNodes)
	targetNode := readyNodes[nodeIndex].Name
	klog.Infof("Scheduling status collector on node %s (run count %d, index %d of %d nodes)", targetNode, runCount, nodeIndex, len(readyNodes))
	return targetNode, nil
}

// getStatusCollectorRunCount returns the number of times the status collector has run
// by counting Jobs created by the CronJob (both active and completed).
func getStatusCollectorRunCount(kubeClient kubernetes.Interface) (int, error) {
	jobList, err := kubeClient.BatchV1().Jobs(operatorclient.TargetNamespace).List(
		context.Background(),
		metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/name=" + pacemakerStatusCollectorName,
		},
	)
	if err != nil {
		return 0, fmt.Errorf("failed to list status collector Jobs: %w", err)
	}

	return len(jobList.Items), nil
}
