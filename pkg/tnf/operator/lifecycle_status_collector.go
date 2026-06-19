package operator

import (
	"context"
	"os"
	"sort"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/cluster-etcd-operator/bindata"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/operatorclient"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/jobs"
	"github.com/openshift/library-go/pkg/controller/controllercmd"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// pacemakerStatusCollectorName is the name of the Pacemaker status collector CronJob
	pacemakerStatusCollectorName = "pacemaker-status-collector"
)

// runPacemakerStatusCollectorCronJob starts the CronJob controller for periodic pacemaker status collection.
// The CronJob runs "tnf-monitor collect" which executes "sudo -n pcs status xml" and updates the PacemakerCluster CR.
// validNodeFunc is called on each sync to determine which nodes are valid targets for the collector.
func runPacemakerStatusCollectorCronJob(
	ctx context.Context,
	controllerContext *controllercmd.ControllerContext,
	operatorClient v1helpers.StaticPodOperatorClient,
	kubeClient kubernetes.Interface,
	validNodeFunc jobs.TargetNodesFunc,
) {
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

			// Get valid nodes using the provided function (same logic as update-setup job)
			validNodes, err := validNodeFunc()
			if err != nil || len(validNodes) == 0 {
				klog.Warningf("Failed to determine valid nodes for status collector: %v - falling back to nodeSelector from manifest", err)
				// On error, rely on existing nodeSelector from manifest
				return nil
			}

			// Extract and sort node names for deterministic spec comparison
			nodeNames := make([]string, len(validNodes))
			for i, node := range validNodes {
				nodeNames[i] = node.Name
			}
			sort.Strings(nodeNames)

			// Set node affinity to constrain jobs to valid nodes (let scheduler choose)
			// This eliminates the flip-flopping problem caused by pinning to a specific NodeName
			cronJob.Spec.JobTemplate.Spec.Template.Spec.Affinity = &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "kubernetes.io/hostname",
										Operator: corev1.NodeSelectorOpIn,
										Values:   nodeNames,
									},
								},
							},
						},
					},
				},
			}

			klog.V(4).Infof("Status collector configured with node affinity for nodes: %v", nodeNames)
			return nil
		},
	)
	go statusCronJobController.Run(ctx, 1)
}
