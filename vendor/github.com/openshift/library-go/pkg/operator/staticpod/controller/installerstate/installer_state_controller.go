package installerstate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"

	operatorv1 "github.com/openshift/api/operator/v1"
	applyoperatorv1 "github.com/openshift/client-go/operator/applyconfigurations/operator/v1"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
)

const installerStateControllerWorkQueueKey = "key"

// maxToleratedPodPendingDuration is the maximum time we tolerate installer pod in pending state
var maxToleratedPodPendingDuration = 5 * time.Minute

// InstallerStateController analyzes installer pods and sets degraded conditions suggesting different root causes.
type InstallerStateController struct {
	controllerInstanceName string
	podsGetter             corev1client.PodsGetter
	eventsGetter           corev1client.EventsGetter
	targetNamespace        string
	operatorClient         v1helpers.StaticPodOperatorClient

	timeNowFn func() time.Time
}

func NewInstallerStateController(instanceName string,
	kubeInformersForTargetNamespace informers.SharedInformerFactory,
	podsGetter corev1client.PodsGetter,
	eventsGetter corev1client.EventsGetter,
	operatorClient v1helpers.StaticPodOperatorClient,
	targetNamespace string,
	recorder events.Recorder,
) factory.Controller {
	c := &InstallerStateController{
		controllerInstanceName: factory.ControllerInstanceName(instanceName, "InstallerState"),
		podsGetter:             podsGetter,
		eventsGetter:           eventsGetter,
		targetNamespace:        targetNamespace,
		operatorClient:         operatorClient,
		timeNowFn:              time.Now,
	}

	return factory.New().
		WithInformers(kubeInformersForTargetNamespace.Core().V1().Pods().Informer()).
		WithSync(c.sync).
		ResyncEvery(1*time.Minute).
		WithControllerInstanceName(c.controllerInstanceName).
		ToController(
			c.controllerInstanceName,
			recorder,
		)
}

// degradedConditionNames lists all supported condition types.
var degradedConditionNames = []string{
	"InstallerPodPendingDegraded",
	"InstallerPodContainerWaitingDegraded",
	"InstallerPodNetworkingDegraded",
}

func installerNameToRevision(name string) (int, error) {
	parts := strings.Split(name, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("Installer name %v is invalid, missing revision number", name)
	}
	return strconv.Atoi(parts[1])
}

func (c *InstallerStateController) sync(ctx context.Context, syncCtx factory.SyncContext) error {
	pods, err := c.podsGetter.Pods(c.targetNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{"app": "installer"}).String(),
	})
	if err != nil {
		return err
	}

	masterRevisions := make(map[string][]*v1.Pod)
	installerHighestRunningRevision := make(map[string]int)
	for _, pod := range pods.Items {
		p := pod
		masterRevisions[pod.Spec.NodeName] = append(masterRevisions[pod.Spec.NodeName], &p)
	}
	// find the highest revision of a non-pending pod on each master node
	for masterNode, pods := range masterRevisions {
		maxRunningRev := 0
		for _, pod := range pods {
			if pod.Status.Phase != v1.PodPending || pod.Status.StartTime == nil {
				rev, err := installerNameToRevision(pod.Name)
				if err != nil {
					return err
				}
				if rev > maxRunningRev {
					maxRunningRev = rev
				}
			}
		}
		installerHighestRunningRevision[masterNode] = maxRunningRev
	}

	// collect all startingObjects that are in pending state for longer than maxToleratedPodPendingDuration
	pendingPods := []*v1.Pod{}
	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodPending || pod.Status.StartTime == nil {
			continue
		}
		if rev, _ := installerNameToRevision(pod.Name); c.timeNowFn().Sub(pod.Status.StartTime.Time) >= maxToleratedPodPendingDuration && rev >= installerHighestRunningRevision[pod.Spec.NodeName] {
			pendingPods = append(pendingPods, pod.DeepCopy())
		}
	}

	// handle pending installer pods conditions
	foundConditions := []operatorv1.OperatorCondition{}
	foundConditions = append(foundConditions, c.handlePendingInstallerPods(syncCtx.Recorder(), pendingPods)...)

	// handle networking conditions that are based on events
	networkConditions, err := c.handlePendingInstallerPodsNetworkEvents(ctx, syncCtx.Recorder(), pendingPods)
	if err != nil {
		return err
	}
	foundConditions = append(foundConditions, networkConditions...)

	updateConditions := []*applyoperatorv1.OperatorConditionApplyConfiguration{}
	// check the supported degraded foundConditions and check if any pending pod matching them.
	for _, degradedConditionName := range degradedConditionNames {
		// clean up existing foundConditions
		updatedCondition := applyoperatorv1.OperatorCondition().
			WithType(degradedConditionName).
			WithStatus(operatorv1.ConditionFalse)

		if condition := v1helpers.FindOperatorCondition(foundConditions, degradedConditionName); condition != nil {
			updatedCondition = updatedCondition.
				WithStatus(condition.Status).
				WithReason(condition.Reason).
				WithMessage(condition.Message)
		}
		updateConditions = append(updateConditions, updatedCondition)
	}

	status := applyoperatorv1.StaticPodOperatorStatus().WithConditions(updateConditions...)
	return c.operatorClient.ApplyStaticPodOperatorStatus(ctx, c.controllerInstanceName, status)
}

func (c *InstallerStateController) handlePendingInstallerPodsNetworkEvents(ctx context.Context, recorder events.Recorder, pods []*v1.Pod) ([]operatorv1.OperatorCondition, error) {
	conditions := []operatorv1.OperatorCondition{}
	if len(pods) == 0 {
		return conditions, nil
	}
	namespaceEvents, err := c.eventsGetter.Events(c.targetNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, event := range namespaceEvents.Items {
		if event.InvolvedObject.Kind != "Pod" {
			continue
		}
		if !strings.Contains(event.Message, "failed to create pod network") {
			continue
		}
		for _, pod := range pods {
			if pod.Name != event.InvolvedObject.Name {
				continue
			}
			// If we already find the pod that is pending because of the networking problem, skip other pods.
			// This will reduce the events we fire.
			if c := v1helpers.FindOperatorCondition(conditions, "InstallerPodNetworkingDegraded"); c != nil && c.Status == operatorv1.ConditionTrue {
				break
			}
			condition := operatorv1.OperatorCondition{
				Type:    "InstallerPodNetworkingDegraded",
				Status:  operatorv1.ConditionTrue,
				Reason:  event.Reason,
				Message: fmt.Sprintf("Pod %q on node %q observed degraded networking: %s", pod.Name, pod.Spec.NodeName, event.Message),
			}
			conditions = append(conditions, condition)
			recorder.Warningf(condition.Reason, condition.Message)
		}
	}
	return conditions, nil
}

func (c *InstallerStateController) handlePendingInstallerPods(recorder events.Recorder, pods []*v1.Pod) []operatorv1.OperatorCondition {
	conditions := []operatorv1.OperatorCondition{}
	for _, pod := range pods {
		// the pod is in the pending state for longer than maxToleratedPodPendingDuration, report the reason and message
		// as degraded condition for the operator.
		if len(pod.Status.Reason) > 0 {
			condition := operatorv1.OperatorCondition{
				Type:    "InstallerPodPendingDegraded",
				Reason:  pod.Status.Reason,
				Status:  operatorv1.ConditionTrue,
				Message: fmt.Sprintf("Pod %q on node %q is Pending since %s because %s", pod.Name, pod.Spec.NodeName, pod.Status.StartTime.Time, pod.Status.Message),
			}
			conditions = append(conditions, condition)
			recorder.Warningf(condition.Reason, condition.Message)
		}

		// one or more containers are in waiting state for longer than maxToleratedPodPendingDuration, report the reason and message
		// as degraded condition for the operator.
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.State.Waiting == nil {
				continue
			}
			if state := containerStatus.State.Waiting; len(state.Reason) > 0 {
				message := fmt.Sprintf("Pod %q on node %q container %q is waiting since %s because", pod.Name, pod.Spec.NodeName, containerStatus.Name, pod.Status.StartTime.Time)
				if len(state.Message) > 0 {
					message = fmt.Sprintf("%s %q", message, state.Message)
				} else {
					message = fmt.Sprintf("%s %s", message, state.Reason)
				}
				condition := operatorv1.OperatorCondition{
					Type:    "InstallerPodContainerWaitingDegraded",
					Reason:  state.Reason,
					Status:  operatorv1.ConditionTrue,
					Message: message,
				}
				conditions = append(conditions, condition)
				recorder.Warningf(condition.Reason, condition.Message)
			}
		}
	}

	return conditions
}
