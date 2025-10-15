package pacemaker

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	v1alpha1 "github.com/openshift/api/etcd/v1alpha1"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/exec"
)

const (
	// pcsStatusXMLCommand is the command to get pacemaker status in XML format.
	// Security: This is a hardcoded string (no user input) and runs with sudo -n (non-interactive).
	// The command is whitelisted in sudoers configuration for the service account.
	pcsStatusXMLCommand = "sudo -n pcs status xml"

	// execTimeout is the timeout for executing the pcs command to prevent hanging
	execTimeout = 10 * time.Second

	// collectorTimeout is the overall timeout for the collector run
	collectorTimeout = 30 * time.Second

	// maxXMLSize prevents XML bombs and excessive memory consumption (10MB limit)
	maxXMLSize = 10 * 1024 * 1024

	// Time windows for detecting recent events
	failedActionTimeWindow = 5 * time.Minute
	fencingEventTimeWindow = 24 * time.Hour

	// Pacemaker state strings
	booleanValueTrue  = "true"
	booleanValueFalse = "false"
	nodeStatusStarted = "Started"

	// Node attribute names
	nodeAttributeIP = "node_ip"

	// Time formats for parsing Pacemaker timestamps
	pacemakerTimeFormat      = "Mon Jan 2 15:04:05 2006"
	pacemakerFenceTimeFormat = "2006-01-02 15:04:05.000000Z"

	// Kubernetes API constants (kubernetesAPIPath and pacemakerResourceName shared with healthcheck.go)
	statusSubresource = "status"
	pacemakerKind     = "PacemakerStatus"

	// Environment variables
	envKubeconfig         = "KUBECONFIG"
	envHome               = "HOME"
	defaultKubeconfigPath = "/.kube/config"
)

// XML structures for parsing pacemaker status from "pcs status xml" command output.
// The healthcheck controller does not parse XML - it reads structured data from the PacemakerStatus CR.
type PacemakerResult struct {
	XMLName        xml.Name       `xml:"pacemaker-result"`
	Summary        Summary        `xml:"summary"`
	Nodes          Nodes          `xml:"nodes"`
	Resources      Resources      `xml:"resources"`
	NodeAttributes NodeAttributes `xml:"node_attributes"`
	NodeHistory    NodeHistory    `xml:"node_history"`
	FenceHistory   FenceHistory   `xml:"fence_history"`
}

type Summary struct {
	Stack               Stack               `xml:"stack"`
	CurrentDC           CurrentDC           `xml:"current_dc"`
	NodesConfigured     NodesConfigured     `xml:"nodes_configured"`
	ResourcesConfigured ResourcesConfigured `xml:"resources_configured"`
}

type Stack struct {
	PacemakerdState string `xml:"pacemakerd-state,attr"`
}

type CurrentDC struct {
	WithQuorum string `xml:"with_quorum,attr"`
}

type NodesConfigured struct {
	Number string `xml:"number,attr"`
}

type ResourcesConfigured struct {
	Number string `xml:"number,attr"`
}

type Nodes struct {
	Node []Node `xml:"node"`
}

type Node struct {
	Name             string `xml:"name,attr"`
	ID               string `xml:"id,attr"`
	Online           string `xml:"online,attr"`
	Standby          string `xml:"standby,attr"`
	StandbyOnFail    string `xml:"standby_onfail,attr"`
	Maintenance      string `xml:"maintenance,attr"`
	Pending          string `xml:"pending,attr"`
	Unclean          string `xml:"unclean,attr"`
	Shutdown         string `xml:"shutdown,attr"`
	ExpectedUp       string `xml:"expected_up,attr"`
	IsDC             string `xml:"is_dc,attr"`
	ResourcesRunning string `xml:"resources_running,attr"`
	Type             string `xml:"type,attr"`
}

type NodeAttributes struct {
	Node []NodeAttributeSet `xml:"node"`
}

type NodeAttributeSet struct {
	Name      string          `xml:"name,attr"`
	Attribute []NodeAttribute `xml:"attribute"`
}

type NodeAttribute struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

type Resources struct {
	Clone    []Clone    `xml:"clone"`
	Resource []Resource `xml:"resource"`
}

type Clone struct {
	Resource []Resource `xml:"resource"`
}

type Resource struct {
	ID             string  `xml:"id,attr"`
	ResourceAgent  string  `xml:"resource_agent,attr"`
	Role           string  `xml:"role,attr"`
	TargetRole     string  `xml:"target_role,attr"`
	Active         string  `xml:"active,attr"`
	Orphaned       string  `xml:"orphaned,attr"`
	Blocked        string  `xml:"blocked,attr"`
	Managed        string  `xml:"managed,attr"`
	Failed         string  `xml:"failed,attr"`
	FailureIgnored string  `xml:"failure_ignored,attr"`
	NodesRunningOn string  `xml:"nodes_running_on,attr"`
	Node           NodeRef `xml:"node"`
}

type NodeRef struct {
	Name string `xml:"name,attr"`
}

type NodeHistory struct {
	Node []NodeHistoryNode `xml:"node"`
}

type NodeHistoryNode struct {
	Name            string            `xml:"name,attr"`
	ResourceHistory []ResourceHistory `xml:"resource_history"`
}

type ResourceHistory struct {
	ID               string             `xml:"id,attr"`
	OperationHistory []OperationHistory `xml:"operation_history"`
}

type OperationHistory struct {
	Call         string `xml:"call,attr"`
	Task         string `xml:"task,attr"`
	RC           string `xml:"rc,attr"`
	RCText       string `xml:"rc_text,attr"`
	ExitReason   string `xml:"exit-reason,attr"`
	LastRCChange string `xml:"last-rc-change,attr"`
	ExecTime     string `xml:"exec-time,attr"`
	QueueTime    string `xml:"queue-time,attr"`
}

type FenceHistory struct {
	FenceEvent []FenceEvent `xml:"fence_event"`
}

type FenceEvent struct {
	Target     string `xml:"target,attr"`
	Action     string `xml:"action,attr"`
	Delegate   string `xml:"delegate,attr"`
	Client     string `xml:"client,attr"`
	Origin     string `xml:"origin,attr"`
	Status     string `xml:"status,attr"`
	ExitReason string `xml:"exit-reason,attr"`
	Completed  string `xml:"completed,attr"`
}

// NewPacemakerStatusCollectorCommand creates a new command for collecting pacemaker status
func NewPacemakerStatusCollectorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pacemaker-status-collector",
		Short: "Collects pacemaker status and updates PacemakerStatus CR",
		Run: func(cmd *cobra.Command, args []string) {
			if err := runCollector(); err != nil {
				klog.Errorf("Failed to collect pacemaker status: %v", err)
				os.Exit(1)
			}
		},
	}
	return cmd
}

// runCollector executes the full pacemaker status collection workflow:
// 1. Executes "sudo -n pcs status xml" to get cluster status
// 2. Parses the XML output into structured data
// 3. Updates or creates the PacemakerStatus CR with the collected information
func runCollector() error {
	ctx, cancel := context.WithTimeout(context.Background(), collectorTimeout)
	defer cancel()

	klog.Info("Starting pacemaker status collection...")

	// Collect pacemaker status
	rawXML, summary, nodes, resources, nodeHistory, fencingHistory, collectionError := collectPacemakerStatus(ctx)

	// Update PacemakerStatus CR
	if err := updatePacemakerStatusCR(ctx, rawXML, summary, nodes, resources, nodeHistory, fencingHistory, collectionError); err != nil {
		return fmt.Errorf("failed to update PacemakerStatus CR: %w", err)
	}

	klog.Info("Successfully updated PacemakerStatus CR")
	return nil
}

// collectPacemakerStatus executes "pcs status xml" and parses the output into structured data.
// Returns the raw XML, parsed status components, and any error encountered during collection.
// If an error occurs, collectionError will contain the error message and other return values
// will be zero/empty values.
func collectPacemakerStatus(ctx context.Context) (
	rawXML string,
	summary *v1alpha1.PacemakerSummary,
	nodes []v1alpha1.NodeStatus,
	resources []v1alpha1.ResourceStatus,
	nodeHistory []v1alpha1.NodeHistoryEntry,
	fencingHistory []v1alpha1.FencingEvent,
	collectionError string,
) {
	// Execute the pcs status xml command with a timeout
	ctxExec, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	stdout, stderr, err := exec.Execute(ctxExec, pcsStatusXMLCommand)
	if err != nil {
		collectionError = fmt.Sprintf("Failed to execute pcs status xml command: %v", err)
		klog.Warning(collectionError)
		return "", summary, nil, nil, nil, nil, collectionError
	}

	if stderr != "" {
		klog.Warningf("pcs status xml command produced stderr: %s", stderr)
	}

	// Validate XML size to prevent XML bombs
	if len(stdout) > maxXMLSize {
		collectionError = fmt.Sprintf("XML output too large: %d bytes (max: %d bytes)", len(stdout), maxXMLSize)
		klog.Warning(collectionError)
		return "", summary, nil, nil, nil, nil, collectionError
	}

	rawXML = stdout

	// Parse the XML to create a summary
	var result PacemakerResult
	if err := xml.Unmarshal([]byte(rawXML), &result); err != nil {
		collectionError = fmt.Sprintf("Failed to parse XML: %v", err)
		klog.Warning(collectionError)
		// Still return the raw XML even if parsing fails
		return rawXML, summary, nil, nil, nil, nil, collectionError
	}

	// Build all status components
	summary, nodes, resources, nodeHistory, fencingHistory = buildStatusComponents(&result)

	return rawXML, summary, nodes, resources, nodeHistory, fencingHistory, ""
}

// buildStatusComponents parses a PacemakerResult (XML) into structured status components.
// It extracts summary information, node status, resource status, and recent history.
// Historical data is filtered to recent time windows (5 minutes for operations, 24 hours for fencing).
func buildStatusComponents(result *PacemakerResult) (
	summary *v1alpha1.PacemakerSummary,
	nodes []v1alpha1.NodeStatus,
	resources []v1alpha1.ResourceStatus,
	nodeHistory []v1alpha1.NodeHistoryEntry,
	fencingHistory []v1alpha1.FencingEvent,
) {
	// Build high-level summary
	// Convert quorum boolean to typed constant
	quorumStatus := v1alpha1.QuorumStatusNoQuorum
	if result.Summary.CurrentDC.WithQuorum == booleanValueTrue {
		quorumStatus = v1alpha1.QuorumStatusQuorate
	}

	summary = &v1alpha1.PacemakerSummary{
		PacemakerdState: result.Summary.Stack.PacemakerdState,
		QuorumStatus:    quorumStatus,
	}

	// Build node IP map from node attributes
	nodeIPMap := make(map[string]string)
	for _, nodeAttrSet := range result.NodeAttributes.Node {
		for _, attr := range nodeAttrSet.Attribute {
			if attr.Name == nodeAttributeIP {
				nodeIPMap[nodeAttrSet.Name] = attr.Value
				break
			}
		}
	}

	// Build detailed node information
	onlineCount := int32(0)
	for _, node := range result.Nodes.Node {
		// Get IP address from node attributes - skip node if IP is not available
		ip := nodeIPMap[node.Name]
		if ip == "" {
			klog.V(2).Infof("Skipping node %s: IP address not available in node attributes", node.Name)
			continue
		}

		// Convert online status to typed constant
		onlineStatus := v1alpha1.NodeOnlineStatusOffline
		if node.Online == booleanValueTrue {
			onlineStatus = v1alpha1.NodeOnlineStatusOnline
			onlineCount++
		}

		// Convert standby to mode typed constant
		mode := v1alpha1.NodeModeActive
		if node.Standby == booleanValueTrue {
			mode = v1alpha1.NodeModeStandby
		}

		nodes = append(nodes, v1alpha1.NodeStatus{
			Name:         node.Name,
			IP:           ip,
			OnlineStatus: onlineStatus,
			Mode:         mode,
		})
	}
	totalNodes := int32(len(result.Nodes.Node))
	summary.NodesOnline = &onlineCount
	summary.NodesTotal = &totalNodes

	// Build resource information
	resourcesStarted := int32(0)

	// Helper function to process a resource
	processResource := func(resource Resource) {
		// Convert active status to typed constant
		activeStatus := v1alpha1.ResourceActiveStatusInactive
		if resource.Active == booleanValueTrue {
			activeStatus = v1alpha1.ResourceActiveStatusActive
		}

		started := resource.Role == nodeStatusStarted && activeStatus == v1alpha1.ResourceActiveStatusActive
		if started {
			resourcesStarted++
		}

		resources = append(resources, v1alpha1.ResourceStatus{
			Name:          resource.ID,
			ResourceAgent: resource.ResourceAgent,
			Role:          resource.Role,
			ActiveStatus:  activeStatus,
			Node:          resource.Node.Name,
		})
	}

	// Process clone resources
	for _, clone := range result.Resources.Clone {
		for _, resource := range clone.Resource {
			processResource(resource)
		}
	}

	// Process standalone resources
	for _, resource := range result.Resources.Resource {
		processResource(resource)
	}

	totalResources := int32(len(resources))
	summary.ResourcesStarted = &resourcesStarted
	summary.ResourcesTotal = &totalResources

	// Build node history (recent operations)
	cutoffTime := time.Now().Add(-failedActionTimeWindow)

	for _, node := range result.NodeHistory.Node {
		for _, resourceHistory := range node.ResourceHistory {
			for _, operation := range resourceHistory.OperationHistory {
				// Parse the timestamp
				t, err := time.Parse(pacemakerTimeFormat, operation.LastRCChange)
				if err != nil {
					klog.Warningf("Failed to parse operation timestamp: %v", err)
					continue
				}

				// Only include recent operations
				if !t.After(cutoffTime) {
					continue
				}

				// Parse RC
				rc := int32(0)
				if operation.RC != "" {
					if _, err := fmt.Sscanf(operation.RC, "%d", &rc); err != nil {
						klog.Warningf("Failed to parse RC value '%s': %v", operation.RC, err)
					}
				}

				nodeHistory = append(nodeHistory, v1alpha1.NodeHistoryEntry{
					Node:         node.Name,
					Resource:     resourceHistory.ID,
					Operation:    operation.Task,
					RC:           &rc,
					RCText:       operation.RCText,
					LastRCChange: metav1.NewTime(t),
				})
			}
		}
	}

	// Build fencing history
	fenceCutoffTime := time.Now().Add(-fencingEventTimeWindow)

	for _, fenceEvent := range result.FenceHistory.FenceEvent {
		// Parse the timestamp
		t, err := time.Parse(pacemakerFenceTimeFormat, fenceEvent.Completed)
		if err != nil {
			klog.Warningf("Failed to parse fence event timestamp: %v", err)
			continue
		}

		// Only include recent fencing events
		if !t.After(fenceCutoffTime) {
			continue
		}

		fencingHistory = append(fencingHistory, v1alpha1.FencingEvent{
			Target:    fenceEvent.Target,
			Action:    fenceEvent.Action,
			Status:    fenceEvent.Status,
			Completed: metav1.NewTime(t),
		})
	}

	return summary, nodes, resources, nodeHistory, fencingHistory
}

// updatePacemakerStatusCR updates or creates the PacemakerStatus custom resource
// with the collected status information. The CR is named "cluster" and is cluster-scoped.
// If the CR doesn't exist, it will be created; otherwise, its status subresource is updated.
//
// All data collected from Pacemaker is written to the status as-is. The health check controller
// is responsible for handling potentially missing or empty fields.
func updatePacemakerStatusCR(
	ctx context.Context,
	rawXML string,
	summary *v1alpha1.PacemakerSummary,
	nodes []v1alpha1.NodeStatus,
	resources []v1alpha1.ResourceStatus,
	nodeHistory []v1alpha1.NodeHistoryEntry,
	fencingHistory []v1alpha1.FencingEvent,
	collectionError string,
) error {
	// Create REST client for the PacemakerStatus CRD
	config, err := getKubeConfig()
	if err != nil {
		return err
	}

	restClient, err := createPacemakerRESTClient(config)
	if err != nil {
		return err
	}

	// Try to get existing PacemakerStatus
	var existing v1alpha1.PacemakerStatus
	err = restClient.Get().
		Resource(pacemakerResourceName).
		Name(PacemakerStatusResourceName).
		Do(ctx).
		Into(&existing)

	now := metav1.Now()

	if err != nil {
		// Create new PacemakerStatus if it doesn't exist
		if apierrors.IsNotFound(err) {
			newStatus := &v1alpha1.PacemakerStatus{
				TypeMeta: metav1.TypeMeta{
					APIVersion: v1alpha1.SchemeGroupVersion.String(),
					Kind:       pacemakerKind,
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: PacemakerStatusResourceName,
				},
				Status: &v1alpha1.PacemakerStatusStatus{
					LastUpdated:     now,
					RawXML:          rawXML,
					CollectionError: collectionError,
					Summary:         summary,
					Nodes:           nodes,
					Resources:       resources,
					NodeHistory:     nodeHistory,
					FencingHistory:  fencingHistory,
				},
			}

			result := restClient.Post().
				Resource(pacemakerResourceName).
				Body(newStatus).
				Do(ctx)

			if result.Error() != nil {
				return fmt.Errorf("failed to create PacemakerStatus: %w", result.Error())
			}

			// Ensure status is set on initial create when CRD uses the status subresource
			result = restClient.Put().
				Resource(pacemakerResourceName).
				Name(PacemakerStatusResourceName).
				SubResource(statusSubresource).
				Body(newStatus).
				Do(ctx)
			if result.Error() != nil {
				return fmt.Errorf("failed to initialize PacemakerStatus status: %w", result.Error())
			}
			klog.Info("Created and initialized PacemakerStatus CR")

			return nil
		}
		return fmt.Errorf("failed to get PacemakerStatus: %w", err)
	}

	// Initialize Status field if it's nil to avoid nil pointer dereference
	if existing.Status == nil {
		existing.Status = &v1alpha1.PacemakerStatusStatus{}
	}

	// Update existing PacemakerStatus
	existing.Status.LastUpdated = now
	existing.Status.RawXML = rawXML
	existing.Status.CollectionError = collectionError
	existing.Status.Summary = summary
	existing.Status.Nodes = nodes
	existing.Status.Resources = resources
	existing.Status.NodeHistory = nodeHistory
	existing.Status.FencingHistory = fencingHistory

	result := restClient.Put().
		Resource(pacemakerResourceName).
		Name(PacemakerStatusResourceName).
		SubResource(statusSubresource).
		Body(&existing).
		Do(ctx)

	if result.Error() != nil {
		return fmt.Errorf("failed to update PacemakerStatus: %w", result.Error())
	}

	klog.Info("Updated existing PacemakerStatus CR")
	return nil
}
