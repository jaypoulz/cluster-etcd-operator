package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// QuorumStatusType represents the quorum status of a Pacemaker cluster
// +kubebuilder:validation:Enum=Quorate;NoQuorum
type QuorumStatusType string

const (
	// QuorumStatusQuorate indicates the cluster has quorum
	QuorumStatusQuorate QuorumStatusType = "Quorate"

	// QuorumStatusNoQuorum indicates the cluster does not have quorum
	QuorumStatusNoQuorum QuorumStatusType = "NoQuorum"
)

// NodeOnlineStatusType represents whether a node is online or offline
// +kubebuilder:validation:Enum=Online;Offline
type NodeOnlineStatusType string

const (
	// NodeOnlineStatusOnline indicates the node is online
	NodeOnlineStatusOnline NodeOnlineStatusType = "Online"

	// NodeOnlineStatusOffline indicates the node is offline
	NodeOnlineStatusOffline NodeOnlineStatusType = "Offline"
)

// NodeModeType represents whether a node is in active or standby mode
// +kubebuilder:validation:Enum=Active;Standby
type NodeModeType string

const (
	// NodeModeActive indicates the node is in active mode
	NodeModeActive NodeModeType = "Active"

	// NodeModeStandby indicates the node is in standby mode
	NodeModeStandby NodeModeType = "Standby"
)

// ResourceActiveStatusType represents whether a resource is active or inactive
// +kubebuilder:validation:Enum=Active;Inactive
type ResourceActiveStatusType string

const (
	// ResourceActiveStatusActive indicates the resource is active
	ResourceActiveStatusActive ResourceActiveStatusType = "Active"

	// ResourceActiveStatusInactive indicates the resource is inactive
	ResourceActiveStatusInactive ResourceActiveStatusType = "Inactive"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
//
// # PacemakerStatus represents the current state of the Pacemaker cluster as reported by the pcs status command
//
// This resource provides a view into the health and status of a Pacemaker-managed cluster in dual-replica (two-node)
// deployments. The status is periodically collected by a privileged controller and made available for monitoring
// and health checking purposes.
//
// Compatibility level 4: No compatibility is provided, the API can change at any point for any reason. These capabilities should not be used by applications needing long term support.
// +openshift:compatibility-gen:level=4
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=pacemakerstatuses,scope=Cluster
// +kubebuilder:subresource:status
// +openshift:file-pattern=cvoRunLevel=0000_25,operatorName=etcd,operatorOrdering=01,operatorComponent=two-node-fencing
// +openshift:api-approved.openshift.io=https://github.com/openshift/api/pull/2544
type PacemakerStatus struct {
	metav1.TypeMeta   `json:",inline"`

	// metadata is the standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// spec is an empty spec to satisfy Kubernetes API conventions.
	// PacemakerStatus is a status-only resource and does not use spec for configuration.
	// +optional
	Spec *PacemakerStatusSpec `json:"spec,omitempty"`

	// status contains the actual pacemaker cluster status information collected from the cluster.
	// +optional
	Status *PacemakerStatusStatus `json:"status,omitempty"`
}

// PacemakerStatusSpec is an empty spec as PacemakerStatus is a status-only resource
type PacemakerStatusSpec struct {
}

// PacemakerStatusStatus contains the actual pacemaker cluster status information
type PacemakerStatusStatus struct {
	// lastUpdated is the timestamp when this status was last updated
	// +optional
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`

	// rawXML contains the raw XML output from pcs status xml command.
	// Kept for debugging purposes only; healthcheck should not need to parse this.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=262144
	// +optional
	RawXML string `json:"rawXML,omitempty"`

	// collectionError contains any error encountered while collecting status
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	CollectionError string `json:"collectionError,omitempty"`

	// summary provides high-level counts and flags for the cluster state
	// +optional
	Summary *PacemakerSummary `json:"summary,omitempty"`

	// nodes provides detailed information about each node in the cluster
	// +listType=atomic
	// +kubebuilder:validation:MinItems=2
	// +kubebuilder:validation:MaxItems=2
	// +optional
	Nodes []NodeStatus `json:"nodes,omitempty"`

	// resources provides detailed information about each resource in the cluster
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +optional
	Resources []ResourceStatus `json:"resources,omitempty"`

	// nodeHistory provides recent operation history for troubleshooting
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +optional
	NodeHistory []NodeHistoryEntry `json:"nodeHistory,omitempty"`

	// fencingHistory provides recent fencing events
	// +listType=atomic
 	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +optional
	FencingHistory []FencingEvent `json:"fencingHistory,omitempty"`
}

// PacemakerSummary provides a high-level summary of cluster state
type PacemakerSummary struct {
	// pacemakerdState indicates if pacemaker is running
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	// +optional
	PacemakerdState string `json:"pacemakerdState,omitempty"`

	// quorumStatus indicates if the cluster has quorum
	// +optional
	QuorumStatus QuorumStatusType `json:"quorumStatus,omitempty"`

	// nodesOnline is the count of online nodes
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2
	// +optional
	NodesOnline *int32 `json:"nodesOnline,omitempty"`

	// nodesTotal is the total count of configured nodes
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2
	// +optional
	NodesTotal *int32 `json:"nodesTotal,omitempty"`

	// resourcesStarted is the count of started resources
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=16
	// +optional
	ResourcesStarted *int32 `json:"resourcesStarted,omitempty"`

	// resourcesTotal is the total count of configured resources
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=16
	// +optional
	ResourcesTotal *int32 `json:"resourcesTotal,omitempty"`
}

// NodeStatus represents the status of a single node in the Pacemaker cluster
type NodeStatus struct {
	// name is the name of the node
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Name string `json:"name,omitempty"`

	// ip is the IP address of the node
	// +kubebuilder:validation:MinLength=2
	// +kubebuilder:validation:MaxLength=62
	// +optional
	IP string `json:"ip,omitempty"`

	// onlineStatus indicates if the node is online or offline
	// +optional
	OnlineStatus NodeOnlineStatusType `json:"onlineStatus,omitempty"`

	// mode indicates if the node is in active or standby mode
	// +optional
	Mode NodeModeType `json:"mode,omitempty"`
}

// ResourceStatus represents the status of a single resource in the Pacemaker cluster
type ResourceStatus struct {
	// name is the name of the resource
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Name string `json:"name,omitempty"`

	// resourceAgent is the resource agent type (e.g., "ocf:heartbeat:IPaddr2", "systemd:kubelet")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	ResourceAgent string `json:"resourceAgent,omitempty"`

	// role is the current role of the resource (e.g., "Started", "Stopped")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Role string `json:"role,omitempty"`

	// activeStatus indicates if the resource is active or inactive
	// +optional
	ActiveStatus ResourceActiveStatusType `json:"activeStatus,omitempty"`

	// node is the node where the resource is running
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Node string `json:"node,omitempty"`
}

// NodeHistoryEntry represents a single operation history entry from node_history
type NodeHistoryEntry struct {
	// node is the node where the operation occurred
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Node string `json:"node,omitempty"`

	// resource is the resource that was operated on
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Resource string `json:"resource,omitempty"`

	// operation is the operation that was performed (e.g., "monitor", "start", "stop")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Operation string `json:"operation,omitempty"`

	// rc is the return code from the operation
	// +optional
	RC *int32 `json:"rc,omitempty"`

	// rcText is the human-readable return code text (e.g., "ok", "error", "not running")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	// +optional
	RCText string `json:"rcText,omitempty"`

	// lastRCChange is the timestamp when the RC last changed
	// +optional
	LastRCChange metav1.Time `json:"lastRCChange,omitempty"`
}

// FencingEvent represents a single fencing event from fence history
type FencingEvent struct {
	// target is the node that was fenced
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +optional
	Target string `json:"target,omitempty"`

	// action is the fencing action performed (e.g., "reboot", "off", "on")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Action string `json:"action,omitempty"`

	// status is the status of the fencing operation (e.g., "success", "failed")
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=16
	// +optional
	Status string `json:"status,omitempty"`

	// completed is the timestamp when the fencing event was completed
	// +optional
	Completed metav1.Time `json:"completed,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +openshift:compatibility-gen:level=4

// PacemakerStatusList contains a list of PacemakerStatus
//
// Compatibility level 4: No compatibility is provided, the API can change at any point for any reason. These capabilities should not be used by applications needing long term support.
type PacemakerStatusList struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is the standard list's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ListMeta `json:"metadata,omitempty"`

	// items is a list of PacemakerStatus objects.
	Items []PacemakerStatus `json:"items"`
}
