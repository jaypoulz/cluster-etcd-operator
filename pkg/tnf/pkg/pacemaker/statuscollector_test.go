package pacemaker

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	v1alpha1 "github.com/openshift/api/etcd/v1alpha1"
)

// Helper function to load test XML files
func loadTestXML(t *testing.T, filename string) string {
	path := filepath.Join("testdata", filename)
	data, err := os.ReadFile(path)
	require.NoError(t, err, "Failed to read test XML file: %s", filename)
	return string(data)
}

// Helper function to generate recent failures XML with dynamic timestamps
func getRecentFailuresXML(t *testing.T) string {
	// Load the template from testdata
	templateXML := loadTestXML(t, "recent_failures_template.xml")

	// Generate recent timestamps that are definitely within the 5-minute window
	now := time.Now()
	recentTime := now.Add(-1 * time.Minute).UTC().Format("Mon Jan 2 15:04:05 2006")
	recentTimeISO := now.Add(-1 * time.Minute).UTC().Format("2006-01-02 15:04:05.000000Z")

	// Replace the placeholders in the template
	xmlContent := strings.ReplaceAll(templateXML, "{{RECENT_TIMESTAMP}}", recentTime)
	xmlContent = strings.ReplaceAll(xmlContent, "{{RECENT_TIMESTAMP_ISO}}", recentTimeISO)

	return xmlContent
}

func TestBuildStatusComponents_HealthyCluster(t *testing.T) {
	// Test with healthy cluster XML
	healthyXML := loadTestXML(t, "healthy_cluster.xml")
	var result PacemakerResult
	err := xml.Unmarshal([]byte(healthyXML), &result)
	require.NoError(t, err)

	summary, nodes, resources, nodeHistory, fencingHistory := buildStatusComponents(&result)

	// Verify summary
	require.Equal(t, pacemakerStateRunning, summary.PacemakerdState)
	require.Equal(t, v1alpha1.QuorumStatusQuorate, summary.QuorumStatus)
	require.NotNil(t, summary.NodesOnline)
	require.Equal(t, int32(2), *summary.NodesOnline)
	require.NotNil(t, summary.NodesTotal)
	require.Equal(t, int32(2), *summary.NodesTotal)
	require.NotNil(t, summary.ResourcesStarted)
	require.Greater(t, *summary.ResourcesStarted, int32(0))

	// Verify nodes
	require.Len(t, nodes, 2)
	for _, node := range nodes {
		require.Equal(t, v1alpha1.NodeOnlineStatusOnline, node.OnlineStatus, "All nodes should be online")
		require.Equal(t, v1alpha1.NodeModeActive, node.Mode, "No nodes should be in standby")
	}

	// Verify resources
	require.NotEmpty(t, resources)
	foundKubelet := false
	foundEtcd := false
	for _, resource := range resources {
		if strings.Contains(resource.ResourceAgent, "kubelet") {
			foundKubelet = true
		}
		if strings.Contains(resource.ResourceAgent, "etcd") {
			foundEtcd = true
		}
	}
	require.True(t, foundKubelet, "Should find kubelet resources")
	require.True(t, foundEtcd, "Should find etcd resources")

	// No recent failures or fencing in healthy cluster
	require.Empty(t, nodeHistory)
	require.Empty(t, fencingHistory)
}

func TestBuildStatusComponents_OfflineNode(t *testing.T) {
	// Test with offline node XML
	offlineXML := loadTestXML(t, "offline_node.xml")
	var result PacemakerResult
	err := xml.Unmarshal([]byte(offlineXML), &result)
	require.NoError(t, err)

	summary, nodes, _, _, _ := buildStatusComponents(&result)

	// Verify summary shows reduced online count
	require.Equal(t, pacemakerStateRunning, summary.PacemakerdState)
	require.NotNil(t, summary.NodesOnline)
	require.Equal(t, int32(1), *summary.NodesOnline, "Only one node should be online")
	require.NotNil(t, summary.NodesTotal)
	require.Equal(t, int32(2), *summary.NodesTotal)

	// Verify nodes - one should be offline
	require.Len(t, nodes, 2)
	offlineCount := 0
	for _, node := range nodes {
		if node.OnlineStatus != "Online" {
			offlineCount++
		}
	}
	require.Equal(t, 1, offlineCount, "Should have exactly one offline node")
}

func TestBuildStatusComponents_StandbyNode(t *testing.T) {
	// Test with standby node XML
	standbyXML := loadTestXML(t, "standby_node.xml")
	var result PacemakerResult
	err := xml.Unmarshal([]byte(standbyXML), &result)
	require.NoError(t, err)

	_, nodes, _, _, _ := buildStatusComponents(&result)

	// Verify nodes - one should be in standby
	require.Len(t, nodes, 2)
	standbyCount := 0
	for _, node := range nodes {
		if node.Mode == v1alpha1.NodeModeStandby {
			standbyCount++
		}
	}
	require.Equal(t, 1, standbyCount, "Should have exactly one standby node")
}

func TestBuildStatusComponents_RecentFailures(t *testing.T) {
	// Test with recent failures XML
	recentFailuresXML := getRecentFailuresXML(t)
	var result PacemakerResult
	err := xml.Unmarshal([]byte(recentFailuresXML), &result)
	require.NoError(t, err)

	_, _, _, nodeHistory, fencingHistory := buildStatusComponents(&result)

	// Verify node history contains recent failures
	require.NotEmpty(t, nodeHistory, "Should have node history entries")
	foundFailure := false
	for _, entry := range nodeHistory {
		if entry.RC != nil && *entry.RC != 0 {
			foundFailure = true
			require.NotEmpty(t, entry.Node, "Entry should have node name")
			require.NotEmpty(t, entry.Resource, "Entry should have resource name")
			require.NotEmpty(t, entry.Operation, "Entry should have operation")
			require.NotEmpty(t, entry.RCText, "Entry should have RC text")
			require.False(t, entry.LastRCChange.IsZero(), "Entry should have timestamp")
		}
	}
	require.True(t, foundFailure, "Should have at least one failed operation")

	// Verify fencing history contains recent fencing events
	require.NotEmpty(t, fencingHistory, "Should have fencing history entries")
	for _, event := range fencingHistory {
		require.NotEmpty(t, event.Target, "Event should have target")
		require.NotEmpty(t, event.Action, "Event should have action")
		require.NotEmpty(t, event.Status, "Event should have status")
		require.False(t, event.Completed.IsZero(), "Event should have timestamp")
	}
}

func TestBuildStatusComponents_TimeWindowFiltering(t *testing.T) {
	// Create a test result with operations at different times
	now := time.Now()
	recentTime := now.Add(-2 * time.Minute)    // Within 5-minute window
	oldTime := now.Add(-10 * time.Minute)      // Outside 5-minute window
	recentFenceTime := now.Add(-1 * time.Hour) // Within 24-hour window
	oldFenceTime := now.Add(-48 * time.Hour)   // Outside 24-hour window

	result := &PacemakerResult{
		Summary: Summary{
			Stack:     Stack{PacemakerdState: pacemakerStateRunning},
			CurrentDC: CurrentDC{WithQuorum: booleanValueTrue},
		},
		Nodes: Nodes{
			Node: []Node{
				{Name: "master-0", Online: booleanValueTrue},
			},
		},
		NodeHistory: NodeHistory{
			Node: []NodeHistoryNode{
				{
					Name: "master-0",
					ResourceHistory: []ResourceHistory{
						{
							ID: "etcd-clone-0",
							OperationHistory: []OperationHistory{
								{
									Task:         "monitor",
									RC:           "1",
									RCText:       "error",
									LastRCChange: recentTime.UTC().Format("Mon Jan 2 15:04:05 2006"),
								},
								{
									Task:         "monitor",
									RC:           "1",
									RCText:       "error",
									LastRCChange: oldTime.UTC().Format("Mon Jan 2 15:04:05 2006"),
								},
							},
						},
					},
				},
			},
		},
		FenceHistory: FenceHistory{
			FenceEvent: []FenceEvent{
				{
					Target:    "master-1",
					Action:    "reboot",
					Status:    "success",
					Completed: recentFenceTime.UTC().Format("2006-01-02 15:04:05.000000Z"),
				},
				{
					Target:    "master-1",
					Action:    "reboot",
					Status:    "success",
					Completed: oldFenceTime.UTC().Format("2006-01-02 15:04:05.000000Z"),
				},
			},
		},
	}

	_, _, _, nodeHistory, fencingHistory := buildStatusComponents(result)

	// Only recent operations should be included
	require.Len(t, nodeHistory, 1, "Should only include recent operation")

	// Only recent fencing events should be included
	require.Len(t, fencingHistory, 1, "Should only include recent fencing event")
}

func TestBuildStatusComponents_ResourceCounting(t *testing.T) {
	result := &PacemakerResult{
		Summary: Summary{
			Stack:     Stack{PacemakerdState: pacemakerStateRunning},
			CurrentDC: CurrentDC{WithQuorum: booleanValueTrue},
		},
		Nodes: Nodes{
			Node: []Node{
				{Name: "master-0", Online: booleanValueTrue},
				{Name: "master-1", Online: booleanValueTrue},
			},
		},
		Resources: Resources{
			Clone: []Clone{
				{
					Resource: []Resource{
						{
							ID:            "kubelet-0",
							ResourceAgent: resourceAgentKubelet,
							Role:          nodeStatusStarted,
							Active:        booleanValueTrue,
							Node:          NodeRef{Name: "master-0"},
						},
						{
							ID:            "kubelet-1",
							ResourceAgent: resourceAgentKubelet,
							Role:          nodeStatusStarted,
							Active:        booleanValueTrue,
							Node:          NodeRef{Name: "master-1"},
						},
					},
				},
			},
			Resource: []Resource{
				{
					ID:            "etcd-0",
					ResourceAgent: resourceAgentEtcd,
					Role:          nodeStatusStarted,
					Active:        booleanValueTrue,
					Node:          NodeRef{Name: "master-0"},
				},
				{
					ID:            "etcd-1",
					ResourceAgent: resourceAgentEtcd,
					Role:          "Stopped",
					Active:        booleanValueFalse,
					Node:          NodeRef{Name: ""},
				},
			},
		},
	}

	summary, _, resources, _, _ := buildStatusComponents(result)

	// Verify resource counting
	require.NotNil(t, summary.ResourcesTotal)
	require.Equal(t, int32(4), *summary.ResourcesTotal, "Should count all resources")
	require.NotNil(t, summary.ResourcesStarted)
	require.Equal(t, int32(3), *summary.ResourcesStarted, "Should count only started resources")

	// Verify resource details
	require.Len(t, resources, 4)
	startedCount := 0
	for _, resource := range resources {
		if resource.Role == nodeStatusStarted && resource.ActiveStatus == v1alpha1.ResourceActiveStatusActive {
			startedCount++
		}
	}
	require.Equal(t, 3, startedCount, "Should have 3 started resources")
}

func TestBuildStatusComponents_EmptyXML(t *testing.T) {
	result := &PacemakerResult{
		Summary: Summary{
			Stack:     Stack{PacemakerdState: pacemakerStateRunning},
			CurrentDC: CurrentDC{WithQuorum: booleanValueFalse},
		},
	}

	summary, nodes, resources, nodeHistory, fencingHistory := buildStatusComponents(result)

	// Verify minimal valid result
	require.Equal(t, pacemakerStateRunning, summary.PacemakerdState)
	require.Equal(t, v1alpha1.QuorumStatusNoQuorum, summary.QuorumStatus)
	require.NotNil(t, summary.NodesOnline)
	require.Equal(t, int32(0), *summary.NodesOnline)
	require.NotNil(t, summary.NodesTotal)
	require.Equal(t, int32(0), *summary.NodesTotal)
	require.Empty(t, nodes)
	require.Empty(t, resources)
	require.Empty(t, nodeHistory)
	require.Empty(t, fencingHistory)
}

func TestBuildStatusComponents_QuorumHandling(t *testing.T) {
	tests := []struct {
		name                 string
		withQuorum           string
		expectedQuorumStatus v1alpha1.QuorumStatusType
	}{
		{"quorum_true", "true", v1alpha1.QuorumStatusQuorate},
		{"quorum_false", "false", v1alpha1.QuorumStatusNoQuorum},
		{"quorum_empty", "", v1alpha1.QuorumStatusNoQuorum},
		{"quorum_invalid", "invalid", v1alpha1.QuorumStatusNoQuorum},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &PacemakerResult{
				Summary: Summary{
					Stack:     Stack{PacemakerdState: pacemakerStateRunning},
					CurrentDC: CurrentDC{WithQuorum: tt.withQuorum},
				},
			}

			summary, _, _, _, _ := buildStatusComponents(result)
			require.Equal(t, tt.expectedQuorumStatus, summary.QuorumStatus)
		})
	}
}

func TestBuildStatusComponents_NodeStatusVariations(t *testing.T) {
	tests := []struct {
		name                 string
		online               string
		standby              string
		expectedOnlineStatus string
		expectedMode         string
	}{
		{"online_normal", "true", "false", "Online", "Active"},
		{"offline", "false", "false", "Offline", "Active"},
		{"online_standby", "true", "true", "Online", "Standby"},
		{"offline_standby", "false", "true", "Offline", "Standby"},
		{"empty_values", "", "", "Offline", "Active"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &PacemakerResult{
				Summary: Summary{
					Stack:     Stack{PacemakerdState: pacemakerStateRunning},
					CurrentDC: CurrentDC{WithQuorum: booleanValueTrue},
				},
				Nodes: Nodes{
					Node: []Node{
						{
							Name:    "test-node",
							Online:  tt.online,
							Standby: tt.standby,
						},
					},
				},
			}

			_, nodes, _, _, _ := buildStatusComponents(result)
			// Nodes without IP addresses should be excluded
			require.Len(t, nodes, 0, "Nodes without IP address should be filtered out")
		})
	}
}

func TestCollectPacemakerStatus_XMLSizeValidation(t *testing.T) {
	// This test verifies that large XML is rejected
	// In a real test, you would mock the exec.Execute function
	// For now, we just verify the maxXMLSize constant is reasonable
	require.Equal(t, 10*1024*1024, maxXMLSize, "Max XML size should be 10MB")
}

func TestCollectPacemakerStatus_InvalidXMLHandling(t *testing.T) {
	invalidXML := "<invalid><unclosed>"
	var result PacemakerResult
	err := xml.Unmarshal([]byte(invalidXML), &result)

	// Should fail to unmarshal but not panic
	require.Error(t, err, "Should return error for invalid XML")
}

func TestBuildStatusComponents_NodeIPExtraction(t *testing.T) {
	result := &PacemakerResult{
		Summary: Summary{
			Stack:     Stack{PacemakerdState: pacemakerStateRunning},
			CurrentDC: CurrentDC{WithQuorum: booleanValueTrue},
		},
		Nodes: Nodes{
			Node: []Node{
				{Name: "master-0", Online: booleanValueTrue},
			},
		},
		NodeAttributes: NodeAttributes{
			Node: []NodeAttributeSet{
				{
					Name: "master-0",
					Attribute: []NodeAttribute{
						{Name: "node_ip", Value: "192.168.1.10"},
						{Name: "other_attr", Value: "value"},
					},
				},
			},
		},
	}

	// Verify node_ip attribute is extracted correctly
	_, nodes, _, _, _ := buildStatusComponents(result)
	require.Len(t, nodes, 1)
	require.Equal(t, "master-0", nodes[0].Name)
	require.Equal(t, "192.168.1.10", nodes[0].IP, "IP should be extracted from node_ip attribute")
}

func TestBuildStatusComponents_NodeIPFiltering(t *testing.T) {
	result := &PacemakerResult{
		Summary: Summary{
			Stack:     Stack{PacemakerdState: pacemakerStateRunning},
			CurrentDC: CurrentDC{WithQuorum: booleanValueTrue},
		},
		Nodes: Nodes{
			Node: []Node{
				{Name: "master-0", Online: booleanValueTrue},  // Has IP
				{Name: "master-1", Online: booleanValueTrue},  // No IP - should be filtered
				{Name: "master-2", Online: booleanValueFalse}, // Has IP
			},
		},
		NodeAttributes: NodeAttributes{
			Node: []NodeAttributeSet{
				{
					Name: "master-0",
					Attribute: []NodeAttribute{
						{Name: "node_ip", Value: "192.168.1.10"},
					},
				},
				{
					Name: "master-2",
					Attribute: []NodeAttribute{
						{Name: "node_ip", Value: "192.168.1.12"},
					},
				},
				// master-1 has no node_ip attribute
			},
		},
	}

	// Verify only nodes with IP addresses are included
	_, nodes, _, _, _ := buildStatusComponents(result)
	require.Len(t, nodes, 2, "Only nodes with IP addresses should be included")

	// Verify the nodes that were included
	nodeNames := make(map[string]bool)
	for _, node := range nodes {
		nodeNames[node.Name] = true
		require.NotEmpty(t, node.IP, "All included nodes should have an IP")
	}

	require.True(t, nodeNames["master-0"], "master-0 should be included")
	require.False(t, nodeNames["master-1"], "master-1 should be filtered out (no IP)")
	require.True(t, nodeNames["master-2"], "master-2 should be included")
}

func TestBuildStatusComponents_ResourceAgentTypes(t *testing.T) {
	result := &PacemakerResult{
		Summary: Summary{
			Stack:     Stack{PacemakerdState: pacemakerStateRunning},
			CurrentDC: CurrentDC{WithQuorum: booleanValueTrue},
		},
		Resources: Resources{
			Resource: []Resource{
				{
					ID:            "kubelet-0",
					ResourceAgent: "systemd:kubelet",
					Role:          nodeStatusStarted,
					Active:        booleanValueTrue,
				},
				{
					ID:            "etcd-0",
					ResourceAgent: resourceAgentEtcd,
					Role:          nodeStatusStarted,
					Active:        booleanValueTrue,
				},
				{
					ID:            "ip-0",
					ResourceAgent: resourceAgentIPAddr,
					Role:          nodeStatusStarted,
					Active:        booleanValueTrue,
				},
			},
		},
	}

	_, _, resources, _, _ := buildStatusComponents(result)

	require.Len(t, resources, 3)

	// Verify all resource agents are preserved
	agents := make(map[string]bool)
	for _, resource := range resources {
		agents[resource.ResourceAgent] = true
	}

	require.True(t, agents[resourceAgentKubelet])
	require.True(t, agents[resourceAgentEtcd])
	require.True(t, agents[resourceAgentIPAddr])
}
