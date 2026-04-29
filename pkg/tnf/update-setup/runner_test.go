package updatesetup

import (
	"testing"

	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/pacemaker"
	"github.com/stretchr/testify/require"
)

// TestPacemakerReconciliation tests the pacemaker reconciliation logic
// that determines whether to add/remove nodes based on k8s vs pacemaker membership
func TestPacemakerReconciliation(t *testing.T) {
	tests := []struct {
		name                  string
		k8sNodes              []string
		pacemakerOnlineNodes  []string
		pacemakerOfflineNodes []string
		expectRemove          []string // Nodes expected to be removed, empty if none
		expectAdd             []string // Nodes expected to be added, empty if none
		expectSkip            bool     // True if no action should be taken
	}{
		{
			name:                  "Upgrade reboot - offline node exists in k8s - skip removal",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"master-1"},
			expectRemove:          nil,
			expectAdd:             nil,
			expectSkip:            true,
		},
		{
			name:                  "Node deleted - offline node not in k8s - remove only",
			k8sNodes:              []string{"master-0"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"master-1"},
			expectRemove:          []string{"master-1"},
			expectAdd:             nil,
			expectSkip:            false,
		},
		{
			name:                  "Node added - k8s node not in pacemaker - add only",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{},
			expectRemove:          nil,
			expectAdd:             []string{"master-1"},
			expectSkip:            false,
		},
		{
			name:                  "Upgrade reboot - offline node still in k8s - skip (duplicate of first test)",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"master-1"},
			expectRemove:          nil,
			expectAdd:             nil,
			expectSkip:            true,
		},
		{
			name:                  "Replacement after deletion - old node offline, not in k8s - remove only",
			k8sNodes:              []string{"master-0"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"old-master-1"},
			expectRemove:          []string{"old-master-1"},
			expectAdd:             nil,
			expectSkip:            false,
		},
		{
			name:                  "Replacement after addition - new node in k8s, not in pacemaker - add only",
			k8sNodes:              []string{"master-0", "new-master-1"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{},
			expectRemove:          nil,
			expectAdd:             []string{"new-master-1"},
			expectSkip:            false,
		},
		{
			name:                  "No changes needed - all nodes match",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{"master-0", "master-1"},
			pacemakerOfflineNodes: []string{},
			expectRemove:          nil,
			expectAdd:             nil,
			expectSkip:            true,
		},
		{
			name:                  "No offline nodes - all nodes match",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{"master-0", "master-1"},
			pacemakerOfflineNodes: []string{},
			expectRemove:          nil,
			expectAdd:             nil,
			expectSkip:            true,
		},
		{
			name:                  "Stale offline entries - multiple offline nodes, one not in k8s - remove stale",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"master-1", "old-master-2"},
			expectRemove:          []string{"old-master-2"},
			expectAdd:             nil,
			expectSkip:            false,
		},
		{
			name:                  "Online node deleted from k8s - pacemaker status lag - remove",
			k8sNodes:              []string{"master-0"},
			pacemakerOnlineNodes:  []string{"master-0", "master-1"},
			pacemakerOfflineNodes: []string{},
			expectRemove:          []string{"master-1"},
			expectAdd:             nil,
			expectSkip:            false,
		},
		{
			name:                  "Multiple stale nodes - both offline, both not in k8s - remove both",
			k8sNodes:              []string{"master-0"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"old-master-1", "old-master-2"},
			expectRemove:          []string{"old-master-1", "old-master-2"},
			expectAdd:             nil,
			expectSkip:            false,
		},
		{
			name:                  "Batch: Remove stale and add missing node",
			k8sNodes:              []string{"master-0", "new-master-1"},
			pacemakerOnlineNodes:  []string{"master-0"},
			pacemakerOfflineNodes: []string{"old-master-1"},
			expectRemove:          []string{"old-master-1"},
			expectAdd:             []string{"new-master-1"},
			expectSkip:            false,
		},
		{
			name:                  "Both nodes offline but both in k8s - theoretical dual power loss - skip",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{},
			pacemakerOfflineNodes: []string{"master-0", "master-1"},
			expectRemove:          nil,
			expectAdd:             nil,
			expectSkip:            true,
		},
		{
			name:                  "Empty pacemaker - defensive edge case - add both k8s nodes",
			k8sNodes:              []string{"master-0", "master-1"},
			pacemakerOnlineNodes:  []string{},
			pacemakerOfflineNodes: []string{},
			expectRemove:          nil,
			expectAdd:             []string{"master-0", "master-1"}, // Both nodes added (order non-deterministic)
			expectSkip:            false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build k8s nodes map
			k8sNodesMap := make(map[string]struct{})
			for _, node := range tt.k8sNodes {
				k8sNodesMap[node] = struct{}{}
			}

			// Build pacemaker nodes list (online + offline)
			var pacemakerNodesList []pacemaker.Node
			for _, node := range tt.pacemakerOnlineNodes {
				pacemakerNodesList = append(pacemakerNodesList, pacemaker.Node{Name: node, Online: "true"})
			}
			for _, node := range tt.pacemakerOfflineNodes {
				pacemakerNodesList = append(pacemakerNodesList, pacemaker.Node{Name: node, Online: "false"})
			}

			// Call the actual reconciliation logic from runner.go
			nodesToRemove, nodesToAdd := reconcileNodes(k8sNodesMap, pacemakerNodesList)

			// Verify expectations
			if tt.expectSkip {
				require.Empty(t, nodesToRemove, "Expected no node removal")
				require.Empty(t, nodesToAdd, "Expected no node addition")
			} else {
				// For slices with non-deterministic order (map iteration), use ElementsMatch
				if tt.name == "Empty pacemaker - defensive edge case - add both k8s nodes" {
					require.ElementsMatch(t, tt.expectAdd, nodesToAdd,
						"Expected to add %v but got %v", tt.expectAdd, nodesToAdd)
				} else {
					require.Equal(t, tt.expectRemove, nodesToRemove,
						"Expected to remove %v but got %v", tt.expectRemove, nodesToRemove)
					require.Equal(t, tt.expectAdd, nodesToAdd,
						"Expected to add %v but got %v", tt.expectAdd, nodesToAdd)
				}
			}
		})
	}
}
