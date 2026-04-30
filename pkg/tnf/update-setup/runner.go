package updatesetup

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	operatorv1 "github.com/openshift/api/operator/v1"
	"github.com/openshift/library-go/pkg/operator/genericoperatorclient"
	"k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"github.com/openshift/cluster-etcd-operator/pkg/operator"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/config"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/etcd"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/exec"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/pacemaker"
	"github.com/openshift/cluster-etcd-operator/pkg/tnf/pkg/pcs"
)

func RunTnfUpdateSetup() error {

	klog.Info("Setting up clients etc. for TNF update-setup")

	clientConfig, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	kubeClient, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return err
	}

	operatorClient, dynamicInformers, err := genericoperatorclient.NewStaticPodOperatorClient(clock.RealClock{}, clientConfig, operatorv1.GroupVersion.WithResource("etcds"), operatorv1.GroupVersion.WithKind("Etcd"), operator.ExtractStaticPodOperatorSpec, operator.ExtractStaticPodOperatorStatus)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	shutdownHandler := server.SetupSignalHandler()
	go func() {
		defer cancel()
		<-shutdownHandler
		klog.Info("Received SIGTERM or SIGINT signal, terminating")
	}()

	dynamicInformers.Start(ctx.Done())
	dynamicInformers.WaitForCacheSync(ctx.Done())

	// Get the current node name from environment
	currentNodeName := os.Getenv("MY_NODE_NAME")
	if currentNodeName == "" {
		return fmt.Errorf("MY_NODE_NAME environment variable not set")
	}

	klog.Infof("Running TNF update-setup")

	// check if cluster is running on this node
	command := "/usr/sbin/pcs cluster status"
	_, _, err = exec.Execute(ctx, command)
	if err != nil {
		klog.Infof("Cluster not running (err: %v), skipping update-setup on this node", err)
		return nil
	}

	// Get current cluster config from Kubernetes
	// Use GetClusterConfigIgnoreMissingNode to allow single-node operation
	cfg, err := config.GetClusterConfigIgnoreMissingNode(ctx, kubeClient)
	if err != nil {
		return err
	}

	// Determine which node we are and get the new IP
	var currentNodeIP, otherNodeName, otherNodeIP string
	if cfg.NodeName1 == currentNodeName {
		currentNodeIP = cfg.NodeIP1
		otherNodeName = cfg.NodeName2
		otherNodeIP = cfg.NodeIP2
	} else if cfg.NodeName2 == currentNodeName {
		currentNodeIP = cfg.NodeIP2
		otherNodeName = cfg.NodeName1
		otherNodeIP = cfg.NodeIP1
	} else {
		return fmt.Errorf("current node %s not found in cluster config (nodes: %s, %s)", currentNodeName, cfg.NodeName1, cfg.NodeName2)
	}

	// Check if running in single-node mode
	isSingleNode := cfg.NodeName2 == ""
	if isSingleNode {
		klog.Info("Running in single-node mode")
	}

	// Reconcile pacemaker membership with Kubernetes
	// Get k8s nodes from cluster config
	k8sNodes := map[string]struct{}{
		cfg.NodeName1: {},
	}
	if !isSingleNode {
		k8sNodes[cfg.NodeName2] = struct{}{}
	}

	// Get pacemaker cluster membership using XML output
	command = "/usr/sbin/pcs status xml"
	stdOut, stdErr, err := exec.Execute(ctx, command)
	if err != nil {
		klog.Errorf("Failed to get pacemaker status: %s, stdout: %s, stderr: %s, err: %v", command, stdOut, stdErr, err)
		return err
	}

	// Parse XML output
	var pacemakerStatus pacemaker.PacemakerResult
	if err := xml.Unmarshal([]byte(stdOut), &pacemakerStatus); err != nil {
		klog.Errorf("Failed to parse pacemaker status XML: %v", err)
		return err
	}

	// Build pacemaker nodes map
	pacemakerNodes := make(map[string]struct{})
	for _, node := range pacemakerStatus.Nodes.Node {
		pacemakerNodes[node.Name] = struct{}{}
		klog.Infof("Found pacemaker node: %q (online=%q)", node.Name, node.Online)
	}

	// Determine what changes are needed using reconciliation logic
	nodesToRemove, nodesToAdd := reconcileNodes(k8sNodes, pacemakerStatus.Nodes.Node)

	// Log the decisions
	for _, node := range pacemakerStatus.Nodes.Node {
		if _, exists := k8sNodes[node.Name]; !exists {
			klog.Infof("Node %q is in pacemaker but not in Kubernetes - will remove from pacemaker (online=%q)", node.Name, node.Online)
		}
	}
	for nodeName := range k8sNodes {
		if _, exists := pacemakerNodes[nodeName]; !exists {
			klog.Infof("Node %q is in Kubernetes but not in pacemaker - will add to pacemaker", nodeName)
		}
	}

	// If no changes needed, we're done
	if len(nodesToRemove) == 0 && len(nodesToAdd) == 0 {
		klog.Info("Pacemaker membership matches Kubernetes, no changes needed")
		return nil
	}

	klog.Infof("Current node: %q (IP: %s), Other node: %q (IP: %s)", currentNodeName, currentNodeIP, otherNodeName, otherNodeIP)
	if len(nodesToRemove) > 0 {
		klog.Infof("Will remove: %v", nodesToRemove)
	}
	if len(nodesToAdd) > 0 {
		klog.Infof("Will add: %v", nodesToAdd)
	}

	// If only removing (node delete event), just remove and return
	if len(nodesToRemove) > 0 && len(nodesToAdd) == 0 {
		klog.Infof("Only removing nodes %v from pacemaker (delete event)", nodesToRemove)

		// Remove all nodes from pacemaker
		for _, nodeToRemove := range nodesToRemove {
			command = fmt.Sprintf("/usr/sbin/pcs cluster node remove %s --force --skip-offline", nodeToRemove)
			stdOut, stdErr, err := exec.Execute(ctx, command)
			if err != nil {
				klog.Errorf("Failed to remove node: %s, stdout: %s, stderr: %s, err: %v", command, stdOut, stdErr, err)
				return err
			}
			klog.Infof("Successfully removed node %q from pacemaker", nodeToRemove)
		}

		// Build set of current node IPs
		currentNodeIPs := map[string]struct{}{
			cfg.NodeIP1: {},
		}
		if !isSingleNode {
			currentNodeIPs[cfg.NodeIP2] = struct{}{}
		}

		// Remove unstarted etcd members (from deleted nodes)
		if err := removeUnstartedEtcdMembers(ctx, currentNodeIPs); err != nil {
			return err
		}
		return nil
	}

	// If adding nodes (with or without removal), proceed with full update workflow
	// don't start the cluster on the new node too early, it might result in etcd start failure because of missing manifests on the new node
	klog.Info("Waiting for etcd revision update before going on...")
	err = etcd.WaitForStableRevision(ctx, operatorClient)
	if err != nil {
		klog.Error(err, "Failed to wait for etcd container transition")
		return err
	}

	// Build list of pacemaker commands
	var commands []string
	for _, nodeToRemove := range nodesToRemove {
		commands = append(commands, fmt.Sprintf("/usr/sbin/pcs cluster node remove %s --force --skip-offline", nodeToRemove))
	}
	for _, nodeToAdd := range nodesToAdd {
		commands = append(commands, fmt.Sprintf("/usr/sbin/pcs cluster node add %s", nodeToAdd))
	}

	err = runCommands(ctx, commands)
	if err != nil {
		return err
	}

	// update fence devices
	// this is needed for being able to start resources on the new node!
	// node order matters here: resources can't be restarted while fencing isn't configured on all nodes!
	var fencingNodes []string
	if isSingleNode {
		fencingNodes = []string{currentNodeName}
	} else {
		fencingNodes = []string{otherNodeName, currentNodeName}
	}
	err = pcs.ConfigureFencing(ctx, kubeClient, fencingNodes)
	if err != nil {
		klog.Error(err, "Failed to configure fencing, skipping update of etcd! Restart update-setup job when fencing config is fixed!")
		return err
	}

	// Build node_ip_map parameter
	var nodeIPMap string
	if isSingleNode {
		nodeIPMap = fmt.Sprintf("%s:%s", cfg.NodeName1, cfg.NodeIP1)
	} else {
		nodeIPMap = fmt.Sprintf("%s:%s;%s:%s", cfg.NodeName1, cfg.NodeIP1, cfg.NodeName2, cfg.NodeIP2)
	}

	commands = []string{
		// Force new cluster on next etcd restart on this node
		fmt.Sprintf("crm_attribute --lifetime reboot --node %s --name \"force_new_cluster\" --update %s", currentNodeName, currentNodeName),
		// Update etcd resource
		fmt.Sprintf("/usr/sbin/pcs resource update etcd node_ip_map=\"%s\" --wait=300", nodeIPMap),
	}
	err = runCommands(ctx, commands)
	if err != nil {
		return err
	}

	// Build set of current node IPs
	currentNodeIPs := map[string]struct{}{
		cfg.NodeIP1: {},
	}
	if !isSingleNode {
		currentNodeIPs[cfg.NodeIP2] = struct{}{}
	}

	// Remove unstarted etcd members (from deleted nodes)
	if err := removeUnstartedEtcdMembers(ctx, currentNodeIPs); err != nil {
		return err
	}

	// wait a bit for things to settle
	// without this the etcd start on the new node fails for some reason...
	time.Sleep(10 * time.Second)

	commands = []string{
		// Enable cluster on new node
		"/usr/sbin/pcs cluster enable --all",
		// Start cluster on new node
		"/usr/sbin/pcs cluster start --all",
	}
	err = runCommands(ctx, commands)
	if err != nil {
		return err
	}

	return nil
}

// reconcileNodes determines which nodes to add/remove based on k8s vs pacemaker membership
// This is the core reconciliation logic extracted for testing
func reconcileNodes(k8sNodes map[string]struct{}, pacemakerNodes []pacemaker.Node) (nodesToRemove, nodesToAdd []string) {
	// Build pacemaker nodes map
	pacemakerNodesMap := make(map[string]struct{})
	for _, node := range pacemakerNodes {
		pacemakerNodesMap[node.Name] = struct{}{}
	}

	// Collect all pacemaker nodes not in k8s (cleanup stale/deleted nodes)
	for _, node := range pacemakerNodes {
		if _, exists := k8sNodes[node.Name]; !exists {
			nodesToRemove = append(nodesToRemove, node.Name)
		}
	}

	// Collect all k8s nodes not in pacemaker
	for nodeName := range k8sNodes {
		if _, exists := pacemakerNodesMap[nodeName]; !exists {
			nodesToAdd = append(nodesToAdd, nodeName)
		}
	}

	return nodesToRemove, nodesToAdd
}

func runCommands(ctx context.Context, commands []string) error {
	for _, command := range commands {
		stdOut, stdErr, err := exec.Execute(ctx, command)
		if err != nil {
			klog.Errorf("Failed to run update-setup command: %s, stdout: %s, stderr: %s, err: %v", command, stdOut, stdErr, err)
			return err
		}
		klog.Infof("Successfully executed: %s", command)
	}
	return nil
}

// removeUnstartedEtcdMembers removes unstarted etcd members that don't match any current k8s node.
// When a node is removed from pacemaker, its etcd member becomes unstarted (empty name).
// This function removes all such unstarted members whose IP doesn't match any node in currentNodeIPs,
// cleaning up stale members from deleted nodes while preserving members for current nodes.
func removeUnstartedEtcdMembers(ctx context.Context, currentNodeIPs map[string]struct{}) error {
	klog.Info("Checking for unstarted etcd members to remove")

	// Get etcd member list
	command := "podman exec etcd /usr/bin/etcdctl member list -w json"
	stdOut, stdErr, err := exec.Execute(ctx, command)
	if err != nil {
		return fmt.Errorf("failed to list etcd members: stdout: %s, stderr: %s, err: %w", stdOut, stdErr, err)
	}

	// Parse JSON response
	var memberList struct {
		Members []struct {
			ID         uint64   `json:"ID"`
			Name       string   `json:"name"`
			PeerURLs   []string `json:"peerURLs"`
			ClientURLs []string `json:"clientURLs"`
			IsLearner  bool     `json:"isLearner"`
		} `json:"members"`
	}

	if err := json.Unmarshal([]byte(stdOut), &memberList); err != nil {
		return fmt.Errorf("failed to parse etcd member list JSON: %w", err)
	}

	// Remove all unstarted members that don't match any current node
	var removed []string
	for _, member := range memberList.Members {
		// Only consider unstarted members (empty name indicates unstarted)
		if member.Name != "" {
			continue
		}

		memberIDHex := fmt.Sprintf("%x", member.ID)

		// Extract IP from peer URLs
		var memberIP string
		for _, peerURLStr := range member.PeerURLs {
			parsedURL, err := url.Parse(peerURLStr)
			if err != nil {
				continue
			}

			host, _, err := net.SplitHostPort(parsedURL.Host)
			if err != nil {
				host = parsedURL.Host
			}
			host = strings.Trim(host, "[]")
			memberIP = host
			break
		}

		if memberIP == "" {
			klog.Warningf("Could not extract IP from unstarted member %s peer URLs: %v", memberIDHex, member.PeerURLs)
			continue
		}

		// Check if this IP matches any current node
		if _, isCurrentNode := currentNodeIPs[memberIP]; isCurrentNode {
			klog.Infof("Unstarted etcd member %s (IP: %s) matches a current node - not removing", memberIDHex, memberIP)
			continue
		}

		// This is an unstarted member for a removed/stale node - remove it
		klog.Infof("Found unstarted etcd member %s (IP: %s) not matching any current node - removing", memberIDHex, memberIP)
		command = fmt.Sprintf("podman exec etcd /usr/bin/etcdctl member remove %s", memberIDHex)
		stdOut, stdErr, err = exec.Execute(ctx, command)
		if err != nil {
			return fmt.Errorf("failed to remove unstarted etcd member %s: stdout: %s, stderr: %s, err: %w", memberIDHex, stdOut, stdErr, err)
		}
		removed = append(removed, fmt.Sprintf("%s (IP: %s)", memberIDHex, memberIP))
		klog.Infof("Removed unstarted etcd member %s", memberIDHex)
	}

	if len(removed) == 0 {
		klog.Info("No unstarted etcd members found for removed nodes")
	} else {
		klog.Infof("Successfully removed %d unstarted etcd member(s): %v", len(removed), removed)
	}

	return nil
}
