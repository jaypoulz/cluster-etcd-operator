# etcd.openshift.io/v1alpha1

This API group contains types related to two-node fencing for etcd cluster management.

## PacemakerStatus

The `PacemakerStatus` CRD provides visibility into the health and status of Pacemaker-managed clusters in dual-replica (two-node) OpenShift deployments.

### Feature Gate

- **Feature Gate**: None - this CRD is gated by cluster-etcd-operator start-up. It will only be created once a TNF cluster has transitioned to external etcd.
- **Component**: `two-node-fencing`

### Usage

The PacemakerStatus resource is a cluster-scoped, status-only singleton named "cluster". It is periodically updated by a privileged controller that runs `pcs status xml` and parses the output into structured fields for health checking.

### Fields

- **Summary**: High-level cluster state (quorum, node counts, resource counts, recent failures/fencing)
- **Nodes**: Detailed per-node status (online, standby)
- **Resources**: Detailed per-resource status (agent type, role, active state, node)
- **NodeHistory**: Recent operation history for troubleshooting
- **FencingHistory**: Recent fencing events
- **RawXML**: Complete XML output (for debugging only, max 256KB)

### Notes

The spec field is reserved but unused - all meaningful data is in the status subresource.
