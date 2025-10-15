# etcd.openshift.io API Group

This API group contains CRDs related to etcd cluster management. Specifically, this is only used for TNF (Two Node Fencing)
for gathering status updates from the node to ensure the cluster-admin is warned about unhealthy setups.

## API Versions

### v1alpha1

Contains the `PacemakerStatus` custom resource for monitoring Pacemaker cluster health in TNF (Two Node Fencing) deployments.

#### PacemakerStatus

- **Feature Gate**: None - this CRD is gated by cluster-etcd-operator start-up. It will only be created once a TNF cluster has transitioned to external etcd.
- **Component**: `two-node-fencing`
- **Scope**: Cluster-scoped singleton resource named "cluster"

The `PacemakerStatus` resource provides visibility into the health and status of a Pacemaker-managed cluster. It is periodically updated by the cluster-etcd-operator's status collector running as a privileged CronJob.

**Status Fields:**
- `summary`: High-level cluster health metrics (quorum, node counts, resource counts)
- `nodes`: Detailed status of each node (ip, online, standby)
- `resources`: Detailed status of each resource (role, active, node assignment)
- `nodeHistory`: Recent operation failures for troubleshooting
- `fencingHistory`: Recent fencing events
- `rawXML`: Full XML output from `pcs status xml` for debugging

**Usage:**
The cluster-etcd-operator healthcheck controller watches this resource and updates operator conditions based on the cluster state.
