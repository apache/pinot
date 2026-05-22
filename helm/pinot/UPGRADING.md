<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Upgrading the Pinot Helm Chart

## From 0.x to 1.0.0

Version 1.0.0 replaces the Bitnami ZooKeeper subchart with native Helm
templates using the [official Apache ZooKeeper Docker image](https://hub.docker.com/_/zookeeper).
This is a **breaking change** that requires manual intervention when upgrading
existing deployments.

### Why is this breaking?

1. **Immutable StatefulSet selectors**: The Bitnami chart uses
   `app.kubernetes.io/name: zookeeper` labels, while the new templates use
   `app: pinot, component: zookeeper`. StatefulSet selectors cannot be changed
   in-place, so `helm upgrade` will fail.

2. **Different data paths**: The Bitnami image stores data at
   `/bitnami/zookeeper`, while the official image uses `/data`. Even though the
   PVC name (`data`) is the same, the new container will not find existing data.

3. **Removed Bitnami-specific values**: Options like `zookeeper.image.registry`,
   `zookeeper.global.security.allowInsecureImages`, `zookeeper.tls.*`, and
   `zookeeper.auth.*` no longer apply.

### Migration steps

> **Important**: ZooKeeper stores Pinot cluster metadata (table configs, schemas,
> segment assignments). Losing this data means the Pinot cluster will need to be
> reconfigured. Plan accordingly.

#### Option A: Fresh ZooKeeper (simplest, requires Pinot reconfiguration)

```bash
NAMESPACE=pinot-quickstart
RELEASE=pinot

# 1. Delete the old ZooKeeper StatefulSet (pods will be terminated)
kubectl delete statefulset ${RELEASE}-zookeeper -n ${NAMESPACE}

# 2. Delete old ZooKeeper PVCs for this Helm release only
kubectl delete pvc -l app.kubernetes.io/name=zookeeper,app.kubernetes.io/instance=${RELEASE} -n ${NAMESPACE}

# 3. Upgrade the Helm release
helm upgrade ${RELEASE} -n ${NAMESPACE} ./helm/pinot

# 4. Recreate your Pinot tables and schemas
```

#### Option B: Migrate ZooKeeper data (preserves Pinot metadata)

> **Note**: This option copies ZooKeeper data from the old Bitnami mount
> path (`/bitnami/zookeeper/data`) to a local backup, then restores it into
> the new official image's data directories (`/data` and `/datalog`) after
> upgrade. This preserves Pinot cluster metadata (table configs, schemas,
> segment assignments). Verify your cluster state after migration.
>
> **Important**: The Bitnami image stores both snapshots and transaction
> logs together in `/bitnami/zookeeper/data/version-2/`. The new chart
> uses separate directories (`/data` for snapshots, `/datalog` for
> transaction logs). The restore step below handles this separation
> automatically. If not separated, ZooKeeper will refuse to start with:
> `Snapshot directory has log files`.
>
> If your old deployment used `replicaCount > 1`, repeat the backup
> (step 1) for each pod (e.g. `zookeeper-1`, `zookeeper-2`) and restore
> from the pod that was the ZooKeeper leader.

```bash
NAMESPACE=pinot-quickstart
RELEASE=pinot

# 1. Back up ZooKeeper data while old cluster is running.
kubectl cp ${NAMESPACE}/${RELEASE}-zookeeper-0:/bitnami/zookeeper/data ./zk-data-backup

# 2. Delete the old ZooKeeper StatefulSet and pods.
kubectl delete statefulset ${RELEASE}-zookeeper -n ${NAMESPACE}

# 3. Delete old PVCs for this release (mount paths are incompatible between images).
kubectl delete pvc -l app.kubernetes.io/name=zookeeper,app.kubernetes.io/instance=${RELEASE} -n ${NAMESPACE}

# 4. Upgrade the Helm release (creates new StatefulSet + PVCs).
helm upgrade ${RELEASE} -n ${NAMESPACE} ./helm/pinot

# 5. Wait for the new ZooKeeper to be ready.
kubectl rollout status statefulset/${RELEASE}-zookeeper -n ${NAMESPACE}

# 6. Scale down ZooKeeper so we can safely restore data to the PVCs.
kubectl scale statefulset/${RELEASE}-zookeeper --replicas=0 -n ${NAMESPACE}

# 7. Launch a temporary pod to mount both PVCs and restore data.
#    Snapshots go to /data/version-2/, transaction logs to /datalog/version-2/.
cat <<EOF | kubectl apply -n ${NAMESPACE} -f -
apiVersion: v1
kind: Pod
metadata:
  name: zk-data-restore
spec:
  containers:
  - name: restore
    image: busybox
    command: ["sleep", "3600"]
    volumeMounts:
    - name: data
      mountPath: /data
    - name: datalog
      mountPath: /datalog
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: data-${RELEASE}-zookeeper-0
  - name: datalog
    persistentVolumeClaim:
      claimName: datalog-${RELEASE}-zookeeper-0
  restartPolicy: Never
EOF
kubectl wait --for=condition=ready pod/zk-data-restore -n ${NAMESPACE} --timeout=10m

# 8. Copy backup into the data PVC, then separate snapshots from transaction logs.
kubectl cp ./zk-data-backup/. ${NAMESPACE}/zk-data-restore:/data
kubectl exec -n ${NAMESPACE} zk-data-restore -- \
  sh -c 'mkdir -p /datalog/version-2 && mv /data/version-2/log.* /datalog/version-2/'

# 9. Clean up the restore pod and scale ZooKeeper back up.
kubectl delete pod zk-data-restore -n ${NAMESPACE}
kubectl scale statefulset/${RELEASE}-zookeeper --replicas=1 -n ${NAMESPACE}

# 10. Wait for ZooKeeper to start with restored data.
kubectl rollout status statefulset/${RELEASE}-zookeeper -n ${NAMESPACE}

# 11. Verify ZooKeeper is healthy.
kubectl exec -n ${NAMESPACE} ${RELEASE}-zookeeper-0 -- \
  bash -c 'echo ruok | nc localhost 2181'

# 12. Pinot components will automatically reconnect. Verify cluster state:
kubectl exec -n ${NAMESPACE} ${RELEASE}-controller-0 -- \
  curl -s http://localhost:9000/instances
```

> If restoration fails or the cluster state looks incorrect, fall back to
> Option A and re-apply your table configs and schemas from your source of
> truth. Segment data on Pinot servers is not affected — only the
> ZooKeeper metadata needs to be recreated.

#### Option C: Use an external ZooKeeper (recommended for production)

For production deployments, consider running ZooKeeper outside of this chart
using the [ZooKeeper Kubernetes Operator](https://github.com/pravega/zookeeper-operator).

```yaml
# values.yaml
zookeeper:
  enabled: false
  urlOverride: "my-external-zookeeper:2181/my-pinot"
```

This avoids the migration entirely and gives you independent lifecycle
management of ZooKeeper.

### Changed values reference

| Old (Bitnami) value | New value | Notes |
|---|---|---|
| `zookeeper.image.registry` + `repository` | `zookeeper.image.repository` | Single field, e.g. `"zookeeper"` |
| `zookeeper.image.tag` | `zookeeper.image.tag` | Use official tags, e.g. `"3.9.3"` |
| `zookeeper.containerPorts.client` | `zookeeper.port` | |
| `zookeeper.auth.*` | `zookeeper.extraEnv` | Configure via env vars |
| `zookeeper.tls.*` | N/A | Not yet supported; use external ZK |
| `zookeeper.global.security.allowInsecureImages` | Removed | Not needed with official image |
| `zookeeper.persistence.accessModes` (array) | `zookeeper.persistence.accessMode` (string) | |
