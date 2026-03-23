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

# 2. Delete old ZooKeeper PVCs
kubectl delete pvc -l app.kubernetes.io/name=zookeeper -n ${NAMESPACE}

# 3. Upgrade the Helm release
helm upgrade ${RELEASE} -n ${NAMESPACE} ./helm/pinot

# 4. Recreate your Pinot tables and schemas
```

#### Option B: Migrate ZooKeeper data (best-effort metadata preservation)

> **Note**: This option copies the ZooKeeper data directory from the old
> Bitnami mount path to the new official image mount path. This preserves
> Pinot cluster metadata (table configs, schemas, segment assignments) on a
> best-effort basis. Verify your cluster state after migration.

```bash
NAMESPACE=pinot-quickstart
RELEASE=pinot

# 1. Copy data from the Bitnami path to the official image path within
#    the existing PVC, while the old ZooKeeper is still running.
kubectl exec -n ${NAMESPACE} ${RELEASE}-zookeeper-0 -- \
  bash -c 'cp -a /bitnami/zookeeper/data/* /tmp/ 2>/dev/null; echo "Data backed up to /tmp"'

# 2. Delete the old ZooKeeper StatefulSet and pods.
kubectl delete statefulset ${RELEASE}-zookeeper -n ${NAMESPACE}

# 3. Delete old PVCs (mount paths are incompatible between images).
kubectl delete pvc -l app.kubernetes.io/name=zookeeper -n ${NAMESPACE}

# 4. Upgrade the Helm release (creates new StatefulSet + PVCs).
helm upgrade ${RELEASE} -n ${NAMESPACE} ./helm/pinot

# 5. Wait for the new ZooKeeper to be ready.
kubectl rollout status statefulset/${RELEASE}-zookeeper -n ${NAMESPACE}

# 6. Pinot components will automatically reconnect and re-register
#    with the new ZooKeeper. However, since ZooKeeper started fresh,
#    you will need to re-apply your Pinot table configs and schemas:
#
#    curl -X POST http://<controller>:9000/schemas -d @mySchema.json
#    curl -X POST http://<controller>:9000/tables -d @myTable.json
```

> If you have many tables/schemas, consider scripting the re-application
> from your source of truth (e.g., version-controlled config files or a
> CI/CD pipeline). Segment data on Pinot servers is not affected — only
> the ZooKeeper metadata needs to be recreated.

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
