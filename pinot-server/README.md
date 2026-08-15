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

# Pinot Server

## Admin API access control

The Server admin listener uses the Server `AccessControlFactory` as its inbound trust boundary. The route inventory at
the time this contract was introduced has three authorization categories across JAX-RS declarations and static HTTP
handler prefixes:

| Category | Inventory | Families | Authorization |
|---|---:|---|---|
| Public health/readiness | 3 JAX-RS declarations | `/health`, `/health/liveness`, `/health/readiness` | No credentials |
| Table data | 2 JAX-RS declarations | Immutable segment and valid-document bitmap downloads | `hasDataAccess` for the requested table |
| Privileged administration | 48 JAX-RS declarations | Configuration and instance inspection, table and segment administration, reload and reingestion, query and worker administration, logging and diagnostics, and tier or workload operations | `authorizeAdminAccess` |
| Privileged administration | 3 static handler prefixes | `/api/`, `/help/`, `/swaggerui-dist/` | `authorizeAdminAccess` before serving static content |

The three health paths are exact GET exceptions (a query string does not change the path). Other methods on those
paths, all other JAX-RS routes, and all static handlers on the admin listener are privileged unless they are one of the
two built-in table-data downloads. The public and table-data markers are honored only on the exact built-in
`HealthCheckResource` and `TablesResource` classes; custom resource classes remain privileged even if they reuse a
marker. New routes therefore fail closed and require administrative authorization until the built-in inventory and
enforcement code are deliberately extended together. This boundary does not replace the Server query-port channel
checks.

### Authorization contract

`AccessControl.authorizeAdminAccess(RequesterIdentity)` is the contract for non-table administrative operations. An
implementation should:

- throw `NotAuthorizedException` when credentials are absent or invalid, producing HTTP 401;
- return a denied `AuthorizationResult` for an authenticated identity without administrative authority, producing
  HTTP 403; and
- return an allowed result for an authorized administrator.

The default implementation denies administrative access. Custom `AccessControl` implementations must override the
method before enabling them on an upgraded Server; otherwise table-data authorization continues to work but
administrative requests receive 403. This fail-closed default preserves source and binary compatibility without
silently granting a new class of access.

`BasicAuthAccessFactory` authorizes administrative operations with the `admin` permission. Configure it under the
existing Server access-control namespace, for example:

```properties
pinot.server.admin.access.control.factory.class=org.apache.pinot.server.access.BasicAuthAccessFactory
pinot.server.admin.access.control.principals=serverAdmin,segmentReader
pinot.server.admin.access.control.principals.serverAdmin.password=<admin-password>
pinot.server.admin.access.control.principals.serverAdmin.permissions=admin
pinot.server.admin.access.control.principals.segmentReader.password=<reader-password>
pinot.server.admin.access.control.principals.segmentReader.permissions=read
pinot.server.admin.access.control.principals.segmentReader.tables=<allowed-tables>
```

The `admin` permission must be explicit. Although a Basic principal with no configured permissions retains its
existing wildcard behavior for older permission checks, it does not pass the Server administrative check. This avoids
turning an existing table-data identity into an administrator during upgrade. Use dedicated service identities instead
of sharing an operator credential.

`ZkBasicAuthAccessFactory` uses the Server user model instead: the authenticated user must have role `ADMIN` and
component `SERVER`. Its password and table permissions remain independent inputs to authentication and table-data
authorization.

`AllowAllAccessFactory` remains the default when no Server access-control factory is configured. It allows the health,
table-data, and administrative categories, preserving the unconfigured behavior. Operators who require protection
must configure an access-control factory; upgrading alone does not enable authentication.

### Internal callers and rolling migration

Controllers and Brokers invoke privileged Server operations. Their outbound credentials must identify a Server
administrator: an explicit `admin` permission for `BasicAuthAccessFactory`, or role `ADMIN` and component `SERVER` for
`ZkBasicAuthAccessFactory`:

- configure Controller-to-Server credentials under `controller.server.admin.auth.*`; and
- configure Broker-to-Server credentials under `pinot.broker.server.admin.auth.*`.

The two data-download families continue to use table authorization rather than administrative authorization. Segment
archive downloads use segment-fetcher credentials under `controller.segment.fetcher.auth.*` and
`pinot.server.segment.fetcher.auth.*`. Direct valid-document bitmap downloads are currently used by Minion task code
and remain subject to the separate Minion limitation below. Administrative and table access remain separate decisions.

For a rolling deployment:

1. Add or update inbound Server principals. Grant `admin` to each Controller, Broker, automation, and operator identity
   that needs privileged operations. Update custom `AccessControl` implementations at this stage.
2. Deploy Controller and Broker support for the outbound admin credential prefixes and configure the credentials.
   Older Servers ignore the additional authorization header.
3. Upgrade Servers and enable the configured access-control factory. Verify Controller fan-out, Broker-to-Server
   operations, segment downloads, and the three unauthenticated probes before tightening network policy.
4. Migrate remaining direct admin-listener clients. They now receive 401 for missing or invalid credentials and 403
   when authenticated without administrative authority.

### Minion boundary

Minion is intentionally outside this Server change. It has no equivalent inbound `AccessControlFactory` contract, so
its administrative listener must remain restricted to a trusted internal network (or an independently enforced
service-mesh or proxy boundary). Minion's outbound credentials do not provide inbound protection, and Minion should
not be treated as a configured-BasicAuth exception for the Server listener. Minion task code, including task generators
hosted by the Controller and workers that fetch valid-document bitmaps, does not consume the new Server service
credential in this patch. Deployments that combine those task paths with a protected Server listener must inject a
service identity at a trusted proxy or service-mesh boundary. No Minion task, worker, or listener behavior is changed by
this patch.
