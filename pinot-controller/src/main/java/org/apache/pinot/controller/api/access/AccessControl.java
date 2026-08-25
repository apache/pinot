/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.access;

import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.FineGrainedAccessControl;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/// Controller access-control SPI.
///
/// Custom implementations should audit two resolution changes from apache/pinot#18975:
/// 1. A table name appended as an undeclared query parameter no longer reaches
///    [#hasAccess(String, AccessType, HttpHeaders, String)]; the request arrives with a `null`
///    table name and must be treated as cluster-wide.
/// 2. [org.apache.pinot.core.auth.FineGrainedAuthUtils#findRawTargetId] changed from 3 arguments
///    (`Authorize`, path map, query map) to 4 by adding the resource `Method`. There is no
///    overload; a plugin calling the old signature will fail to link.
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControl extends FineGrainedAccessControl {
  String WORKFLOW_NONE = "NONE";
  String WORKFLOW_BASIC = "BASIC";

  /// Return whether the client has permission to the given table
  ///
  /// A `null` table name means the endpoint named no table — either it declares no table parameter, or the request
  /// omitted an optional one — so the request addresses the cluster rather than a table. Implementations must apply
  /// their cluster policy in that case; returning `true` for a `null` table name grants every cluster endpoint.
  /// Note that a caller-supplied query parameter only names the table when the endpoint declares it, so a table name
  /// appended to a cluster endpoint arrives here as `null`.
  ///
  /// @param tableName name of the table to be accessed, or `null` when the request names no table
  /// @param accessType type of the access
  /// @param httpHeaders HTTP headers containing requester identity
  /// @param endpointUrl the request url for which this access control is called
  /// @return whether the client has permission
  default boolean hasAccess(@Nullable String tableName, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl) {
    return true;
  }

  /// Return whether the client has permission to access the endpoints with are not table level
  ///
  /// This is the cluster-wide gate. The request named no table, so it may reach state belonging to any table: grant it
  /// only to a principal whose authority spans the whole cluster, never to one scoped to a subset of tables. Granting
  /// it on a narrower scope is what made table-scoped users able to drive cluster endpoints in apache/pinot#14595.
  ///
  /// @param accessType type of the access
  /// @param httpHeaders HTTP headers
  /// @param endpointUrl the request url for which this access control is called
  /// @return whether the client has permission
  default boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
    return hasAccess(null, accessType, httpHeaders, endpointUrl);
  }

  /// Determine whether authentication is required for annotated (controller) endpoints only
  ///
  /// @return `true` if annotated methods are protected only, `false` otherwise
  default boolean protectAnnotatedOnly() {
    return true;
  }

  /// Return workflow info for authenticating users. Not all workflows may be supported by the pinot UI implementation.
  ///
  /// @return workflow info for user authentication
  default AuthWorkflowInfo getAuthWorkflowInfo() {
    return new AuthWorkflowInfo(WORKFLOW_NONE);
  }

  /// Container for authentication workflow info for the Pinot UI. May be extended by implementations.
  ///
  /// Auth workflow info hold any configuration necessary to execute a UI workflow. We currently foresee supporting NONE
  /// (auth disabled) and BASIC (basic auth with username and password)
  class AuthWorkflowInfo {
    String _workflow;

    public AuthWorkflowInfo(String workflow) {
      _workflow = workflow;
    }

    public String getWorkflow() {
      return _workflow;
    }

    public void setWorkflow(String workflow) {
      _workflow = workflow;
    }
  }
}
