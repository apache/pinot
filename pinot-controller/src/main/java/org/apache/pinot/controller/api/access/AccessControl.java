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


@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AccessControl extends FineGrainedAccessControl {
  String WORKFLOW_NONE = "NONE";
  String WORKFLOW_BASIC = "BASIC";

  /**
   * Return whether the client has data access to the given table.
   *
   * Note: This method is only used fore read access. It's being deprecated and its usage will be replaced by
   * `hasAccess` method with AccessType.READ.
   *
   * @param httpHeaders HTTP headers containing requester identity
   * @param tableName Name of the table to be accessed
   * @return Whether the client has data access to the table
   */
  @Deprecated
  default boolean hasDataAccess(HttpHeaders httpHeaders, String tableName) {
    return hasAccess(tableName, AccessType.READ, httpHeaders, null);
  }

  /**
   * Return whether the client has permission to the given table
   *
   * @param tableName name of the table to be accessed
   * @param accessType type of the access
   * @param httpHeaders HTTP headers containing requester identity
   * @param endpointUrl the request url for which this access control is called
   * @return whether the client has permission
   */
  default boolean hasAccess(@Nullable String tableName, AccessType accessType, HttpHeaders httpHeaders,
      @Nullable String endpointUrl) {
    return true;
  }

  /**
   * Return whether the client has permission to access the endpoints with are not table level
   *
   * @param accessType type of the access
   * @param httpHeaders HTTP headers
   * @param endpointUrl the request url for which this access control is called
   * @return whether the client has permission
   */
  default boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, @Nullable String endpointUrl) {
    return hasAccess(null, accessType, httpHeaders, endpointUrl);
  }

  /**
   * Determine whether authentication is required for annotated (controller) endpoints only
   *
   * @return {@code true} if annotated methods are protected only, {@code false} otherwise
   */
  default boolean protectAnnotatedOnly() {
    return true;
  }

  /**
   * Return workflow info for authenticating users. Not all workflows may be supported by the pinot UI implementation.
   *
   * @return workflow info for user authentication
   */
  default AuthWorkflowInfo getAuthWorkflowInfo() {
    return new AuthWorkflowInfo(WORKFLOW_NONE);
  }

  /**
   * Container for authentication workflow info for the Pinot UI. May be extended by implementations.
   *
   * Auth workflow info hold any configuration necessary to execute a UI workflow. We currently foresee supporting NONE
   * (auth disabled) and BASIC (basic auth with username and password)
   */
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
