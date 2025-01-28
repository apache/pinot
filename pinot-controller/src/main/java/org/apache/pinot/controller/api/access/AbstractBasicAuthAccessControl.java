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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Abstract class for basic authentication access control implementations of the controller.
 */
abstract class AbstractBasicAuthAccessControl implements AccessControl {
  protected static final String HEADER_AUTHORIZATION = "authorization";
  protected static final String BASIC_AUTH = AccessControl.WORKFLOW_BASIC;

  /**
   * Return whether the client has permission to access the endpoints which are not table level for
   * the given access type and endpoint URL.
   * - If the requester identity is not authorized, throws a NotAuthorizedException.
   * - Else, returns success.
   *
   * @param accessType type of the access
   * @param httpHeaders HTTP headers containing requester identity
   * @param endpointUrl the request url for which this access control is called
   */
  @Override
  public boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
    Optional<? extends BasicAuthPrincipal> principalOpt = getPrincipal(httpHeaders);
    if (!principalOpt.isPresent()) {
      throw new NotAuthorizedException(AccessControl.WORKFLOW_BASIC);
    }

    return true;
  }

  /**
   * Return whether the client has permission to access the given table for the given access type and
   * endpoint URL.
   * - If the requester identity is not authorized, throws a NotAuthorizedException.
   * - If the principal has the required access to the table, returns success.
   * - If the principal does not have the required access to the table, returns failure.
   *
   * @param tableName name of the table to be accessed
   * @param accessType type of the access
   * @param httpHeaders HTTP headers containing requester identity
   * @param endpointUrl the request url for which this access control is called
   */
  @Override
  public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
    Optional<? extends BasicAuthPrincipal> principalOpt = getPrincipal(httpHeaders);
    if (!principalOpt.isPresent()) {
      throw new NotAuthorizedException(BASIC_AUTH);
    }

    return principalOpt
        .filter(p -> p.hasTable(TableNameBuilder.extractRawTableName(tableName))
            && p.hasPermission(Objects.toString(accessType)))
        .isPresent();
  }

  @Override
  public boolean protectAnnotatedOnly() {
    return false;
  }

  @Override
  public AuthWorkflowInfo getAuthWorkflowInfo() {
    return new AuthWorkflowInfo(BASIC_AUTH);
  }

  /**
   * Return the tokens from the given HTTP headers.
   * @param httpHeaders HTTP headers containing requester identity
   */
  protected List<String> getTokens(HttpHeaders httpHeaders) {
    if (httpHeaders == null) {
      return Collections.emptyList();
    }

    return httpHeaders.getRequestHeader(HEADER_AUTHORIZATION);
  }

  /**
   * Return the principal for the given HTTP headers.
   * @param httpHeaders HTTP headers containing requester identity
   */
  protected abstract Optional<? extends BasicAuthPrincipal> getPrincipal(HttpHeaders httpHeaders);
}
