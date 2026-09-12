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

import java.util.Objects;
import java.util.Optional;
import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.core.auth.BasicAuthPrincipal;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/// Shared controller BasicAuth policy, independent of how principals are loaded and credentials are verified.
/// This class is stateless and thread-safe; subclass principal resolution must also be thread-safe.
abstract class BaseBasicAuthAccessControl<P extends BasicAuthPrincipal> implements AccessControl {
  @Override
  public final boolean protectAnnotatedOnly() {
    return false;
  }

  @Override
  public final boolean hasAccess(String tableName, AccessType accessType, HttpHeaders httpHeaders,
      String endpointUrl) {
    Optional<P> principal = getPrincipal(httpHeaders);
    if (principal.isEmpty()) {
      throw new NotAuthorizedException("Basic");
    }
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    P authenticatedPrincipal = principal.get();
    return authenticatedPrincipal.hasTable(rawTableName)
        && authenticatedPrincipal.hasPermission(Objects.toString(accessType));
  }

  @Override
  public final boolean hasAccess(AccessType accessType, HttpHeaders httpHeaders, String endpointUrl) {
    Optional<P> principal = getPrincipal(httpHeaders);
    if (principal.isEmpty()) {
      throw new NotAuthorizedException("Basic");
    }
    return principal.get().hasPermission(Objects.toString(accessType));
  }

  @Override
  public final boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType, String targetId, String action) {
    // Basic auth permissions are CRUD access types, not action names. AuthenticationFilter enforces the resolved
    // AccessType before invoking this fine-grained check, so this overload must only prevent unauthenticated access.
    return getPrincipal(httpHeaders).isPresent();
  }

  @Override
  public final boolean hasAccess(HttpHeaders httpHeaders, TargetType targetType) {
    return getPrincipal(httpHeaders).isPresent();
  }

  @Override
  public final AuthWorkflowInfo getAuthWorkflowInfo() {
    return new AuthWorkflowInfo(AccessControl.WORKFLOW_BASIC);
  }

  protected abstract Optional<P> getPrincipal(HttpHeaders headers);
}
